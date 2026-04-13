//
// Copyright (c) 2017 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"text/template"

	"github.com/joncrlsn/misc"
	"github.com/joncrlsn/pgutil"
)

var (
	grantRelationshipSqlTemplate = initGrantRelationshipSqlTemplate()
)

// Initializes the Sql template
func initGrantRelationshipSqlTemplate() *template.Template {
	sql := `
SELECT n.nspname AS schema_name
  , {{ if eq $.DbSchema "*" }}n.nspname::text  || '.' || {{ end }}c.relkind::text  || '.' || c.relname::text  AS compare_name
  , CASE c.relkind
    WHEN 'r' THEN 'TABLE'
    WHEN 'v' THEN 'VIEW'
    WHEN 'S' THEN 'SEQUENCE'
    WHEN 'f' THEN 'FOREIGN TABLE'
    END as type
  , c.relname AS relationship_name
  , unnest(c.relacl) AS relationship_acl
FROM pg_catalog.pg_class c
LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
WHERE c.relkind IN ('r', 'v', 'S', 'f')
--AND pg_catalog.pg_table_is_visible(c.oid)
{{ if eq $.DbSchema "*" }}
AND n.nspname NOT LIKE 'pg_%'
AND n.nspname <> 'information_schema'
{{ else }}
AND n.nspname = '{{ $.DbSchema }}'
{{ end }};
`

	t := template.New("GrantRelationshipSqlTmpl")
	template.Must(t.Parse(sql))
	return t
}

// ==================================
// GrantRelationshipRows definition
// ==================================

// GrantRelationshipRows is a sortable slice of string maps
type GrantRelationshipRows []map[string]string

func (slice GrantRelationshipRows) Len() int {
	return len(slice)
}

func (slice GrantRelationshipRows) Less(i, j int) bool {
	if slice[i]["compare_name"] != slice[j]["compare_name"] {
		return slice[i]["compare_name"] < slice[j]["compare_name"]
	}

	// Only compare the role part of the ACL
	// Not yet sure if this is absolutely necessary
	// (or if we could just compare the entire ACL string)
	relRole1, _ := parseAcl(slice[i]["relationship_acl"])
	relRole2, _ := parseAcl(slice[j]["relationship_acl"])
	if relRole1 != relRole2 {
		return relRole1 < relRole2
	}

	return false
}

func (slice GrantRelationshipRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// ==================================
// GrantRelationshipSchema definition
// (implements Schema -- defined in pgdiff.go)
// ==================================

// GrantRelationshipSchema holds a slice of rows from one of the databases as well as
// a reference to the current row of data we're viewing.
type GrantRelationshipSchema struct {
	rows   GrantRelationshipRows
	rowNum int
	done   bool
}

// get returns the value from the current row for the given key
func (c *GrantRelationshipSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

func (c *GrantRelationshipSchema) debug() {
	fmt.Println(c.rows[c.rowNum])
}

// get returns the current row for the given key
func (c *GrantRelationshipSchema) getRow() map[string]string {
	if c.rowNum >= len(c.rows) {
		return make(map[string]string)
	}
	return c.rows[c.rowNum]
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *GrantRelationshipSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *GrantRelationshipSchema) Compare(obj interface{}) int {
	c2, ok := obj.(*GrantRelationshipSchema)
	if !ok {
		fmt.Println("Error!!!, Compare needs a GrantRelationshipSchema instance", c2)
		return +999
	}

	val := misc.CompareStrings(c.get("compare_name"), c2.get("compare_name"))
	if val != 0 {
		return val
	}

	relRole1, _ := parseAcl(c.get("relationship_acl"))
	relRole2, _ := parseAcl(c2.get("relationship_acl"))
	val = misc.CompareStrings(relRole1, relRole2)
	return val
}

// Add prints SQL to add the grant
func (c *GrantRelationshipSchema) Add() {
	schema := dbInfo2.DbSchema
	if schema == "*" {
		schema = c.get("schema_name")
	}

	role, grants := parseGrantPrivileges(c.get("relationship_acl"))
	emitGrantRelationshipStatements(schema, c.get("relationship_name"), role, diffGrantPrivileges(grants, GrantPrivileges{}), "Add")
}

// Drop prints SQL to drop the grant
func (c *GrantRelationshipSchema) Drop() {
	role, grants := parseGrantPrivileges(c.get("relationship_acl"))
	if len(grants.Privileges) > 0 {
		fmt.Printf("REVOKE %s ON %s.%s FROM %s; -- Drop\n", strings.Join(grants.Privileges, ", "), c.get("schema_name"), c.get("relationship_name"), role)
	}
}

// Change handles the case where the relationship and column match, but the grant does not
func (c *GrantRelationshipSchema) Change(obj interface{}) {
	c2, ok := obj.(*GrantRelationshipSchema)
	if !ok {
		fmt.Println("-- Error!!!, Change needs a GrantRelationshipSchema instance", c2)
	}

	role, grants1 := parseGrantPrivileges(c.get("relationship_acl"))
	_, grants2 := parseGrantPrivileges(c2.get("relationship_acl"))
	emitGrantRelationshipStatements(c2.get("schema_name"), c.get("relationship_name"), role, diffGrantPrivileges(grants1, grants2), "Change")

	//	fmt.Printf("--1 rel:%s, relAcl:%s, col:%s, colAcl:%s\n", c.get("relationship_name"), c.get("relationship_acl"), c.get("column_name"), c.get("column_acl"))
	//	fmt.Printf("--2 rel:%s, relAcl:%s, col:%s, colAcl:%s\n", c2.get("relationship_name"), c2.get("relationship_acl"), c2.get("column_name"), c2.get("column_acl"))
}

func emitGrantRelationshipStatements(schema string, relationshipName string, role string, diff GrantPrivilegeDiff, action string) {
	if len(diff.GrantPrivileges) > 0 {
		fmt.Printf("GRANT %s ON %s.%s TO %s; -- %s\n", strings.Join(diff.GrantPrivileges, ", "), schema, relationshipName, role, action)
	}
	if len(diff.GrantOptionPrivileges) > 0 {
		fmt.Printf("GRANT %s ON %s.%s TO %s WITH GRANT OPTION; -- %s\n", strings.Join(diff.GrantOptionPrivileges, ", "), schema, relationshipName, role, action)
	}
	if len(diff.RevokeGrantOptionPrivileges) > 0 {
		fmt.Printf("REVOKE GRANT OPTION FOR %s ON %s.%s FROM %s; -- %s\n", strings.Join(diff.RevokeGrantOptionPrivileges, ", "), schema, relationshipName, role, action)
	}
	if len(diff.RevokePrivileges) > 0 {
		fmt.Printf("REVOKE %s ON %s.%s FROM %s; -- %s\n", strings.Join(diff.RevokePrivileges, ", "), schema, relationshipName, role, action)
	}
}

// ==================================
// Functions
// ==================================

// compareGrantRelationships outputs SQL to make the granted permissions match between DBs or schemas
func compareGrantRelationships(conn1 *sql.DB, conn2 *sql.DB) {

	buf1 := new(bytes.Buffer)
	grantRelationshipSqlTemplate.Execute(buf1, dbInfo1)

	buf2 := new(bytes.Buffer)
	grantRelationshipSqlTemplate.Execute(buf2, dbInfo2)

	rowChan1, _ := pgutil.QueryStrings(conn1, buf1.String())
	rowChan2, _ := pgutil.QueryStrings(conn2, buf2.String())

	rows1 := make(GrantRelationshipRows, 0)
	for row := range rowChan1 {
		rows1 = append(rows1, row)
	}
	sort.Sort(rows1)

	rows2 := make(GrantRelationshipRows, 0)
	for row := range rowChan2 {
		rows2 = append(rows2, row)
	}
	sort.Sort(rows2)

	// We have to explicitly type this as Schema here for some unknown (to me) reason
	var schema1 Schema = &GrantRelationshipSchema{rows: rows1, rowNum: -1}
	var schema2 Schema = &GrantRelationshipSchema{rows: rows2, rowNum: -1}

	doDiff(schema1, schema2)
}
