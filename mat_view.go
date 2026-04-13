//
// Copyright (c) 2016 Jon Carlson.  All rights reserved.
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

// ==================================
// MatViewRows definition
// ==================================

// MatViewRows is a sortable slice of string maps
type MatViewRows []map[string]string

func (slice MatViewRows) Len() int {
	return len(slice)
}

func (slice MatViewRows) Less(i, j int) bool {
	return slice[i]["compare_name"] < slice[j]["compare_name"]
}

func (slice MatViewRows) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}

// MatViewSchema holds a channel streaming matview information from one of the databases as well as
// a reference to the current row of data we're matviewing.
//
// MatViewSchema implements the Schema interface defined in pgdiff.go
type MatViewSchema struct {
	rows   MatViewRows
	rowNum int
	done   bool
}

// get returns the value from the current row for the given key
func (c *MatViewSchema) get(key string) string {
	if c.rowNum >= len(c.rows) {
		return ""
	}
	return c.rows[c.rowNum][key]
}

func (c *MatViewSchema) debug() {
	fmt.Println(c.rows[c.rowNum])
}

// NextRow increments the rowNum and tells you whether or not there are more
func (c *MatViewSchema) NextRow() bool {
	if c.rowNum >= len(c.rows)-1 {
		c.done = true
	}
	c.rowNum = c.rowNum + 1
	return !c.done
}

// Compare tells you, in one pass, whether or not the first row matches, is less than, or greater than the second row
func (c *MatViewSchema) Compare(obj interface{}) int {
	c2, ok := obj.(*MatViewSchema)
	if !ok {
		fmt.Println("Error!!!, Compare(obj) needs a MatViewSchema instance", c2)
		return +999
	}

	val := misc.CompareStrings(c.get("compare_name"), c2.get("compare_name"))
	//fmt.Printf("-- Compared %v: %s with %s \n", val, c.get("matviewname"), c2.get("matviewname"))
	return val
}

// Add returns SQL to create the matview
func (c MatViewSchema) Add() {
	schemaName := matViewTargetSchema(c.get("schema_name"))
	fmt.Printf("CREATE MATERIALIZED VIEW %s.%s AS %s \n\n%s \n\n", schemaName, c.get("matviewname"), c.get("definition"), rewriteMatViewIndexDefs(c.get("indexdef"), c.get("schema_name"), schemaName, c.get("matviewname")))
}

// Drop returns SQL to drop the matview
func (c MatViewSchema) Drop() {
	fmt.Printf("DROP MATERIALIZED VIEW %s.%s;\n\n", c.get("schema_name"), c.get("matviewname"))
}

// Change handles the case where the names match, but the definition does not
func (c MatViewSchema) Change(obj interface{}) {
	c2, ok := obj.(*MatViewSchema)
	if !ok {
		fmt.Println("Error!!!, Change needs a MatViewSchema instance", c2)
	}
	if c.get("definition") != c2.get("definition") {
		targetSchema := matViewTargetSchema(c2.get("schema_name"))
		fmt.Printf("DROP MATERIALIZED VIEW %s.%s;\n\n", c2.get("schema_name"), c.get("matviewname"))
		fmt.Printf("CREATE MATERIALIZED VIEW %s.%s AS %s \n\n%s \n\n", targetSchema, c.get("matviewname"), c.get("definition"), rewriteMatViewIndexDefs(c.get("indexdef"), c.get("schema_name"), targetSchema, c.get("matviewname")))
	}
}

var (
	matViewSqlTemplate = initMatViewSqlTemplate()
)

func initMatViewSqlTemplate() *template.Template {
	sql := `
		WITH matviews AS (
			SELECT schemaname AS schema_name
				, {{if eq $.DbSchema "*" }}schemaname || '.' || {{end}}matviewname AS compare_name
				, matviewname
				, definition
			FROM pg_catalog.pg_matviews
			WHERE true
			{{if eq $.DbSchema "*" }}
			AND schemaname NOT LIKE 'pg_%'
			AND schemaname <> 'information_schema'
			{{else}}
			AND schemaname = '{{$.DbSchema}}'
			{{end}}
		)
		SELECT
			m.schema_name,
			m.compare_name,
			m.matviewname,
			m.definition,
			COALESCE(string_agg(i.indexdef, ';' || E'\n\n' ORDER BY i.indexname) || ';', '') AS indexdef
		FROM matviews AS m
		LEFT JOIN pg_catalog.pg_indexes AS i
			ON m.schema_name = i.schemaname AND
			   m.matviewname = i.tablename
		GROUP BY m.schema_name, m.compare_name, m.matviewname, m.definition
		ORDER BY m.compare_name;
	`
	t := template.New("MatViewSqlTmpl")
	template.Must(t.Parse(sql))
	return t
}

// compareMatViews outputs SQL to make the matviews match between DBs
func compareMatViews(conn1 *sql.DB, conn2 *sql.DB) {
	buf1 := new(bytes.Buffer)
	check("rendering source materialized view SQL template", matViewSqlTemplate.Execute(buf1, dbInfo1))

	buf2 := new(bytes.Buffer)
	check("rendering target materialized view SQL template", matViewSqlTemplate.Execute(buf2, dbInfo2))

	rowChan1, _ := pgutil.QueryStrings(conn1, buf1.String())
	rowChan2, _ := pgutil.QueryStrings(conn2, buf2.String())

	rows1 := make(MatViewRows, 0)
	for row := range rowChan1 {
		rows1 = append(rows1, row)
	}
	sort.Sort(rows1)

	rows2 := make(MatViewRows, 0)
	for row := range rowChan2 {
		rows2 = append(rows2, row)
	}
	sort.Sort(rows2)

	// We have to explicitly type this as Schema here
	var schema1 Schema = &MatViewSchema{rows: rows1, rowNum: -1}
	var schema2 Schema = &MatViewSchema{rows: rows2, rowNum: -1}

	// Compare the matviews
	doDiff(schema1, schema2)
}

func matViewTargetSchema(schemaName string) string {
	if dbInfo1.DbSchema != dbInfo2.DbSchema {
		return dbInfo2.DbSchema
	}
	return schemaName
}

func rewriteMatViewIndexDefs(indexDefs string, fromSchema string, toSchema string, matViewName string) string {
	if fromSchema == toSchema || len(indexDefs) == 0 {
		return indexDefs
	}
	return strings.Replace(
		indexDefs,
		fmt.Sprintf(" ON %s.%s ", fromSchema, matViewName),
		fmt.Sprintf(" ON %s.%s ", toSchema, matViewName),
		-1,
	)
}
