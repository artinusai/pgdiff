package main

import (
	"strings"
	"testing"

	"github.com/joncrlsn/pgutil"
)

func TestViewSchemaAddUsesQualifiedTargetSchema(t *testing.T) {
	oldDbInfo1 := dbInfo1
	oldDbInfo2 := dbInfo2
	defer func() {
		dbInfo1 = oldDbInfo1
		dbInfo2 = oldDbInfo2
	}()

	dbInfo1 = pgutil.DbInfo{DbSchema: "s1"}
	dbInfo2 = pgutil.DbInfo{DbSchema: "s2"}

	schema := ViewSchema{
		rows: ViewRows{
			{
				"schema_name": "s1",
				"viewname":    "view_add",
				"definition":  " SELECT 1",
			},
		},
		rowNum: 0,
	}

	output := captureStdout(t, func() {
		schema.Add()
	})

	if !strings.Contains(output, "CREATE VIEW s2.view_add AS") {
		t.Fatalf("expected qualified target schema in view output, got: %s", output)
	}
}

func TestMatViewSchemaAddRewritesIndexTargetSchema(t *testing.T) {
	oldDbInfo1 := dbInfo1
	oldDbInfo2 := dbInfo2
	defer func() {
		dbInfo1 = oldDbInfo1
		dbInfo2 = oldDbInfo2
	}()

	dbInfo1 = pgutil.DbInfo{DbSchema: "s1"}
	dbInfo2 = pgutil.DbInfo{DbSchema: "s2"}

	schema := MatViewSchema{
		rows: MatViewRows{
			{
				"schema_name": "s1",
				"matviewname": "mv_add",
				"definition":  " SELECT 1",
				"indexdef":    "CREATE INDEX mv_add_idx ON s1.mv_add USING btree (id);",
			},
		},
		rowNum: 0,
	}

	output := captureStdout(t, func() {
		schema.Add()
	})

	if !strings.Contains(output, "CREATE MATERIALIZED VIEW s2.mv_add AS") {
		t.Fatalf("expected qualified target schema in matview output, got: %s", output)
	}
	if !strings.Contains(output, "ON s2.mv_add") {
		t.Fatalf("expected rewritten matview index definition, got: %s", output)
	}
}
