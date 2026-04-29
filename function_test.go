package main

import (
	"strings"
	"testing"

	"github.com/joncrlsn/pgutil"
)

func TestNormalizeFunctionDefinitionStripsSchemaFromHeaderOnly(t *testing.T) {
	definition := `
CREATE OR REPLACE FUNCTION s1.increment(i integer)
RETURNS integer
LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN i + 1;
END;
$function$`

	got := normalizeFunctionDefinition(definition, "s1", "increment")
	if strings.Contains(got, "FUNCTIONs1.increment(") {
		t.Fatalf("schema qualifier still present in normalized definition: %s", got)
	}
	if !strings.Contains(got, "FUNCTIONincrement(iinteger)") {
		t.Fatalf("normalized definition lost function signature: %s", got)
	}
}

func TestFunctionSchemaDropUsesIdentityArguments(t *testing.T) {
	schema := FunctionSchema{
		rows: FunctionRows{
			{
				"schema_name":   "s1",
				"function_name": "add",
				"identity_args": "integer, integer",
			},
		},
		rowNum: 0,
	}

	output := captureStdout(t, func() {
		schema.Drop()
	})

	expected := "DROP FUNCTION s1.add(integer, integer) CASCADE;\n"
	if !strings.Contains(output, expected) {
		t.Fatalf("expected signature-specific drop, got: %s", output)
	}
}

func TestFunctionSchemaChangeRewritesOnlyHeaderSchema(t *testing.T) {
	oldDbInfo1 := dbInfo1
	oldDbInfo2 := dbInfo2
	defer func() {
		dbInfo1 = oldDbInfo1
		dbInfo2 = oldDbInfo2
	}()

	dbInfo1 = pgutil.DbInfo{DbSchema: "s1"}
	dbInfo2 = pgutil.DbInfo{DbSchema: "s2"}

	source := &FunctionSchema{
		rows: FunctionRows{
			{
				"schema_name":   "s1",
				"function_name": "increment",
				"identity_args": "i integer",
				"definition": `CREATE OR REPLACE FUNCTION s1.increment(i integer)
RETURNS integer
LANGUAGE sql
AS $$ SELECT i + 1 FROM s1.lookup $$`,
			},
		},
		rowNum: 0,
	}
	target := &FunctionSchema{
		rows: FunctionRows{
			{
				"schema_name":   "s2",
				"function_name": "increment",
				"identity_args": "i integer",
				"definition": `CREATE OR REPLACE FUNCTION s2.increment(i integer)
RETURNS integer
LANGUAGE sql
AS $$ SELECT i + 2 FROM s1.lookup $$`,
			},
		},
		rowNum: 0,
	}

	output := captureStdout(t, func() {
		source.Change(target)
	})

	if !strings.Contains(output, "CREATE OR REPLACE FUNCTION s2.increment(i integer)") {
		t.Fatalf("expected header schema rewrite, got: %s", output)
	}
	if !strings.Contains(output, "FROM s1.lookup") {
		t.Fatalf("expected function body schema reference to remain unchanged, got: %s", output)
	}
}

func TestFunctionCompareTreatsOverloadsAsDistinct(t *testing.T) {
	first := &FunctionSchema{
		rows:   FunctionRows{{"compare_name": "add(integer, integer)"}},
		rowNum: 0,
	}
	second := &FunctionSchema{
		rows:   FunctionRows{{"compare_name": "add(bigint, bigint)"}},
		rowNum: 0,
	}

	if first.Compare(second) == 0 {
		t.Fatal("expected overloaded functions to compare as distinct")
	}
}
