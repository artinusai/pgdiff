package main

import (
	"reflect"
	"testing"

	"github.com/joncrlsn/pgutil"
)

func TestAllExecutionPlanUsesCanonicalWildcardOrder(t *testing.T) {
	steps, err := buildExecutionPlan("ALL", pgutil.DbInfo{DbSchema: "*"}, pgutil.DbInfo{DbSchema: "*"})
	if err != nil {
		t.Fatalf("building ALL plan: %v", err)
	}

	expected := []string{
		"SCHEMA",
		"ROLE",
		"SEQUENCE",
		"TABLE",
		"COLUMN",
		"INDEX",
		"VIEW",
		"MATVIEW",
		"FOREIGN_KEY",
		"FUNCTION",
		"TRIGGER",
		"OWNER",
		"GRANT_RELATIONSHIP",
		"GRANT_ATTRIBUTE",
	}

	if got := planStepNames(steps); !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected ALL wildcard plan:\nwant: %#v\ngot:  %#v", expected, got)
	}
}

func TestAllExecutionPlanSkipsSchemaOutsideWildcardRuns(t *testing.T) {
	steps, err := buildExecutionPlan("ALL", pgutil.DbInfo{DbSchema: "public"}, pgutil.DbInfo{DbSchema: "archive"})
	if err != nil {
		t.Fatalf("building ALL plan: %v", err)
	}

	expected := []string{
		"ROLE",
		"SEQUENCE",
		"TABLE",
		"COLUMN",
		"INDEX",
		"VIEW",
		"MATVIEW",
		"FOREIGN_KEY",
		"FUNCTION",
		"TRIGGER",
		"OWNER",
		"GRANT_RELATIONSHIP",
		"GRANT_ATTRIBUTE",
	}

	if got := planStepNames(steps); !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected ALL cross-schema plan:\nwant: %#v\ngot:  %#v", expected, got)
	}
}

func TestAllExecutionPlanSkipsSchemaForNamedSchemaRuns(t *testing.T) {
	steps, err := buildExecutionPlan("ALL", pgutil.DbInfo{DbSchema: "public"}, pgutil.DbInfo{DbSchema: "public"})
	if err != nil {
		t.Fatalf("building ALL plan: %v", err)
	}

	for _, stepName := range planStepNames(steps) {
		if stepName == "SCHEMA" {
			t.Fatal("did not expect SCHEMA in ALL plan for named schema runs")
		}
	}
}

func TestTablePlusExecutionPlanMatchesCanonicalOrder(t *testing.T) {
	steps, err := buildExecutionPlan("TABLE_PLUS", pgutil.DbInfo{DbSchema: "*"}, pgutil.DbInfo{DbSchema: "*"})
	if err != nil {
		t.Fatalf("building TABLE_PLUS plan: %v", err)
	}

	expected := []string{
		"TABLE",
		"SEQUENCE",
		"COLUMN",
		"INDEX",
		"FOREIGN_KEY",
		"FUNCTION",
		"TRIGGER",
	}

	if got := planStepNames(steps); !reflect.DeepEqual(got, expected) {
		t.Fatalf("unexpected TABLE_PLUS plan:\nwant: %#v\ngot:  %#v", expected, got)
	}
}

func TestExecutionPlansAreDuplicateFree(t *testing.T) {
	testCases := []struct {
		name     string
		planType string
		source   pgutil.DbInfo
		target   pgutil.DbInfo
	}{
		{
			name:     "ALL wildcard",
			planType: "ALL",
			source:   pgutil.DbInfo{DbSchema: "*"},
			target:   pgutil.DbInfo{DbSchema: "*"},
		},
		{
			name:     "ALL cross-schema",
			planType: "ALL",
			source:   pgutil.DbInfo{DbSchema: "s1"},
			target:   pgutil.DbInfo{DbSchema: "s2"},
		},
		{
			name:     "TABLE_PLUS",
			planType: "TABLE_PLUS",
			source:   pgutil.DbInfo{DbSchema: "*"},
			target:   pgutil.DbInfo{DbSchema: "*"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			steps, err := buildExecutionPlan(tc.planType, tc.source, tc.target)
			if err != nil {
				t.Fatalf("building plan: %v", err)
			}

			seen := make(map[string]bool)
			for _, stepName := range planStepNames(steps) {
				if seen[stepName] {
					t.Fatalf("duplicate plan step %s in %s", stepName, tc.name)
				}
				seen[stepName] = true
			}
		})
	}
}
