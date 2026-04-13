package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestNormalizeRoleMemberships(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected []string
	}{
		{name: "empty braces", input: "{}", expected: []string{}},
		{name: "null", input: "null", expected: []string{}},
		{name: "dedupe and sort", input: "{role_b, role_a, role_b}", expected: []string{"role_a", "role_b"}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := normalizeRoleMemberships(testCase.input); !reflect.DeepEqual(got, testCase.expected) {
				t.Fatalf("normalizeRoleMemberships(%q) = %v want %v", testCase.input, got, testCase.expected)
			}
		})
	}
}

func TestRoleSchemaChangeIgnoresMembershipOrderingAndEmptyMemberships(t *testing.T) {
	source := &RoleSchema{
		rows: RoleRows{
			{"rolname": "u2", "memberof": "{role_b,role_a}"},
		},
		rowNum: 0,
	}
	target := &RoleSchema{
		rows: RoleRows{
			{"rolname": "u2", "memberof": "{role_a,role_b}"},
		},
		rowNum: 0,
	}

	if output := captureStdout(t, func() { source.Change(target) }); output != "" {
		t.Fatalf("expected no output for reordered memberships, got: %s", output)
	}

	target.rows[0]["memberof"] = "{}"
	output := captureStdout(t, func() { source.Change(target) })
	if strings.Contains(output, "!=") {
		t.Fatalf("debug output leaked into SQL stream: %s", output)
	}
	if !strings.Contains(output, "GRANT role_a TO u2;") || !strings.Contains(output, "GRANT role_b TO u2;") {
		t.Fatalf("expected grant statements for missing memberships, got: %s", output)
	}
}
