package main

import (
	"reflect"
	"testing"
)

func TestParseAcl(t *testing.T) {
	testCases := []struct {
		name         string
		acl          string
		expectedRole string
		expectedPerm string
	}{
		{name: "simple", acl: "user1=rwa/c42", expectedRole: "user1", expectedPerm: "rwa"},
		{name: "public", acl: "=arwdDxt/c42", expectedRole: "public", expectedPerm: "arwdDxt"},
		{name: "grant option", acl: "user2=r*w*/postgres", expectedRole: "user2", expectedPerm: "r*w*"},
		{name: "empty", acl: "", expectedRole: "", expectedPerm: ""},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			role, perms := parseAcl(testCase.acl)
			if role != testCase.expectedRole {
				t.Fatalf("role mismatch: got %q want %q", role, testCase.expectedRole)
			}
			if perms != testCase.expectedPerm {
				t.Fatalf("perm mismatch: got %q want %q", perms, testCase.expectedPerm)
			}
		})
	}
}

func TestParseGrantPrivileges(t *testing.T) {
	role, privileges := parseGrantPrivileges("=ar*w*/postgres")
	if role != "public" {
		t.Fatalf("role mismatch: got %q want %q", role, "public")
	}

	expectedPrivileges := []string{"INSERT", "SELECT", "UPDATE"}
	if !reflect.DeepEqual(privileges.Privileges, expectedPrivileges) {
		t.Fatalf("privileges mismatch: got %v want %v", privileges.Privileges, expectedPrivileges)
	}

	expectedGrantOptions := []string{"SELECT", "UPDATE"}
	if !reflect.DeepEqual(privileges.GrantOptionPrivileges, expectedGrantOptions) {
		t.Fatalf("grant-option mismatch: got %v want %v", privileges.GrantOptionPrivileges, expectedGrantOptions)
	}
}

func TestDiffGrantPrivileges(t *testing.T) {
	source := GrantPrivileges{
		Privileges:            []string{"INSERT", "SELECT", "UPDATE"},
		GrantOptionPrivileges: []string{"INSERT", "UPDATE"},
	}
	target := GrantPrivileges{
		Privileges:            []string{"INSERT", "DELETE", "UPDATE"},
		GrantOptionPrivileges: []string{"DELETE"},
	}

	diff := diffGrantPrivileges(source, target)

	if !reflect.DeepEqual(diff.GrantPrivileges, []string{"SELECT"}) {
		t.Fatalf("grant diff mismatch: got %v", diff.GrantPrivileges)
	}
	if !reflect.DeepEqual(diff.GrantOptionPrivileges, []string{"INSERT", "UPDATE"}) {
		t.Fatalf("grant option diff mismatch: got %v", diff.GrantOptionPrivileges)
	}
	if !reflect.DeepEqual(diff.RevokePrivileges, []string{"DELETE"}) {
		t.Fatalf("revoke diff mismatch: got %v", diff.RevokePrivileges)
	}
	if !reflect.DeepEqual(diff.RevokeGrantOptionPrivileges, []string{}) {
		t.Fatalf("revoke grant option diff mismatch: got %v", diff.RevokeGrantOptionPrivileges)
	}
}
