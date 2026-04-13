//
// Copyright (c) 2014 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
// grant.go provides functions and structures that are common to grant-relationships and grant-attributes
//

package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

var aclRegex = regexp.MustCompile(`([a-zA-Z0-9_]+)*=([rwadDxtXUCcT*]+)/([a-zA-Z0-9_]+)$`)

var permMap = map[string]string{
	"a": "INSERT",
	"r": "SELECT",
	"w": "UPDATE",
	"d": "DELETE",
	"D": "TRUNCATE",
	"x": "REFERENCES",
	"t": "TRIGGER",
	"X": "EXECUTE",
	"U": "USAGE",
	"C": "CREATE",
	"c": "CONNECT",
	"T": "TEMPORARY",
}

// GrantPrivileges captures the privileges on an object and the subset that also
// carry WITH GRANT OPTION.
type GrantPrivileges struct {
	Privileges            []string
	GrantOptionPrivileges []string
}

/*
parseGrants converts an ACL (access control list) line into a role and a slice of permission strings

Example of an ACL: user1=rwa/c42

rolename=xxxx -- privileges granted to a role

	  =xxxx -- privileges granted to PUBLIC
	      r -- SELECT ("read")
	      w -- UPDATE ("write")
	      a -- INSERT ("append")
	      d -- DELETE
	      D -- TRUNCATE
	      x -- REFERENCES
	      t -- TRIGGER
	      X -- EXECUTE
	      U -- USAGE
	      C -- CREATE
	      c -- CONNECT
	      T -- TEMPORARY
	arwdDxt -- ALL PRIVILEGES (for tables, varies for other objects)
	      * -- grant option for preceding privilege
	  /yyyy -- role that granted this privilege
*/
func parseGrants(acl string) (string, []string) {
	role, privilegeModel := parseGrantPrivileges(acl)
	if len(role) == 0 && len(acl) == 0 {
		return role, make([]string, 0)
	}
	return role, privilegeModel.Privileges
}

// parseAcl parses an ACL (access control list) string (e.g. 'c42=aur/postgres') into a role and
// a string made up of one-character permissions
func parseAcl(acl string) (role string, perms string) {
	role, perms = "", ""
	matches := aclRegex.FindStringSubmatch(acl)
	if matches != nil {
		role = matches[1]
		perms = matches[2]
		if len(role) == 0 {
			role = "public"
		}
	}
	return role, perms
}

func parseGrantPrivileges(acl string) (string, GrantPrivileges) {
	role, perms := parseAcl(acl)
	if len(role) == 0 && len(acl) == 0 {
		return role, GrantPrivileges{
			Privileges:            []string{},
			GrantOptionPrivileges: []string{},
		}
	}
	return role, parsePermissionSpec(perms)
}

func parsePermissionSpec(perms string) GrantPrivileges {
	privileges := make(map[string]struct{})
	grantOptionPrivileges := make(map[string]struct{})
	lastPrivilege := ""

	for _, permission := range strings.Split(perms, "") {
		if permission == "*" {
			if len(lastPrivilege) > 0 {
				grantOptionPrivileges[lastPrivilege] = struct{}{}
			}
			continue
		}

		permWord := permMap[permission]
		if len(permWord) > 0 {
			privileges[permWord] = struct{}{}
			lastPrivilege = permWord
			continue
		}

		fmt.Printf("-- Error, found permission character we haven't coded for: %s", permission)
		lastPrivilege = ""
	}

	return GrantPrivileges{
		Privileges:            sortedGrantPrivilegeKeys(privileges),
		GrantOptionPrivileges: sortedGrantPrivilegeKeys(grantOptionPrivileges),
	}
}

type GrantPrivilegeDiff struct {
	GrantPrivileges             []string
	GrantOptionPrivileges       []string
	RevokePrivileges            []string
	RevokeGrantOptionPrivileges []string
}

func diffGrantPrivileges(source GrantPrivileges, target GrantPrivileges) GrantPrivilegeDiff {
	sourcePrivileges := sliceToStringSet(source.Privileges)
	sourceGrantOptions := sliceToStringSet(source.GrantOptionPrivileges)
	targetPrivileges := sliceToStringSet(target.Privileges)
	targetGrantOptions := sliceToStringSet(target.GrantOptionPrivileges)

	missingPrivileges := differenceStringSets(sourcePrivileges, targetPrivileges)
	targetExtraPrivileges := differenceStringSets(targetPrivileges, sourcePrivileges)

	grantPrivileges := make([]string, 0)
	grantOptionPrivileges := make([]string, 0)
	for _, privilege := range sortedGrantPrivilegeKeys(missingPrivileges) {
		if _, hasGrantOption := sourceGrantOptions[privilege]; hasGrantOption {
			grantOptionPrivileges = append(grantOptionPrivileges, privilege)
			continue
		}
		grantPrivileges = append(grantPrivileges, privilege)
	}

	for _, privilege := range sortedGrantPrivilegeKeys(sourceGrantOptions) {
		if _, hasTargetPrivilege := targetPrivileges[privilege]; !hasTargetPrivilege {
			continue
		}
		if _, hasTargetGrantOption := targetGrantOptions[privilege]; hasTargetGrantOption {
			continue
		}
		grantOptionPrivileges = append(grantOptionPrivileges, privilege)
	}

	revokePrivileges := sortedGrantPrivilegeKeys(targetExtraPrivileges)

	revokeGrantOptionPrivileges := make([]string, 0)
	for _, privilege := range sortedGrantPrivilegeKeys(targetGrantOptions) {
		if _, hasSourcePrivilege := sourcePrivileges[privilege]; !hasSourcePrivilege {
			continue
		}
		if _, hasSourceGrantOption := sourceGrantOptions[privilege]; hasSourceGrantOption {
			continue
		}
		revokeGrantOptionPrivileges = append(revokeGrantOptionPrivileges, privilege)
	}

	return GrantPrivilegeDiff{
		GrantPrivileges:             uniqueSortedStrings(grantPrivileges),
		GrantOptionPrivileges:       uniqueSortedStrings(grantOptionPrivileges),
		RevokePrivileges:            revokePrivileges,
		RevokeGrantOptionPrivileges: uniqueSortedStrings(revokeGrantOptionPrivileges),
	}
}

func sortedGrantPrivilegeKeys(values map[string]struct{}) []string {
	keys := make(sort.StringSlice, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	keys.Sort()
	return []string(keys)
}

func sliceToStringSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func differenceStringSets(left map[string]struct{}, right map[string]struct{}) map[string]struct{} {
	diff := make(map[string]struct{})
	for key := range left {
		if _, exists := right[key]; exists {
			continue
		}
		diff[key] = struct{}{}
	}
	return diff
}

func uniqueSortedStrings(values []string) []string {
	set := sliceToStringSet(values)
	return sortedGrantPrivilegeKeys(set)
}
