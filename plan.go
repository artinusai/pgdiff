package main

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/joncrlsn/pgutil"
)

type compareFunc func(*sql.DB, *sql.DB)

type executionStep struct {
	schemaType    string
	compare       compareFunc
	includeInPlan func(pgutil.DbInfo, pgutil.DbInfo) bool
}

func (s executionStep) shouldIncludeInPlan(source pgutil.DbInfo, target pgutil.DbInfo) bool {
	if s.includeInPlan == nil {
		return true
	}
	return s.includeInPlan(source, target)
}

var supportedSchemaTypes = []string{
	"ALL",
	"TABLE_PLUS",
	"SCHEMA",
	"ROLE",
	"SEQUENCE",
	"TABLE",
	"TABLE_COLUMN",
	"VIEW",
	"MATVIEW",
	"COLUMN",
	"INDEX",
	"FOREIGN_KEY",
	"OWNER",
	"GRANT_RELATIONSHIP",
	"GRANT_ATTRIBUTE",
	"TRIGGER",
	"FUNCTION",
}

var executionSteps = map[string]executionStep{
	"SCHEMA": {
		schemaType: "SCHEMA",
		compare:    compareSchematas,
		includeInPlan: func(source pgutil.DbInfo, target pgutil.DbInfo) bool {
			return source.DbSchema == "*" && target.DbSchema == "*"
		},
	},
	"ROLE": {
		schemaType: "ROLE",
		compare:    compareRoles,
	},
	"SEQUENCE": {
		schemaType: "SEQUENCE",
		compare:    compareSequences,
	},
	"TABLE": {
		schemaType: "TABLE",
		compare:    compareTables,
	},
	"COLUMN": {
		schemaType: "COLUMN",
		compare:    compareColumns,
	},
	"TABLE_COLUMN": {
		schemaType: "TABLE_COLUMN",
		compare:    compareTableColumns,
	},
	"INDEX": {
		schemaType: "INDEX",
		compare:    compareIndexes,
	},
	"VIEW": {
		schemaType: "VIEW",
		compare:    compareViews,
	},
	"MATVIEW": {
		schemaType: "MATVIEW",
		compare:    compareMatViews,
	},
	"FOREIGN_KEY": {
		schemaType: "FOREIGN_KEY",
		compare:    compareForeignKeys,
	},
	"FUNCTION": {
		schemaType: "FUNCTION",
		compare:    compareFunctions,
	},
	"TRIGGER": {
		schemaType: "TRIGGER",
		compare:    compareTriggers,
	},
	"OWNER": {
		schemaType: "OWNER",
		compare:    compareOwners,
	},
	"GRANT_RELATIONSHIP": {
		schemaType: "GRANT_RELATIONSHIP",
		compare:    compareGrantRelationships,
	},
	"GRANT_ATTRIBUTE": {
		schemaType: "GRANT_ATTRIBUTE",
		compare:    compareGrantAttributes,
	},
}

var executionPlanDefinitions = map[string][]string{
	"ALL": {
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
	},
	"TABLE_PLUS": {
		"TABLE",
		"SEQUENCE",
		"COLUMN",
		"INDEX",
		"FOREIGN_KEY",
		"FUNCTION",
		"TRIGGER",
	},
}

func schemaTypesText() string {
	return strings.Join(supportedSchemaTypes, ", ")
}

func buildExecutionPlan(schemaType string, source pgutil.DbInfo, target pgutil.DbInfo) ([]executionStep, error) {
	normalizedType := strings.ToUpper(schemaType)

	if step, ok := executionSteps[normalizedType]; ok {
		return []executionStep{step}, nil
	}

	stepNames, ok := executionPlanDefinitions[normalizedType]
	if !ok {
		return nil, fmt.Errorf("Not yet handled: %s", normalizedType)
	}

	steps := make([]executionStep, 0, len(stepNames))
	for _, stepName := range stepNames {
		step, ok := executionSteps[stepName]
		if !ok {
			return nil, fmt.Errorf("Invalid execution plan step %s for %s", stepName, normalizedType)
		}
		if step.shouldIncludeInPlan(source, target) {
			steps = append(steps, step)
		}
	}

	return steps, nil
}

func planStepNames(steps []executionStep) []string {
	names := make([]string, 0, len(steps))
	for _, step := range steps {
		names = append(names, step.schemaType)
	}
	return names
}

func printExecutionPlan(steps []executionStep) {
	for _, step := range steps {
		fmt.Println(step.schemaType)
	}
}

func runExecutionPlan(conn1 *sql.DB, conn2 *sql.DB, steps []executionStep) {
	for _, step := range steps {
		step.compare(conn1, conn2)
	}
}
