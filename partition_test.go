package main

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/joncrlsn/pgutil"
)

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()

	oldStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("creating pipe: %v", err)
	}

	os.Stdout = writer
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := writer.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, reader); err != nil {
		t.Fatalf("reading captured stdout: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatalf("closing reader: %v", err)
	}

	return buf.String()
}

func TestFilterTableRowsRemovesInheritedPartitions(t *testing.T) {
	rows := TableRows{
		{"table_name": "forecast_prediction", "is_inherited": "false"},
		{"table_name": "forecast_prediction_2024", "is_inherited": "true"},
		{"table_name": "forecast_prediction_2025", "is_inherited": "t"},
	}

	filtered := filterTableRows(rows)

	if len(filtered) != 1 {
		t.Fatalf("expected 1 table row after filtering, got %d", len(filtered))
	}
	if filtered[0]["table_name"] != "forecast_prediction" {
		t.Fatalf("unexpected table row kept: %#v", filtered[0])
	}
}

func TestFilterColumnRowsRemovesInheritedPartitionColumns(t *testing.T) {
	rows := ColumnRows{
		{"compare_name": "drywall.forecast_prediction.date_range", "is_inherited": "false"},
		{"compare_name": "drywall.forecast_prediction_2024.date_range", "is_inherited": "true"},
	}

	filtered := filterColumnRows(rows)

	if len(filtered) != 1 {
		t.Fatalf("expected 1 column row after filtering, got %d", len(filtered))
	}
	if filtered[0]["compare_name"] != "drywall.forecast_prediction.date_range" {
		t.Fatalf("unexpected column row kept: %#v", filtered[0])
	}
}

func TestFilterIndexRowsRemovesPartitionArtifacts(t *testing.T) {
	rows := IndexRows{
		{
			"compare_name":   "drywall.forecast_prediction_2024.forecast_prediction_2024_pkey",
			"is_inherited":   "true",
			"table_relkind":  "r",
			"constraint_def": "PRIMARY KEY (date_range, forecast_prediction_id)",
			"table_name":     "forecast_prediction_2024",
			"index_name":     "forecast_prediction_2024_pkey",
		},
		{
			"compare_name":           "drywall.forecast_prediction.forecast_prediction_pkey",
			"is_inherited":           "false",
			"has_inherited_children": "true",
			"table_relkind":          "p",
			"constraint_def":         "PRIMARY KEY (date_range, forecast_prediction_id)",
			"table_name":             "forecast_prediction",
			"index_name":             "forecast_prediction_pkey",
		},
		{
			"compare_name":           "drywall.forecast_prediction.fp_forecast_id_btree",
			"is_inherited":           "false",
			"has_inherited_children": "true",
			"table_relkind":          "p",
			"constraint_def":         "null",
			"table_name":             "forecast_prediction",
			"index_name":             "fp_forecast_id_btree",
		},
		{
			"compare_name":           "drywall.customer.customer_name_idx",
			"is_inherited":           "false",
			"has_inherited_children": "false",
			"table_relkind":          "r",
			"constraint_def":         "null",
			"table_name":             "customer",
			"index_name":             "customer_name_idx",
		},
	}

	skipTables := collectPartitionedIndexTables(rows)
	filtered := filterIndexRows(rows, skipTables)

	if len(filtered) != 1 {
		t.Fatalf("expected 1 index row after filtering, got %d", len(filtered))
	}
	if filtered[0]["index_name"] != "customer_name_idx" {
		t.Fatalf("unexpected index row kept: %#v", filtered[0])
	}
}

func TestFilterForeignKeyRowsRemovesInheritedPartitions(t *testing.T) {
	rows := ForeignKeyRows{
		{"compare_name": "drywall.forecast_prediction.forecast_prediction_forecast_fk", "is_inherited": "false"},
		{"compare_name": "drywall.forecast_prediction_2024.forecast_prediction_forecast_fk", "is_inherited": "true"},
	}

	filtered := filterForeignKeyRows(rows)

	if len(filtered) != 1 {
		t.Fatalf("expected 1 foreign key row after filtering, got %d", len(filtered))
	}
}

func TestFilterIndexRowsSkipsSameTableWhenEitherSideIsPartitioned(t *testing.T) {
	sourceRows := IndexRows{
		{
			"schema_name":            "drywall",
			"table_name":             "forecast_prediction",
			"index_name":             "fp_forecast_id_btree",
			"constraint_def":         "null",
			"table_relkind":          "r",
			"is_inherited":           "false",
			"has_inherited_children": "false",
		},
	}
	targetRows := IndexRows{
		{
			"schema_name":            "drywall_new",
			"table_name":             "forecast_prediction",
			"index_name":             "fp_forecast_id_btree",
			"constraint_def":         "null",
			"table_relkind":          "r",
			"is_inherited":           "false",
			"has_inherited_children": "true",
		},
	}

	skipTables := mergeStringSets(collectPartitionedIndexTables(sourceRows), collectPartitionedIndexTables(targetRows))

	filteredSource := filterIndexRows(sourceRows, skipTables)
	filteredTarget := filterIndexRows(targetRows, skipTables)

	if len(filteredSource) != 0 {
		t.Fatalf("expected source rows to be skipped, got %#v", filteredSource)
	}
	if len(filteredTarget) != 0 {
		t.Fatalf("expected target rows to be skipped, got %#v", filteredTarget)
	}
}

func TestFilterTriggerRowsRemovesInheritedPartitions(t *testing.T) {
	rows := TriggerRows{
		{"compare_name": "drywall.forecast_prediction.trg_set_pg_updated_at", "is_inherited": "false"},
		{"compare_name": "drywall.forecast_prediction_2024.trg_set_pg_updated_at", "is_inherited": "true"},
	}

	filtered := filterTriggerRows(rows)

	if len(filtered) != 1 {
		t.Fatalf("expected 1 trigger row after filtering, got %d", len(filtered))
	}
}

func TestNormalizeIndexDefRemovesOnlyAndSchema(t *testing.T) {
	input := "CREATE INDEX fp_forecast_id_btree ON ONLY drywall_new.forecast_prediction USING btree (forecast_id)"
	expected := "CREATE INDEX fp_forecast_id_btree ON forecast_prediction USING btree (forecast_id)"

	if got := normalizeIndexDef(input); got != expected {
		t.Fatalf("normalizeIndexDef mismatch:\nwant: %s\ngot:  %s", expected, got)
	}
}

func TestIndexSchemaChangeIgnoresOnlyOnPartitionedParent(t *testing.T) {
	oldDbInfo1 := dbInfo1
	oldDbInfo2 := dbInfo2
	defer func() {
		dbInfo1 = oldDbInfo1
		dbInfo2 = oldDbInfo2
	}()

	dbInfo1 = pgutil.DbInfo{DbSchema: "drywall"}
	dbInfo2 = pgutil.DbInfo{DbSchema: "drywall_new"}

	source := &IndexSchema{
		rows: IndexRows{
			{
				"compare_name":   "drywall.forecast_prediction.fp_forecast_id_btree",
				"schema_name":    "drywall",
				"table_name":     "forecast_prediction",
				"index_name":     "fp_forecast_id_btree",
				"index_def":      "CREATE INDEX fp_forecast_id_btree ON drywall.forecast_prediction USING btree (forecast_id)",
				"constraint_def": "null",
			},
		},
		rowNum: 0,
	}
	target := &IndexSchema{
		rows: IndexRows{
			{
				"compare_name":   "drywall_new.forecast_prediction.fp_forecast_id_btree",
				"schema_name":    "drywall_new",
				"table_name":     "forecast_prediction",
				"index_name":     "fp_forecast_id_btree",
				"index_def":      "CREATE INDEX fp_forecast_id_btree ON ONLY drywall_new.forecast_prediction USING btree (forecast_id)",
				"constraint_def": "null",
			},
		},
		rowNum: 0,
	}

	output := captureStdout(t, func() {
		source.Change(target)
	})

	if output != "" {
		t.Fatalf("expected no diff output for ONLY-normalized partition index, got: %s", output)
	}
}
