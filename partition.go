package main

import "regexp"

var (
	indexOnlyPattern = regexp.MustCompile(`(?i)(\bON\s+)ONLY\s+`)
)

func isTrueString(value string) bool {
	switch value {
	case "true", "TRUE", "t", "T", "1", "yes", "YES":
		return true
	default:
		return false
	}
}

func filterTableRows(rows TableRows) TableRows {
	filtered := make(TableRows, 0, len(rows))
	for _, row := range rows {
		if isTrueString(row["is_inherited"]) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func filterColumnRows(rows ColumnRows) ColumnRows {
	filtered := make(ColumnRows, 0, len(rows))
	for _, row := range rows {
		if isTrueString(row["is_inherited"]) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func indexTableKey(row map[string]string) string {
	if dbInfo1.DbSchema == "*" || dbInfo2.DbSchema == "*" {
		return row["schema_name"] + "." + row["table_name"]
	}
	return row["table_name"]
}

func collectPartitionedIndexTables(rows IndexRows) map[string]struct{} {
	tables := make(map[string]struct{})
	for _, row := range rows {
		if isTrueString(row["has_inherited_children"]) || row["table_relkind"] == "p" {
			tables[indexTableKey(row)] = struct{}{}
		}
	}
	return tables
}

func mergeStringSets(left map[string]struct{}, right map[string]struct{}) map[string]struct{} {
	merged := make(map[string]struct{}, len(left)+len(right))
	for key := range left {
		merged[key] = struct{}{}
	}
	for key := range right {
		merged[key] = struct{}{}
	}
	return merged
}

func filterIndexRows(rows IndexRows, skipTables map[string]struct{}) IndexRows {
	filtered := make(IndexRows, 0, len(rows))
	for _, row := range rows {
		if isTrueString(row["is_inherited"]) {
			continue
		}
		if _, skip := skipTables[indexTableKey(row)]; skip {
			continue
		}
		if row["table_relkind"] == "p" && row["constraint_def"] != "null" {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func filterForeignKeyRows(rows ForeignKeyRows) ForeignKeyRows {
	filtered := make(ForeignKeyRows, 0, len(rows))
	for _, row := range rows {
		if isTrueString(row["is_inherited"]) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func filterTriggerRows(rows TriggerRows) TriggerRows {
	filtered := make(TriggerRows, 0, len(rows))
	for _, row := range rows {
		if isTrueString(row["is_inherited"]) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func normalizeIndexDef(input string) string {
	normalized := stripSchemaIndex(input)
	return indexOnlyPattern.ReplaceAllString(normalized, "${1}")
}
