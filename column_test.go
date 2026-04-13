package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/joncrlsn/pgutil"
)

func TestTableColumnTemplateExecutesWithoutTableTypeField(t *testing.T) {
	buf := new(bytes.Buffer)
	err := tableColumnSqlTemplate.Execute(buf, pgutil.DbInfo{DbSchema: "public"})
	if err != nil {
		t.Fatalf("unexpected template execution error: %v", err)
	}

	sql := buf.String()
	if strings.Contains(sql, "TableType") {
		t.Fatalf("template output still references removed TableType field: %s", sql)
	}
	if !strings.Contains(sql, "b.table_type = 'BASE TABLE'") {
		t.Fatalf("expected base table filter in rendered SQL: %s", sql)
	}
}
