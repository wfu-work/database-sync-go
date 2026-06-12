package services

import (
	"strings"
	"testing"

	"database-sync-go/domains"
	"database-sync-go/syncer/connector"

	commonDomains "github.com/wfu-work/nav-common-go-lib/domains"
)

func TestSelectRestoreTablesKeepsBackupOrder(t *testing.T) {
	tables := []backupTableManifest{
		{Name: "metrics", RowCount: 3},
		{Name: "alerts", RowCount: 5},
		{Name: "devices", RowCount: 7},
	}

	selected := selectRestoreTables(tables, []string{"DEVICES", "metrics"})

	if len(selected) != 2 {
		t.Fatalf("expected 2 selected tables, got %d", len(selected))
	}
	if selected[0].Name != "metrics" || selected[1].Name != "devices" {
		t.Fatalf("selected tables should keep manifest order, got %+v", selected)
	}
}

func TestNormalizeRestoreWriteMode(t *testing.T) {
	cases := map[string]string{
		"":        domains.WriteModeInsert,
		"insert":  domains.WriteModeInsert,
		"replace": domains.WriteModeReplace,
		"upsert":  domains.WriteModeUpsert,
		"bad":     domains.WriteModeInsert,
	}
	for input, want := range cases {
		if got := normalizeRestoreWriteMode(input); got != want {
			t.Fatalf("normalizeRestoreWriteMode(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestRestoreGuidIsReadableAndFitsColumn(t *testing.T) {
	guid := restoreGuid(
		&domains.DatabaseBackup{BaseDataEntity: commonDomains.BaseDataEntity{Guid: "bk_20240612_135942_source_database_abcdef"}},
		&domains.DataSource{
			BaseDataEntity: commonDomains.BaseDataEntity{Guid: "target-guid-abcdef123456"},
			Name:           "目标TDengine",
		},
		1718171982000,
	)
	if !strings.HasPrefix(guid, "rs_20240612_135942_") {
		t.Fatalf("restore guid should start with timestamp prefix, got %q", guid)
	}
	if len(guid) > 50 {
		t.Fatalf("restore guid should fit database column, got length %d: %q", len(guid), guid)
	}
}

func TestSumBackupTableRows(t *testing.T) {
	total := sumBackupTableRows([]backupTableManifest{
		{Name: "a", RowCount: 1},
		{Name: "b", RowCount: 2},
		{Name: "c", RowCount: 3},
	})
	if total != 6 {
		t.Fatalf("unexpected row sum: %d", total)
	}
}

func TestTableManifestNames(t *testing.T) {
	names := tableManifestNames([]backupTableManifest{
		{Name: "a", Columns: []connector.ColumnInfo{{Name: "ts"}}},
		{Name: "b"},
	})
	if strings.Join(names, ",") != "a,b" {
		t.Fatalf("unexpected table names: %+v", names)
	}
}
