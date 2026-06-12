package services

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"database-sync-go/domains"

	commonDomains "github.com/wfu-work/nav-common-go-lib/domains"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func TestDatabaseBackupRetryReusesOriginalRecord(t *testing.T) {
	db := newDatabaseBackupTestDB(t)
	service := ServiceGroupApp.DatabaseBackupService.WithDB(db)
	ServiceGroupApp.DataSourceService = ServiceGroupApp.DataSourceService.WithDB(db)
	ServiceGroupApp.EventNotificationService = ServiceGroupApp.EventNotificationService.WithDB(db)

	source := domains.DataSource{
		BaseDataEntity: commonDomains.BaseDataEntity{Guid: "source-retry"},
		Name:           "retry source",
		Type:           "unsupported-test",
		Database:       "demo",
		Status:         int(domains.StatusEnabled),
	}
	if err := db.Create(&source).Error; err != nil {
		t.Fatalf("create datasource failed: %v", err)
	}
	tables, err := json.Marshal([]string{"table_a", "table_b"})
	if err != nil {
		t.Fatalf("marshal tables failed: %v", err)
	}
	row := domains.DatabaseBackup{
		BaseDataEntity: commonDomains.BaseDataEntity{Guid: "backup-retry"},
		DataSourceGuid: source.Guid,
		DataSourceName: "old source name",
		DataSourceType: domains.DataSourceTypeMySQL,
		Database:       "old_db",
		Tables:         string(tables),
		Format:         domains.BackupFormatJSONLZip,
		Status:         domains.BackupStatusFailed,
		TotalTables:    2,
		FinishedTables: 1,
		CurrentTable:   "table_b",
		TotalRows:      12,
		FileName:       "old.zip",
		FilePath:       "/tmp/old.zip",
		FileSize:       123,
		StartTime:      1000,
		EndTime:        2000,
		DurationMs:     1000,
		LastError:      "old error",
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	retried, err := service.Retry(row.Guid)
	if err != nil {
		t.Fatalf("retry backup failed: %v", err)
	}
	if retried.Guid != row.Guid {
		t.Fatalf("retry should reuse original guid, got %q want %q", retried.Guid, row.Guid)
	}
	var count int64
	if err := db.Model(&domains.DatabaseBackup{}).Count(&count).Error; err != nil {
		t.Fatalf("count backups failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("retry should not create a new backup record, got %d records", count)
	}
	if retried.Status != domains.BackupStatusPending && retried.Status != domains.BackupStatusRunning {
		t.Fatalf("retried backup should be pending or running immediately, got %q", retried.Status)
	}
	if retried.FileName != "" || retried.FilePath != "" || retried.LastError != "" {
		t.Fatalf("retry should clear stale file and error fields: %+v", retried)
	}
	if retried.FinishedTables != 0 || retried.CurrentTable != "" || retried.TotalRows != 0 {
		t.Fatalf("retry should reset progress fields: %+v", retried)
	}
	if retried.DataSourceName != source.Name || retried.DataSourceType != source.Type || retried.Database != source.Database {
		t.Fatalf("retry should refresh datasource snapshot: %+v", retried)
	}

	time.Sleep(50 * time.Millisecond)
	if err := db.Model(&domains.DatabaseBackup{}).Count(&count).Error; err != nil {
		t.Fatalf("count backups after async run failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("async retry should still keep a single backup record, got %d records", count)
	}
}

func newDatabaseBackupTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:database_backup_retry?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := db.AutoMigrate(&domains.DataSource{}, &domains.DatabaseBackup{}, &domains.EventNotification{}); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	return db
}

func TestDatabaseBackupDeleteRemovesRecordAndFile(t *testing.T) {
	db := newDatabaseBackupTestDB(t)
	service := ServiceGroupApp.DatabaseBackupService.WithDB(db)

	filePath := filepath.Join(t.TempDir(), "backup.zip")
	if err := os.WriteFile(filePath, []byte("backup"), 0o600); err != nil {
		t.Fatalf("write backup file failed: %v", err)
	}
	row := domains.DatabaseBackup{
		BaseDataEntity: commonDomains.BaseDataEntity{Guid: "backup-delete"},
		Status:         domains.BackupStatusFailed,
		FilePath:       filePath,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	if err := service.Delete(row.Guid); err != nil {
		t.Fatalf("delete backup failed: %v", err)
	}
	var count int64
	if err := db.Model(&domains.DatabaseBackup{}).Where("guid = ?", row.Guid).Count(&count).Error; err != nil {
		t.Fatalf("count backup failed: %v", err)
	}
	if count != 0 {
		t.Fatalf("backup record should be deleted, got %d", count)
	}
	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("backup file should be removed, stat err: %v", err)
	}
}

func TestDatabaseBackupDeleteRejectsRunningRecord(t *testing.T) {
	db := newDatabaseBackupTestDB(t)
	service := ServiceGroupApp.DatabaseBackupService.WithDB(db)

	row := domains.DatabaseBackup{
		BaseDataEntity: commonDomains.BaseDataEntity{Guid: "backup-running"},
		Status:         domains.BackupStatusRunning,
	}
	if err := db.Create(&row).Error; err != nil {
		t.Fatalf("create backup failed: %v", err)
	}

	if err := service.Delete(row.Guid); err == nil {
		t.Fatal("delete running backup should fail")
	}
	var count int64
	if err := db.Model(&domains.DatabaseBackup{}).Where("guid = ?", row.Guid).Count(&count).Error; err != nil {
		t.Fatalf("count backup failed: %v", err)
	}
	if count != 1 {
		t.Fatalf("running backup record should remain, got %d", count)
	}
}

func TestDatabaseBackupGuidIsReadableAndFitsColumn(t *testing.T) {
	guid := backupGuid(&domains.DataSource{
		BaseDataEntity: commonDomains.BaseDataEntity{Guid: "source-guid-abcdef123456"},
		Name:           "生产TDengine",
		Database:       "nav_radar",
	}, 1718171982000)
	if !strings.HasPrefix(guid, "bk_20240612_135942_") {
		t.Fatalf("backup guid should start with timestamp prefix, got %q", guid)
	}
	if !strings.Contains(guid, "nav_radar") {
		t.Fatalf("backup guid should include business database, got %q", guid)
	}
	if len(guid) > 50 {
		t.Fatalf("backup guid should fit database column, got length %d: %q", len(guid), guid)
	}
}

func TestRemovePartialBackupFile(t *testing.T) {
	filePath := filepath.Join(t.TempDir(), "partial.zip")
	if err := os.WriteFile(filePath, []byte("partial"), 0o600); err != nil {
		t.Fatalf("write partial backup file failed: %v", err)
	}

	removePartialBackupFile(filePath)

	if _, err := os.Stat(filePath); !os.IsNotExist(err) {
		t.Fatalf("partial backup file should be removed, stat err: %v", err)
	}
}

func TestMergeBackupParamsOverridesBase(t *testing.T) {
	merged := mergeBackupParams(
		`{"timeout":"30s","scheme":"http"}`,
		`{"timeout":"5m","endpoint":"http://127.0.0.1:6041"}`,
	)
	var params map[string]string
	if err := json.Unmarshal([]byte(merged), &params); err != nil {
		t.Fatalf("merged params should be JSON: %v", err)
	}
	if params["timeout"] != "5m" {
		t.Fatalf("timeout should be overridden, got %q", params["timeout"])
	}
	if params["scheme"] != "http" {
		t.Fatalf("base param should be kept, got %q", params["scheme"])
	}
	if params["endpoint"] != "http://127.0.0.1:6041" {
		t.Fatalf("override param should be added, got %q", params["endpoint"])
	}
}

func TestParseBackupParamsRejectsInvalidJSON(t *testing.T) {
	if _, err := parseBackupParams(`{"timeout":`); err == nil {
		t.Fatal("invalid JSON params should fail")
	}
}
