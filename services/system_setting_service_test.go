package services

import (
	"testing"

	"database-sync-go/domains"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newSystemSettingTestService(t *testing.T) SystemSettingService {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	if err := db.AutoMigrate(&domains.SystemSetting{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return ServiceGroupApp.SystemSettingService.WithDB(db)
}

func TestSystemSettingServiceGetDefaultSyncSetting(t *testing.T) {
	service := newSystemSettingTestService(t)
	setting, err := service.GetSyncSetting()
	if err != nil {
		t.Fatalf("get default sync setting: %v", err)
	}
	if setting.BackupBatchSize != 1000 {
		t.Fatalf("unexpected default backup batch size: %d", setting.BackupBatchSize)
	}
	if setting.TdengineParams == "" || setting.MysqlParams == "" {
		t.Fatal("expected default database params")
	}
}

func TestSystemSettingServiceSaveAndGetSyncSetting(t *testing.T) {
	service := newSystemSettingTestService(t)
	saved, err := service.SaveSyncSetting(SyncSetting{
		BackupBatchSize:           500,
		BackupRetryTimes:          2,
		BackupRetryIntervalMs:     1500,
		TdengineParams:            `{"timeout":"10m"}`,
		MysqlParams:               `{"timeout":"3m"}`,
		SyncBatchSize:             800,
		MonitorRefreshSeconds:     8,
		NotificationRetentionDays: 14,
		BackupRetentionDays:       21,
		LogLevel:                  "warn",
	})
	if err != nil {
		t.Fatalf("save sync setting: %v", err)
	}
	if saved.UpdateTime == 0 {
		t.Fatal("expected update time")
	}

	got, err := service.GetSyncSetting()
	if err != nil {
		t.Fatalf("get sync setting: %v", err)
	}
	if got.BackupBatchSize != 500 || got.LogLevel != "warn" || got.TdengineParams != `{"timeout":"10m"}` {
		t.Fatalf("unexpected saved setting: %+v", got)
	}
}

func TestSystemSettingServiceRejectInvalidJSONParams(t *testing.T) {
	service := newSystemSettingTestService(t)
	_, err := service.SaveSyncSetting(SyncSetting{
		BackupBatchSize:           1000,
		BackupRetryTimes:          3,
		BackupRetryIntervalMs:     3000,
		TdengineParams:            `[1,2]`,
		MysqlParams:               `{}`,
		SyncBatchSize:             1000,
		MonitorRefreshSeconds:     5,
		NotificationRetentionDays: 30,
		BackupRetentionDays:       30,
		LogLevel:                  "info",
	})
	if err == nil {
		t.Fatal("expected invalid tdengine params error")
	}
}
