package services

import (
	"encoding/json"
	"errors"
	"strings"

	"database-sync-go/domains"

	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type SystemSettingService struct {
	commonServices.CrudService[domains.SystemSetting]
}

func (s SystemSettingService) WithDB(db *gorm.DB) SystemSettingService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

type SyncSetting struct {
	BackupBatchSize           int    `json:"backupBatchSize"`
	BackupRetryTimes          int    `json:"backupRetryTimes"`
	BackupRetryIntervalMs     int    `json:"backupRetryIntervalMs"`
	BackupTimeoutSeconds      int    `json:"backupTimeoutSeconds"`
	TdengineParams            string `json:"tdengineParams"`
	MysqlParams               string `json:"mysqlParams"`
	SyncBatchSize             int    `json:"syncBatchSize"`
	SyncRetryTimes            int    `json:"syncRetryTimes"`
	SyncRetryIntervalMs       int    `json:"syncRetryIntervalMs"`
	SyncTimeoutSeconds        int    `json:"syncTimeoutSeconds"`
	RestoreBatchSize          int    `json:"restoreBatchSize"`
	RestoreWriteMode          string `json:"restoreWriteMode"`
	RestoreCreateTable        bool   `json:"restoreCreateTable"`
	RestoreTruncateBefore     bool   `json:"restoreTruncateBefore"`
	HealthCheckIntervalSec    int    `json:"healthCheckIntervalSec"`
	MonitorRefreshSeconds     int    `json:"monitorRefreshSeconds"`
	RunRetentionDays          int    `json:"runRetentionDays"`
	NotificationRetentionDays int    `json:"notificationRetentionDays"`
	BackupRetentionDays       int    `json:"backupRetentionDays"`
	RestoreRetentionDays      int    `json:"restoreRetentionDays"`
	LogLevel                  string `json:"logLevel"`
	UpdateTime                int64  `json:"updateTime"`
}

func DefaultSyncSetting() SyncSetting {
	return SyncSetting{
		BackupBatchSize:           1000,
		BackupRetryTimes:          3,
		BackupRetryIntervalMs:     3000,
		BackupTimeoutSeconds:      21600,
		TdengineParams:            `{"timeout":"5m"}`,
		MysqlParams:               `{"timeout":"5m","readTimeout":"5m","writeTimeout":"5m"}`,
		SyncBatchSize:             1000,
		SyncRetryTimes:            2,
		SyncRetryIntervalMs:       1500,
		SyncTimeoutSeconds:        21600,
		RestoreBatchSize:          1000,
		RestoreWriteMode:          domains.WriteModeInsert,
		RestoreCreateTable:        true,
		RestoreTruncateBefore:     false,
		HealthCheckIntervalSec:    60,
		MonitorRefreshSeconds:     5,
		RunRetentionDays:          30,
		NotificationRetentionDays: 30,
		BackupRetentionDays:       30,
		RestoreRetentionDays:      30,
		LogLevel:                  "info",
	}
}

func (s SystemSettingService) GetSyncSetting() (SyncSetting, error) {
	setting := DefaultSyncSetting()
	db := s.DB()
	if db == nil {
		return setting, nil
	}
	var row domains.SystemSetting
	err := db.Where("key = ?", domains.SystemSettingKeySync).First(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return setting, nil
	}
	if err != nil {
		return setting, err
	}
	if strings.TrimSpace(row.Value) == "" {
		setting.UpdateTime = row.UpdateTime
		return setting, nil
	}
	if err := json.Unmarshal([]byte(row.Value), &setting); err != nil {
		return DefaultSyncSetting(), err
	}
	setting = normalizeSyncSetting(setting)
	setting.UpdateTime = row.UpdateTime
	return setting, nil
}

func (s SystemSettingService) SaveSyncSetting(req SyncSetting) (SyncSetting, error) {
	req = normalizeSyncSetting(req)
	if err := validateJSONObj(req.TdengineParams); err != nil {
		return req, errors.New("tdengine params must be json object")
	}
	if err := validateJSONObj(req.MysqlParams); err != nil {
		return req, errors.New("mysql params must be json object")
	}
	raw, err := json.Marshal(req)
	if err != nil {
		return req, err
	}
	now := domains.NowMilli()
	var row domains.SystemSetting
	err = s.DB().Where("key = ?", domains.SystemSettingKeySync).First(&row).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return req, err
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		row.CreateTime = now
		row.Key = domains.SystemSettingKeySync
	}
	row.Value = string(raw)
	row.Remark = "同步设置"
	row.UpdateTime = now
	if err := s.DB().Save(&row).Error; err != nil {
		return req, err
	}
	req.UpdateTime = now
	return req, nil
}

func normalizeSyncSetting(value SyncSetting) SyncSetting {
	defaults := DefaultSyncSetting()
	if value.BackupBatchSize <= 0 {
		value.BackupBatchSize = defaults.BackupBatchSize
	}
	if value.BackupRetryTimes < 0 {
		value.BackupRetryTimes = defaults.BackupRetryTimes
	}
	if value.BackupRetryIntervalMs < 0 {
		value.BackupRetryIntervalMs = defaults.BackupRetryIntervalMs
	}
	if value.BackupTimeoutSeconds <= 0 {
		value.BackupTimeoutSeconds = defaults.BackupTimeoutSeconds
	}
	if strings.TrimSpace(value.TdengineParams) == "" {
		value.TdengineParams = defaults.TdengineParams
	}
	if strings.TrimSpace(value.MysqlParams) == "" {
		value.MysqlParams = defaults.MysqlParams
	}
	if value.SyncBatchSize <= 0 {
		value.SyncBatchSize = defaults.SyncBatchSize
	}
	if value.SyncRetryTimes < 0 {
		value.SyncRetryTimes = defaults.SyncRetryTimes
	}
	if value.SyncRetryIntervalMs < 0 {
		value.SyncRetryIntervalMs = defaults.SyncRetryIntervalMs
	}
	if value.SyncTimeoutSeconds <= 0 {
		value.SyncTimeoutSeconds = defaults.SyncTimeoutSeconds
	}
	if value.RestoreBatchSize <= 0 {
		value.RestoreBatchSize = defaults.RestoreBatchSize
	}
	value.RestoreWriteMode = strings.ToLower(strings.TrimSpace(value.RestoreWriteMode))
	switch value.RestoreWriteMode {
	case domains.WriteModeInsert, domains.WriteModeReplace, domains.WriteModeUpsert:
	default:
		value.RestoreWriteMode = defaults.RestoreWriteMode
	}
	if value.HealthCheckIntervalSec < 10 {
		value.HealthCheckIntervalSec = defaults.HealthCheckIntervalSec
	}
	if value.MonitorRefreshSeconds < 3 {
		value.MonitorRefreshSeconds = defaults.MonitorRefreshSeconds
	}
	if value.RunRetentionDays <= 0 {
		value.RunRetentionDays = defaults.RunRetentionDays
	}
	if value.NotificationRetentionDays <= 0 {
		value.NotificationRetentionDays = defaults.NotificationRetentionDays
	}
	if value.BackupRetentionDays <= 0 {
		value.BackupRetentionDays = defaults.BackupRetentionDays
	}
	if value.RestoreRetentionDays <= 0 {
		value.RestoreRetentionDays = defaults.RestoreRetentionDays
	}
	value.LogLevel = strings.ToLower(strings.TrimSpace(value.LogLevel))
	switch value.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		value.LogLevel = defaults.LogLevel
	}
	return value
}

func validateJSONObj(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	var parsed map[string]any
	return json.Unmarshal([]byte(value), &parsed)
}
