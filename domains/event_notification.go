package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

const (
	EventLevelInfo    = "info"
	EventLevelWarning = "warning"
	EventLevelError   = "error"

	EventTypeDataSourceConnectionFailed    = "datasource_connection_failed"
	EventTypeDataSourceConnectionRecovered = "datasource_connection_recovered"
	EventTypeSyncRunSuccess                = "sync_run_success"
	EventTypeSyncRunFailed                 = "sync_run_failed"
	EventTypeBackupSuccess                 = "backup_success"
	EventTypeBackupFailed                  = "backup_failed"

	EventSourceDataSource = "datasource"
	EventSourceSyncRun    = "sync_run"
	EventSourceBackup     = "backup"
)

type EventNotification struct {
	commonDomains.BaseDataEntity
	Type       string `json:"type" gorm:"size:64;index;comment:事件类型"`
	Level      string `json:"level" gorm:"size:16;index;comment:事件级别"`
	Title      string `json:"title" gorm:"size:128;comment:标题"`
	Content    string `json:"content" gorm:"type:text;comment:内容"`
	SourceType string `json:"sourceType" gorm:"size:32;index;comment:来源类型"`
	SourceGuid string `json:"sourceGuid" gorm:"size:50;index;comment:来源 GUID"`
	SourceName string `json:"sourceName" gorm:"size:128;comment:来源名称"`
	Read       int    `json:"read" gorm:"index;comment:是否已读"`
	EventTime  int64  `json:"eventTime" gorm:"index;comment:事件时间"`
}

func (EventNotification) TableName() string { return "data_sync_event_notifications" }

func (s EventNotification) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
