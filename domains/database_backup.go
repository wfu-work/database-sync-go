package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

const (
	BackupStatusPending = "pending"
	BackupStatusRunning = "running"
	BackupStatusSuccess = "success"
	BackupStatusFailed  = "failed"

	BackupFormatJSONLZip = "jsonl_zip"
)

type DatabaseBackup struct {
	commonDomains.BaseDataEntity
	DataSourceGuid string `json:"dataSourceGuid" gorm:"size:50;index;comment:数据源 GUID"`
	DataSourceName string `json:"dataSourceName" gorm:"size:128;comment:数据源名称快照"`
	DataSourceType string `json:"dataSourceType" gorm:"size:32;index;comment:数据源类型快照"`
	Database       string `json:"database" gorm:"size:128;comment:数据库名快照"`
	Tables         string `json:"tables" gorm:"type:text;comment:备份表 JSON"`
	Format         string `json:"format" gorm:"size:32;comment:备份格式"`
	Status         string `json:"status" gorm:"size:32;index;comment:备份状态"`
	TotalTables    int    `json:"totalTables" gorm:"comment:表数量"`
	TotalRows      int64  `json:"totalRows" gorm:"comment:数据行数量"`
	FileName       string `json:"fileName" gorm:"size:255;comment:备份文件名"`
	FilePath       string `json:"filePath" gorm:"size:1024;comment:备份文件路径"`
	FileSize       int64  `json:"fileSize" gorm:"comment:备份文件大小"`
	StartTime      int64  `json:"startTime" gorm:"index;comment:开始时间"`
	EndTime        int64  `json:"endTime" gorm:"index;comment:结束时间"`
	DurationMs     int64  `json:"durationMs" gorm:"comment:耗时毫秒"`
	LastError      string `json:"lastError" gorm:"type:text;comment:最后错误"`
	Remark         string `json:"remark" gorm:"size:512;comment:备注"`
}

func (DatabaseBackup) TableName() string { return "data_sync_backups" }

func (s DatabaseBackup) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
