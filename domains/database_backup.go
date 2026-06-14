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
	DataSourceGuid   string `json:"dataSourceGuid" gorm:"size:50;index;comment:数据源 GUID"`
	DataSourceName   string `json:"dataSourceName" gorm:"size:128;comment:数据源名称快照"`
	DataSourceType   string `json:"dataSourceType" gorm:"size:32;index;comment:数据源类型快照"`
	Database         string `json:"database" gorm:"size:128;comment:数据库名快照"`
	Tables           string `json:"tables" gorm:"type:text;comment:备份表 JSON"`
	ConnectionParams string `json:"connectionParams" gorm:"type:text;comment:本次备份连接参数 JSON"`
	BatchSize        int    `json:"batchSize" gorm:"comment:备份批次大小"`
	RetryTimes       int    `json:"retryTimes" gorm:"comment:批次失败重试次数"`
	RetryIntervalMs  int    `json:"retryIntervalMs" gorm:"comment:批次失败重试间隔毫秒"`
	BackupTimeField  string `json:"backupTimeField" gorm:"size:128;comment:TDengine 备份时间字段"`
	BackupStartTime  string `json:"backupStartTime" gorm:"size:64;comment:TDengine 备份开始时间"`
	BackupEndTime    string `json:"backupEndTime" gorm:"size:64;comment:TDengine 备份结束时间"`
	BackupWindow     string `json:"backupWindow" gorm:"size:32;comment:TDengine 备份时间窗口"`
	CurrentWindow    string `json:"currentWindow" gorm:"size:128;comment:当前备份时间窗口"`
	Format           string `json:"format" gorm:"size:32;comment:备份格式"`
	Status           string `json:"status" gorm:"size:32;index;comment:备份状态"`
	TotalTables      int    `json:"totalTables" gorm:"comment:表数量"`
	FinishedTables   int    `json:"finishedTables" gorm:"comment:已完成表数量"`
	CurrentTable     string `json:"currentTable" gorm:"size:255;comment:当前备份表"`
	CurrentRows      int64  `json:"currentRows" gorm:"comment:当前表已读取行数"`
	CurrentTotal     int64  `json:"currentTotal" gorm:"comment:当前表预估总行数"`
	CurrentBatch     int    `json:"currentBatch" gorm:"comment:当前表读取批次"`
	CurrentStarted   int64  `json:"currentStarted" gorm:"comment:当前表开始时间"`
	TotalRows        int64  `json:"totalRows" gorm:"comment:数据行数量"`
	FileName         string `json:"fileName" gorm:"size:255;comment:备份文件名"`
	FilePath         string `json:"filePath" gorm:"size:1024;comment:备份文件路径"`
	FileSize         int64  `json:"fileSize" gorm:"comment:备份文件大小"`
	StartTime        int64  `json:"startTime" gorm:"index;comment:开始时间"`
	EndTime          int64  `json:"endTime" gorm:"index;comment:结束时间"`
	DurationMs       int64  `json:"durationMs" gorm:"comment:耗时毫秒"`
	LastError        string `json:"lastError" gorm:"type:text;comment:最后错误"`
	Remark           string `json:"remark" gorm:"size:512;comment:备注"`
}

func (DatabaseBackup) TableName() string { return "data_sync_backups" }

func (s DatabaseBackup) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
