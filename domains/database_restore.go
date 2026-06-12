package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

const (
	RestoreStatusPending = "pending"
	RestoreStatusRunning = "running"
	RestoreStatusSuccess = "success"
	RestoreStatusFailed  = "failed"
)

type DatabaseRestore struct {
	commonDomains.BaseDataEntity
	BackupGuid            string `json:"backupGuid" gorm:"size:50;index;comment:备份 GUID"`
	BackupName            string `json:"backupName" gorm:"size:255;comment:备份文件名快照"`
	SourceDataSourceGuid  string `json:"sourceDataSourceGuid" gorm:"size:50;index;comment:原数据源 GUID"`
	SourceDataSourceName  string `json:"sourceDataSourceName" gorm:"size:128;comment:原数据源名称快照"`
	TargetDataSourceGuid  string `json:"targetDataSourceGuid" gorm:"size:50;index;comment:目标数据源 GUID"`
	TargetDataSourceName  string `json:"targetDataSourceName" gorm:"size:128;comment:目标数据源名称快照"`
	TargetDataSourceType  string `json:"targetDataSourceType" gorm:"size:32;index;comment:目标数据源类型快照"`
	TargetDatabase        string `json:"targetDatabase" gorm:"size:128;comment:目标数据库快照"`
	Tables                string `json:"tables" gorm:"type:text;comment:恢复表 JSON"`
	BatchSize             int    `json:"batchSize" gorm:"comment:恢复批次大小"`
	WriteMode             string `json:"writeMode" gorm:"size:32;comment:写入模式"`
	CreateTable           bool   `json:"createTable" gorm:"comment:目标表不存在时自动创建"`
	TruncateBeforeRestore bool   `json:"truncateBeforeRestore" gorm:"comment:恢复前清空目标表"`
	RetryTimes            int    `json:"retryTimes" gorm:"comment:批次失败重试次数"`
	RetryIntervalMs       int    `json:"retryIntervalMs" gorm:"comment:批次失败重试间隔毫秒"`
	Status                string `json:"status" gorm:"size:32;index;comment:恢复状态"`
	TotalTables           int    `json:"totalTables" gorm:"comment:表数量"`
	FinishedTables        int    `json:"finishedTables" gorm:"comment:已完成表数量"`
	CurrentTable          string `json:"currentTable" gorm:"size:255;comment:当前恢复表"`
	CurrentRows           int64  `json:"currentRows" gorm:"comment:当前表已恢复行数"`
	CurrentTotal          int64  `json:"currentTotal" gorm:"comment:当前表总行数"`
	CurrentBatch          int    `json:"currentBatch" gorm:"comment:当前表写入批次"`
	TotalRows             int64  `json:"totalRows" gorm:"comment:总数据行数量"`
	SuccessRows           int64  `json:"successRows" gorm:"comment:成功写入行数量"`
	FailedRows            int64  `json:"failedRows" gorm:"comment:失败行数量"`
	StartTime             int64  `json:"startTime" gorm:"index;comment:开始时间"`
	EndTime               int64  `json:"endTime" gorm:"index;comment:结束时间"`
	DurationMs            int64  `json:"durationMs" gorm:"comment:耗时毫秒"`
	LastError             string `json:"lastError" gorm:"type:text;comment:最后错误"`
	Remark                string `json:"remark" gorm:"size:512;comment:备注"`
}

func (DatabaseRestore) TableName() string { return "data_sync_restores" }

func (s DatabaseRestore) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
