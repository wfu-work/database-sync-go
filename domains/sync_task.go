package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

const (
	SyncModeFull        = "full"
	SyncModeIncremental = "incremental"

	WriteModeInsert  = "insert"
	WriteModeUpsert  = "upsert"
	WriteModeReplace = "replace"
)

type SyncTask struct {
	commonDomains.BaseDataEntity
	Name          string `json:"name" gorm:"size:128;index;comment:任务名称"`
	SourceGuid    string `json:"sourceGuid" gorm:"size:50;index;comment:源数据源"`
	TargetGuid    string `json:"targetGuid" gorm:"size:50;index;comment:目标数据源"`
	SourceTable   string `json:"sourceTable" gorm:"size:255;comment:源表"`
	TargetTable   string `json:"targetTable" gorm:"size:255;comment:目标表"`
	Mode          string `json:"mode" gorm:"size:32;index;comment:同步模式"`
	CursorField   string `json:"cursorField" gorm:"size:128;comment:增量游标字段"`
	CursorValue   string `json:"cursorValue" gorm:"size:255;comment:当前游标值"`
	BatchSize     int    `json:"batchSize" gorm:"comment:批次大小"`
	FieldMapping  string `json:"fieldMapping" gorm:"type:text;comment:字段映射 JSON"`
	WriteMode     string `json:"writeMode" gorm:"size:32;comment:写入模式"`
	ConflictKeys  string `json:"conflictKeys" gorm:"size:512;comment:冲突字段，逗号分隔"`
	WhereClause   string `json:"whereClause" gorm:"type:text;comment:源表过滤条件"`
	CronExpr      string `json:"cronExpr" gorm:"size:128;comment:定时执行表达式"`
	ScheduleOn    int    `json:"scheduleOn" gorm:"index;comment:是否启用定时"`
	LastRunGuid   string `json:"lastRunGuid" gorm:"size:50;index;comment:最近执行记录"`
	LastRunStatus string `json:"lastRunStatus" gorm:"size:32;index;comment:最近执行状态"`
	Remark        string `json:"remark" gorm:"size:512;comment:备注"`
	Status        int    `json:"status" gorm:"index;comment:状态"`
}

func (SyncTask) TableName() string { return "data_sync_tasks" }

func (s SyncTask) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
