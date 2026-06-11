package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

const (
	RunStatusPending  = "pending"
	RunStatusRunning  = "running"
	RunStatusSuccess  = "success"
	RunStatusFailed   = "failed"
	RunStatusCanceled = "canceled"
)

type SyncRun struct {
	commonDomains.BaseDataEntity
	TaskGuid       string `json:"taskGuid" gorm:"size:50;index;comment:任务 ID"`
	TaskName       string `json:"taskName" gorm:"size:128;comment:任务名称快照"`
	Status         string `json:"status" gorm:"size:32;index;comment:执行状态"`
	TotalCount     int64  `json:"totalCount" gorm:"comment:预计总数"`
	ProcessedCount int64  `json:"processedCount" gorm:"comment:已处理数量"`
	SuccessCount   int64  `json:"successCount" gorm:"comment:成功数量"`
	FailedCount    int64  `json:"failedCount" gorm:"comment:失败数量"`
	StartTime      int64  `json:"startTime" gorm:"index;comment:开始时间"`
	EndTime        int64  `json:"endTime" gorm:"index;comment:结束时间"`
	DurationMs     int64  `json:"durationMs" gorm:"comment:耗时毫秒"`
	CursorStart    string `json:"cursorStart" gorm:"size:255;comment:起始游标"`
	CursorEnd      string `json:"cursorEnd" gorm:"size:255;comment:结束游标"`
	LastError      string `json:"lastError" gorm:"type:text;comment:最后错误"`
}

func (SyncRun) TableName() string { return "data_sync_runs" }

func (s SyncRun) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
