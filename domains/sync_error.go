package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

type SyncError struct {
	commonDomains.BaseDataEntity
	RunGuid      string `json:"runGuid" gorm:"size:50;index;comment:执行记录 ID"`
	TaskGuid     string `json:"taskGuid" gorm:"size:50;index;comment:任务 ID"`
	SourcePK     string `json:"sourcePk" gorm:"size:255;index;comment:源数据标识"`
	SourceData   string `json:"sourceData" gorm:"type:text;comment:源数据快照"`
	ErrorMessage string `json:"errorMessage" gorm:"type:text;comment:错误信息"`
}

func (SyncError) TableName() string { return "data_sync_errors" }

func (s SyncError) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
