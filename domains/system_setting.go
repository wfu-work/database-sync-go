package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

const SystemSettingKeySync = "sync"

type SystemSetting struct {
	commonDomains.BaseDataEntity
	Key    string `json:"key" gorm:"size:64;uniqueIndex;comment:配置键"`
	Value  string `json:"value" gorm:"type:text;comment:配置 JSON"`
	Remark string `json:"remark" gorm:"size:255;comment:备注"`
}

func (SystemSetting) TableName() string { return "data_sync_system_settings" }

func (s SystemSetting) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
