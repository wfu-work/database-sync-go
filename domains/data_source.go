package domains

import commonDomains "github.com/wfu-work/nav-common-go-lib/domains"

const (
	DataSourceTypeMySQL    = "mysql"
	DataSourceTypeTDengine = "tdengine"

	DataSourceConnectionUnknown   = "unknown"
	DataSourceConnectionChecking  = "checking"
	DataSourceConnectionConnected = "connected"
	DataSourceConnectionFailed    = "failed"
)

type DataSource struct {
	commonDomains.BaseDataEntity
	Name                string `json:"name" gorm:"size:128;index;comment:数据源名称"`
	Type                string `json:"type" gorm:"size:32;index;comment:数据源类型"`
	Host                string `json:"host" gorm:"size:255;comment:主机"`
	Port                int    `json:"port" gorm:"comment:端口"`
	Username            string `json:"username" gorm:"size:128;comment:用户名"`
	Password            string `json:"password" gorm:"size:512;comment:密码"`
	Database            string `json:"database" gorm:"size:128;comment:数据库名"`
	Params              string `json:"params" gorm:"type:text;comment:额外连接参数 JSON"`
	Remark              string `json:"remark" gorm:"size:512;comment:备注"`
	ConnectionStatus    string `json:"connectionStatus" gorm:"size:32;index;comment:连接状态"`
	ConnectionCheckedAt int64  `json:"connectionCheckedAt" gorm:"index;comment:最后连接检查时间"`
	ConnectionError     string `json:"connectionError" gorm:"type:text;comment:最后连接错误"`
	Status              int    `json:"status" gorm:"index;comment:状态"`
}

func (DataSource) TableName() string { return "data_sync_sources" }

func (s DataSource) GetBaseData() commonDomains.BaseDataEntity {
	return s.BaseDataEntity
}
