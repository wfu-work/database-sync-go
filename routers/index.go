package routers

import (
	"database-sync-go/apis"

	"github.com/gin-gonic/gin"
)

var RouterGroupApp = new(RouterGroup)

type RouterGroup struct {
	DataSourceRouter
	SyncRouter
	DatabaseBackupRouter
	EventNotificationRouter
	SystemSettingRouter
}

var (
	dataSourceApi        = apis.ApiGroupApp.DataSourceApi
	syncTaskApi          = apis.ApiGroupApp.SyncTaskApi
	syncRunApi           = apis.ApiGroupApp.SyncRunApi
	backupApi            = apis.ApiGroupApp.DatabaseBackupApi
	eventNotificationApi = apis.ApiGroupApp.EventNotificationApi
	systemSettingApi     = apis.ApiGroupApp.SystemSettingApi
)

func (r *RouterGroup) InitRouters(publicGroup *gin.RouterGroup, privateGroup *gin.RouterGroup) {
	r.InitDataSourceRouter(publicGroup)
	r.InitSyncRouter(publicGroup)
	r.InitDatabaseBackupRouter(publicGroup)
	r.InitEventNotificationRouter(publicGroup)
	r.InitSystemSettingRouter(publicGroup)
	_ = privateGroup
}
