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
	SystemMonitorRouter
	WebSocketRouter
}

var (
	dataSourceApi        = apis.ApiGroupApp.DataSourceApi
	syncTaskApi          = apis.ApiGroupApp.SyncTaskApi
	syncTemplateApi      = apis.ApiGroupApp.SyncTemplateApi
	syncRunApi           = apis.ApiGroupApp.SyncRunApi
	backupApi            = apis.ApiGroupApp.DatabaseBackupApi
	restoreApi           = apis.ApiGroupApp.DatabaseRestoreApi
	eventNotificationApi = apis.ApiGroupApp.EventNotificationApi
	systemSettingApi     = apis.ApiGroupApp.SystemSettingApi
	systemMonitorApi     = apis.ApiGroupApp.SystemMonitorApi
	webSocketApi         = apis.ApiGroupApp.WebSocketApi
)

func (r *RouterGroup) InitRouters(publicGroup *gin.RouterGroup, privateGroup *gin.RouterGroup) {
	r.InitWebSocketRouter(publicGroup)
	r.InitDataSourceRouter(publicGroup)
	r.InitSyncRouter(publicGroup)
	r.InitDatabaseBackupRouter(publicGroup)
	r.InitEventNotificationRouter(publicGroup)
	r.InitSystemSettingRouter(publicGroup)
	r.InitSystemMonitorRouter(publicGroup)
	_ = privateGroup
}
