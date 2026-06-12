package apis

import "database-sync-go/services"

var ApiGroupApp = new(ApiGroup)

type ApiGroup struct {
	DataSourceApi
	SyncTaskApi
	SyncTemplateApi
	SyncRunApi
	DatabaseBackupApi
	DatabaseRestoreApi
	EventNotificationApi
	SystemSettingApi
	SystemMonitorApi
	WebSocketApi
}

var (
	dataSourceService        = services.ServiceGroupApp.DataSourceService
	syncTaskService          = services.ServiceGroupApp.SyncTaskService
	syncTemplateService      = services.ServiceGroupApp.SyncTemplateService
	syncRunService           = services.ServiceGroupApp.SyncRunService
	syncErrorService         = services.ServiceGroupApp.SyncErrorService
	backupService            = services.ServiceGroupApp.DatabaseBackupService
	restoreService           = services.ServiceGroupApp.DatabaseRestoreService
	eventNotificationService = services.ServiceGroupApp.EventNotificationService
	systemSettingService     = services.ServiceGroupApp.SystemSettingService
	systemMonitorService     = services.ServiceGroupApp.SystemMonitorService
	webSocketService         = &services.ServiceGroupApp.WebSocketService
)
