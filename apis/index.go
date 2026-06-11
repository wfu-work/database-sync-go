package apis

import "database-sync-go/services"

var ApiGroupApp = new(ApiGroup)

type ApiGroup struct {
	DataSourceApi
	SyncTaskApi
	SyncRunApi
	DatabaseBackupApi
}

var (
	dataSourceService = services.ServiceGroupApp.DataSourceService
	syncTaskService   = services.ServiceGroupApp.SyncTaskService
	syncRunService    = services.ServiceGroupApp.SyncRunService
	syncErrorService  = services.ServiceGroupApp.SyncErrorService
	backupService     = services.ServiceGroupApp.DatabaseBackupService
)
