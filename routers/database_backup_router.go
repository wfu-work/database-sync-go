package routers

import "github.com/gin-gonic/gin"

type DatabaseBackupRouter struct{}

func (r *DatabaseBackupRouter) InitDatabaseBackupRouter(router *gin.RouterGroup) {
	group := router.Group("backups")
	{
		group.GET("", backupApi.List)
		group.GET("list", backupApi.List)
		group.POST("", backupApi.Start)
		group.POST(":guid/retry", backupApi.Retry)
		group.POST(":guid/restore", restoreApi.Start)
		group.GET(":guid", backupApi.Get)
		group.GET(":guid/download", backupApi.Download)
		group.DELETE(":guid", backupApi.Delete)
	}
	restoreGroup := router.Group("backup-restores")
	{
		restoreGroup.GET("", restoreApi.List)
		restoreGroup.GET("list", restoreApi.List)
		restoreGroup.GET(":guid", restoreApi.Get)
	}
}
