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
		group.GET(":guid", backupApi.Get)
		group.GET(":guid/download", backupApi.Download)
		group.DELETE(":guid", backupApi.Delete)
	}
}
