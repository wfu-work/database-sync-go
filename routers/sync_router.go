package routers

import "github.com/gin-gonic/gin"

type SyncRouter struct{}

func (r *SyncRouter) InitSyncRouter(router *gin.RouterGroup) {
	tasks := router.Group("sync/tasks")
	{
		tasks.GET("list", syncTaskApi.List)
		tasks.GET("schedules", syncTaskApi.Schedules)
		tasks.POST("schedules/reload", syncTaskApi.ReloadSchedules)
		tasks.POST("validate", syncTaskApi.Validate)
		tasks.POST("", syncTaskApi.Save)
		tasks.GET(":guid", syncTaskApi.Get)
		tasks.PUT(":guid", syncTaskApi.Save)
		tasks.DELETE(":guid", syncTaskApi.Delete)
		tasks.GET(":guid/validate", syncTaskApi.ValidateSaved)
		tasks.POST(":guid/preview", syncTaskApi.Preview)
		tasks.POST(":guid/run", syncTaskApi.Run)
		tasks.POST(":guid/stop", syncTaskApi.Stop)
	}

	templates := router.Group("sync/templates")
	{
		templates.GET("list", syncTemplateApi.List)
		templates.POST("", syncTemplateApi.Save)
		templates.PUT(":guid", syncTemplateApi.Save)
		templates.PUT(":guid/status", syncTemplateApi.UpdateStatus)
		templates.DELETE(":guid", syncTemplateApi.Delete)
		templates.POST("from-task/:guid", syncTemplateApi.SaveFromTask)
	}

	runs := router.Group("sync/runs")
	{
		runs.GET("list", syncRunApi.List)
		runs.GET(":guid", syncRunApi.Get)
		runs.GET(":guid/progress", syncRunApi.Progress)
		runs.GET(":guid/errors", syncRunApi.Errors)
		runs.POST(":guid/retry-errors", syncRunApi.RetryErrors)
	}
}
