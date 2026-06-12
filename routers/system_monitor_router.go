package routers

import "github.com/gin-gonic/gin"

type SystemMonitorRouter struct{}

func (r *SystemMonitorRouter) InitSystemMonitorRouter(router *gin.RouterGroup) {
	group := router.Group("system/monitor")
	{
		group.GET("runtime", systemMonitorApi.Runtime)
	}
}
