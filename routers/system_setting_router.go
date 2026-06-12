package routers

import "github.com/gin-gonic/gin"

type SystemSettingRouter struct{}

func (r *SystemSettingRouter) InitSystemSettingRouter(router *gin.RouterGroup) {
	group := router.Group("system/settings")
	{
		group.GET("sync", systemSettingApi.GetSync)
		group.PUT("sync", systemSettingApi.SaveSync)
	}
}
