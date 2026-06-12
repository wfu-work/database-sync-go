package routers

import "github.com/gin-gonic/gin"

type EventNotificationRouter struct{}

func (r *EventNotificationRouter) InitEventNotificationRouter(router *gin.RouterGroup) {
	group := router.Group("events/notifications")
	{
		group.GET("", eventNotificationApi.List)
		group.GET("list", eventNotificationApi.List)
		group.POST(":guid/read", eventNotificationApi.MarkRead)
		group.POST("read-all", eventNotificationApi.MarkAllRead)
	}
}
