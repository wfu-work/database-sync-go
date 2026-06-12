package routers

import "github.com/gin-gonic/gin"

type WebSocketRouter struct{}

func (r *WebSocketRouter) InitWebSocketRouter(router *gin.RouterGroup) {
	router.GET("ws", webSocketApi.Connect)
}
