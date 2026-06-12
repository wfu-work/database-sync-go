package apis

import "github.com/gin-gonic/gin"

type WebSocketApi struct{}

func (a WebSocketApi) Connect(c *gin.Context) {
	webSocketService.ServeHTTP(c.Writer, c.Request)
}
