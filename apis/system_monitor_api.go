package apis

import (
	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type SystemMonitorApi struct{}

func (a SystemMonitorApi) Runtime(c *gin.Context) {
	item, err := systemMonitorService.Runtime()
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}
