package apis

import (
	"database-sync-go/services"
	"database-sync-go/utils"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type EventNotificationApi struct{}

func (a EventNotificationApi) List(c *gin.Context) {
	params := utils.QueryParams(c)
	items, total, err := eventNotificationService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

func (a EventNotificationApi) MarkRead(c *gin.Context) {
	if err := eventNotificationService.MarkRead(c.Param("guid")); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}

func (a EventNotificationApi) MarkAllRead(c *gin.Context) {
	if err := eventNotificationService.MarkAllRead(); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}
