package apis

import (
	"database-sync-go/services"
	"database-sync-go/utils"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type DatabaseRestoreApi struct{}

func (a DatabaseRestoreApi) List(c *gin.Context) {
	params := utils.QueryParams(c)
	items, total, err := restoreService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

func (a DatabaseRestoreApi) Get(c *gin.Context) {
	item, err := restoreService.Get(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

func (a DatabaseRestoreApi) Start(c *gin.Context) {
	var req services.StartDatabaseRestoreRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	if req.BackupGuid == "" {
		req.BackupGuid = c.Param("guid")
	}
	item, err := restoreService.Start(req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}
