package apis

import (
	"database-sync-go/services"
	"database-sync-go/utils"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type SyncTemplateApi struct{}

type UpdateSyncTemplateStatusRequest struct {
	Status int `json:"status"`
}

func (a SyncTemplateApi) List(c *gin.Context) {
	params := utils.QueryParams(c)
	items, total, err := syncTemplateService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

func (a SyncTemplateApi) Save(c *gin.Context) {
	var req services.SaveSyncTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	if guid := c.Param("guid"); guid != "" {
		req.Guid = guid
	}
	item, err := syncTemplateService.Save(req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

func (a SyncTemplateApi) SaveFromTask(c *gin.Context) {
	var req services.SaveTemplateFromTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	item, err := syncTemplateService.SaveFromTask(c.Param("guid"), req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

func (a SyncTemplateApi) UpdateStatus(c *gin.Context) {
	var req UpdateSyncTemplateStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	if err := syncTemplateService.UpdateStatus(c.Param("guid"), req.Status); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}

func (a SyncTemplateApi) Delete(c *gin.Context) {
	if err := syncTemplateService.Delete(c.Param("guid")); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}
