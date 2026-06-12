package apis

import (
	"database-sync-go/services"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type SystemSettingApi struct{}

func (a SystemSettingApi) GetSync(c *gin.Context) {
	item, err := systemSettingService.GetSyncSetting()
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

func (a SystemSettingApi) SaveSync(c *gin.Context) {
	var req services.SyncSetting
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	item, err := systemSettingService.SaveSyncSetting(req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}
