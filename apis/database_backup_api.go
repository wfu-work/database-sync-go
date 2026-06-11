package apis

import (
	"database-sync-go/services"
	"database-sync-go/utils"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type DatabaseBackupApi struct{}

// List 分页获取数据库备份记录
// @Summary 分页获取数据库备份记录
// @Description 分页获取数据库备份记录，支持按数据源、状态和关键字过滤
// @Tags 数据同步-数据库备份
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data query domains.PageInfo true "页码, 每页大小, 查询关键字"
// @Param dataSourceGuid query string false "数据源 GUID"
// @Param status query string false "备份状态: pending, running, success, failed"
// @Success 200 {object} response.Response{data=domains.PageResult,msg=string} "分页获取数据库备份记录"
// @Router /backups/list [get]
func (a DatabaseBackupApi) List(c *gin.Context) {
	params := utils.QueryParams(c)
	items, total, err := backupService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

// Get 获取数据库备份详情
// @Summary 获取数据库备份详情
// @Description 根据备份 GUID 获取数据库备份详情和当前状态
// @Tags 数据同步-数据库备份
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "备份 GUID"
// @Success 200 {object} response.Response{data=domains.DatabaseBackup,msg=string} "获取数据库备份详情成功"
// @Router /backups/{guid} [get]
func (a DatabaseBackupApi) Get(c *gin.Context) {
	item, err := backupService.Get(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

// Start 启动数据库备份
// @Summary 启动数据库备份
// @Description 对指定数据源启动异步备份。tables 为空时自动备份数据源中的全部表
// @Tags 数据同步-数据库备份
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data body services.StartDatabaseBackupRequest true "数据库备份参数"
// @Success 200 {object} response.Response{data=domains.DatabaseBackup,msg=string} "数据库备份已开始"
// @Router /backups [post]
func (a DatabaseBackupApi) Start(c *gin.Context) {
	var req services.StartDatabaseBackupRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	item, err := backupService.Start(req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

// Download 下载数据库备份文件
// @Summary 下载数据库备份文件
// @Description 下载已成功完成的数据库备份 zip 文件
// @Tags 数据同步-数据库备份
// @Security ApiKeyAuth
// @Accept json
// @Produce application/octet-stream
// @Param guid path string true "备份 GUID"
// @Success 200 {file} file "数据库备份文件"
// @Router /backups/{guid}/download [get]
func (a DatabaseBackupApi) Download(c *gin.Context) {
	filePath, fileName, err := backupService.DownloadInfo(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	c.FileAttachment(filePath, fileName)
}
