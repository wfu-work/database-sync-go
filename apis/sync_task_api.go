package apis

import (
	"io"

	"database-sync-go/services"
	"database-sync-go/utils"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type SyncTaskApi struct{}

// List 分页获取同步任务列表
// @Summary 分页获取同步任务列表
// @Description 分页获取同步任务列表，支持按关键字、同步模式和状态过滤
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data query domains.PageInfo true "页码, 每页大小, 查询关键字"
// @Param mode query string false "同步模式: full 或 incremental"
// @Param status query int false "状态: 0 禁用, 1 启用"
// @Param scheduleOn query int false "定时状态: 0 关闭, 1 开启"
// @Success 200 {object} response.Response{data=domains.PageResult,msg=string} "分页获取同步任务列表, 返回列表、总数、页码和每页数量"
// @Router /sync/tasks/list [get]
func (a SyncTaskApi) List(c *gin.Context) {
	params := utils.QueryParams(c)
	items, total, err := syncTaskService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

// Save 保存同步任务
// @Summary 保存同步任务
// @Description 创建或更新同步任务，支持字段映射、默认值、转换函数、全量同步和增量同步配置
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data body services.SaveSyncTaskRequest true "同步任务配置"
// @Success 200 {object} response.Response{data=domains.SyncTask,msg=string} "保存同步任务成功"
// @Router /sync/tasks [post]
// @Router /sync/tasks/{guid} [put]
func (a SyncTaskApi) Save(c *gin.Context) {
	var req services.SaveSyncTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	if guid := c.Param("guid"); guid != "" {
		req.Guid = guid
	}
	item, err := syncTaskService.Save(req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

// Delete 删除同步任务
// @Summary 删除同步任务
// @Description 删除指定同步任务
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步任务 GUID"
// @Success 200 {object} response.Response{data=bool,msg=string} "删除同步任务成功"
// @Router /sync/tasks/{guid} [delete]
func (a SyncTaskApi) Delete(c *gin.Context) {
	if err := syncTaskService.Delete(c.Param("guid")); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}

// Run 手动执行同步任务
// @Summary 手动执行同步任务
// @Description 手动触发指定同步任务，返回本次同步运行记录
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步任务 GUID"
// @Success 200 {object} response.Response{data=domains.SyncRun,msg=string} "同步任务已开始执行"
// @Router /sync/tasks/{guid}/run [post]
func (a SyncTaskApi) Run(c *gin.Context) {
	run, err := syncTaskService.Run(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(run, c)
}

// Stop 停止同步任务
// @Summary 停止同步任务
// @Description 停止指定任务当前正在执行的同步运行
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步任务 GUID"
// @Success 200 {object} response.Response{data=bool,msg=string} "同步任务已请求停止"
// @Router /sync/tasks/{guid}/stop [post]
func (a SyncTaskApi) Stop(c *gin.Context) {
	if err := syncTaskService.Stop(c.Param("guid")); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}

// ReloadSchedules 重载同步任务调度
// @Summary 重载同步任务调度
// @Description 重新加载所有已启用定时的同步任务
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Success 200 {object} response.Response{data=bool,msg=string} "重载同步任务调度成功"
// @Router /sync/tasks/schedules/reload [post]
func (a SyncTaskApi) ReloadSchedules(c *gin.Context) {
	if err := syncTaskService.ReloadSchedules(); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}

// Schedules 获取同步任务调度列表
// @Summary 获取同步任务调度列表
// @Description 获取当前运行中的同步任务调度列表
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Success 200 {object} response.Response{data=[]manager.ScheduleItem,msg=string} "获取同步任务调度列表成功"
// @Router /sync/tasks/schedules [get]
func (a SyncTaskApi) Schedules(c *gin.Context) {
	response.Ok(syncTaskService.ScheduleItems(), c)
}

// Validate 校验同步任务配置
// @Summary 校验同步任务配置
// @Description 校验未保存的同步任务配置，包括数据源、表、字段映射、游标字段和过滤条件
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data body services.SaveSyncTaskRequest true "同步任务配置"
// @Success 200 {object} response.Response{data=services.ValidateSyncTaskResult,msg=string} "校验同步任务配置成功"
// @Router /sync/tasks/validate [post]
func (a SyncTaskApi) Validate(c *gin.Context) {
	var req services.SaveSyncTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	result, err := syncTaskService.ValidateRequest(req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(result, c)
}

// ValidateSaved 校验已保存同步任务
// @Summary 校验已保存同步任务
// @Description 校验指定已保存同步任务，包括数据源、表、字段映射、游标字段和过滤条件
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步任务 GUID"
// @Success 200 {object} response.Response{data=services.ValidateSyncTaskResult,msg=string} "校验已保存同步任务成功"
// @Router /sync/tasks/{guid}/validate [get]
func (a SyncTaskApi) ValidateSaved(c *gin.Context) {
	result, err := syncTaskService.ValidateSaved(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(result, c)
}

// Preview 预览同步任务数据
// @Summary 预览同步任务数据
// @Description 预览指定同步任务的源数据和字段映射后的目标数据样例
// @Tags 数据同步-任务
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步任务 GUID"
// @Param data body services.SyncTaskPreviewRequest false "预览参数"
// @Success 200 {object} response.Response{data=services.SyncTaskPreviewResult,msg=string} "预览同步任务数据成功"
// @Router /sync/tasks/{guid}/preview [post]
func (a SyncTaskApi) Preview(c *gin.Context) {
	var req services.SyncTaskPreviewRequest
	if err := c.ShouldBindJSON(&req); err != nil && err != io.EOF {
		response.FailWithMessage(err.Error(), c)
		return
	}
	result, err := syncTaskService.Preview(c.Param("guid"), req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(result, c)
}
