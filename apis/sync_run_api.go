package apis

import (
	"database-sync-go/services"
	"database-sync-go/utils"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type SyncRunApi struct{}

// List 分页获取同步历史
// @Summary 分页获取同步历史
// @Description 分页获取同步任务运行历史，支持按任务、状态和关键字过滤
// @Tags 数据同步-运行记录
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data query domains.PageInfo true "页码, 每页大小, 查询关键字"
// @Param taskGuid query string false "同步任务 GUID"
// @Param status query string false "运行状态: pending, running, success, failed, canceled"
// @Success 200 {object} response.Response{data=domains.PageResult,msg=string} "分页获取同步历史, 返回列表、总数、页码和每页数量"
// @Router /sync/runs/list [get]
func (a SyncRunApi) List(c *gin.Context) {
	params := utils.QueryParams(c)
	items, total, err := syncRunService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

// Get 获取同步运行详情
// @Summary 获取同步运行详情
// @Description 根据运行记录 GUID 获取同步运行详情
// @Tags 数据同步-运行记录
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步运行记录 GUID"
// @Success 200 {object} response.Response{data=domains.SyncRun,msg=string} "获取同步运行详情成功"
// @Router /sync/runs/{guid} [get]
func (a SyncRunApi) Get(c *gin.Context) {
	item, err := syncRunService.Get(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

// Progress 获取同步进度
// @Summary 获取同步进度
// @Description 获取指定运行记录的同步进度，包括总数、已处理数量、成功数量、失败数量和当前游标
// @Tags 数据同步-运行记录
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步运行记录 GUID"
// @Success 200 {object} response.Response{data=object,msg=string} "获取同步进度成功"
// @Router /sync/runs/{guid}/progress [get]
func (a SyncRunApi) Progress(c *gin.Context) {
	item, err := syncRunService.Get(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	progress := 0.0
	if item.TotalCount > 0 {
		progress = float64(item.ProcessedCount) / float64(item.TotalCount)
		if progress > 1 {
			progress = 1
		}
	}
	response.Ok(gin.H{
		"guid":           item.Guid,
		"taskGuid":       item.TaskGuid,
		"status":         item.Status,
		"totalCount":     item.TotalCount,
		"processedCount": item.ProcessedCount,
		"successCount":   item.SuccessCount,
		"failedCount":    item.FailedCount,
		"progress":       progress,
		"cursorStart":    item.CursorStart,
		"cursorEnd":      item.CursorEnd,
		"lastError":      item.LastError,
	}, c)
}

// Errors 分页获取同步失败明细
// @Summary 分页获取同步失败明细
// @Description 分页获取指定运行记录的失败明细，包括源数据快照和错误原因
// @Tags 数据同步-运行记录
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步运行记录 GUID"
// @Param data query domains.PageInfo true "页码, 每页大小, 查询关键字"
// @Success 200 {object} response.Response{data=domains.PageResult,msg=string} "分页获取同步失败明细, 返回列表、总数、页码和每页数量"
// @Router /sync/runs/{guid}/errors [get]
func (a SyncRunApi) Errors(c *gin.Context) {
	params := utils.QueryParams(c)
	params["runGuid"] = c.Param("guid")
	items, total, err := syncErrorService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

// RetryErrors 重试同步失败明细
// @Summary 重试同步失败明细
// @Description 对指定运行记录中的失败明细重新执行字段映射和目标库写入，并生成新的运行记录
// @Tags 数据同步-运行记录
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "同步运行记录 GUID"
// @Success 200 {object} response.Response{data=domains.SyncRun,msg=string} "重试同步失败明细已开始执行"
// @Router /sync/runs/{guid}/retry-errors [post]
func (a SyncRunApi) RetryErrors(c *gin.Context) {
	run, err := syncRunService.RetryErrors(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(run, c)
}
