package apis

import (
	"database-sync-go/services"
	"database-sync-go/utils"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/response"
)

type DataSourceApi struct{}

// List 分页获取数据源列表
// @Summary 分页获取数据源列表
// @Description 分页获取数据源列表，支持按关键字、类型和状态过滤
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data query domains.PageInfo true "页码, 每页大小, 查询关键字"
// @Param type query string false "数据源类型: mysql 或 tdengine"
// @Param status query int false "状态: 0 禁用, 1 启用"
// @Success 200 {object} response.Response{data=domains.PageResult,msg=string} "分页获取数据源列表, 返回列表、总数、页码和每页数量"
// @Router /datasources/list [get]
func (a DataSourceApi) List(c *gin.Context) {
	params := utils.QueryParams(c)
	items, total, err := dataSourceService.List(params)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(services.PageResult(items, total, params), c)
}

// Save 保存数据源
// @Summary 保存数据源
// @Description 创建或更新 MySQL/TDengine 数据源。创建时不需要传 guid，更新时可通过路径 guid 或请求体 guid 指定
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param data body services.SaveDataSourceRequest true "数据源配置"
// @Success 200 {object} response.Response{data=domains.DataSource,msg=string} "保存数据源成功"
// @Router /datasources [post]
// @Router /datasources/{guid} [put]
func (a DataSourceApi) Save(c *gin.Context) {
	var req services.SaveDataSourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	if guid := c.Param("guid"); guid != "" {
		req.Guid = guid
	}
	item, err := dataSourceService.Save(req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

// Delete 删除数据源
// @Summary 删除数据源
// @Description 删除指定数据源。已被同步任务引用的数据源不允许删除
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "数据源 GUID"
// @Success 200 {object} response.Response{data=bool,msg=string} "删除数据源成功"
// @Router /datasources/{guid} [delete]
func (a DataSourceApi) Delete(c *gin.Context) {
	if err := dataSourceService.Delete(c.Param("guid")); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(true, c)
}

// Test 测试数据源连接
// @Summary 测试数据源连接
// @Description 测试指定 MySQL/TDengine 数据源是否可以连接
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "数据源 GUID"
// @Success 200 {object} response.Response{data=services.PublicDataSource,msg=string} "测试连接成功，返回更新后的连接状态"
// @Router /datasources/{guid}/test [post]
func (a DataSourceApi) Test(c *gin.Context) {
	item, err := dataSourceService.TestConnection(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(item, c)
}

// Tables 获取数据源表列表
// @Summary 获取数据源表列表
// @Description 获取指定数据源下的表列表，MySQL 返回当前数据库表，TDengine 返回普通表和超级表
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "数据源 GUID"
// @Success 200 {object} response.Response{data=[]connector.TableInfo,msg=string} "获取数据源表列表成功"
// @Router /datasources/{guid}/tables [get]
func (a DataSourceApi) Tables(c *gin.Context) {
	items, err := dataSourceService.Tables(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(items, c)
}

// DatabaseDetail 获取数据库详情
// @Summary 获取数据库详情
// @Description 获取健康数据源的数据库基础信息、连接信息、存储信息、表统计和性能指标
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "数据源 GUID"
// @Success 200 {object} response.Response{data=connector.DatabaseDetail,msg=string} "获取数据库详情成功"
// @Router /datasources/{guid}/database-detail [get]
func (a DataSourceApi) DatabaseDetail(c *gin.Context) {
	detail, err := dataSourceService.DatabaseDetail(c.Param("guid"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(detail, c)
}

// Columns 获取数据源表字段
// @Summary 获取数据源表字段
// @Description 获取指定数据源中某张表的字段列表
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "数据源 GUID"
// @Param table query string true "表名"
// @Success 200 {object} response.Response{data=[]connector.ColumnInfo,msg=string} "获取数据源表字段成功"
// @Router /datasources/{guid}/columns [get]
func (a DataSourceApi) Columns(c *gin.Context) {
	items, err := dataSourceService.Columns(c.Param("guid"), c.Query("table"))
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(items, c)
}

// Preview 预览数据源表数据
// @Summary 预览数据源表数据
// @Description 按表名、过滤条件和限制条数预览数据源中的数据
// @Tags 数据同步-数据源
// @Security ApiKeyAuth
// @Accept json
// @Produce json
// @Param guid path string true "数据源 GUID"
// @Param data body services.PreviewTableRequest true "预览参数"
// @Success 200 {object} response.Response{data=services.TablePreviewResult,msg=string} "预览数据源表数据成功"
// @Router /datasources/{guid}/preview [post]
func (a DataSourceApi) Preview(c *gin.Context) {
	var req services.PreviewTableRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	result, err := dataSourceService.Preview(c.Param("guid"), req)
	if err != nil {
		response.FailWithMessage(err.Error(), c)
		return
	}
	response.Ok(result, c)
}
