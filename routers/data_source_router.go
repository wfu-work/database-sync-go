package routers

import "github.com/gin-gonic/gin"

type DataSourceRouter struct{}

func (r *DataSourceRouter) InitDataSourceRouter(router *gin.RouterGroup) {
	group := router.Group("datasources")
	{
		group.GET("", dataSourceApi.List)
		group.GET("list", dataSourceApi.List)
		group.POST("", dataSourceApi.Save)
		group.PUT(":guid", dataSourceApi.Save)
		group.DELETE(":guid", dataSourceApi.Delete)
		group.POST(":guid/test", dataSourceApi.Test)
		group.GET(":guid/tables", dataSourceApi.Tables)
		group.GET(":guid/columns", dataSourceApi.Columns)
		group.POST(":guid/preview", dataSourceApi.Preview)
	}
}
