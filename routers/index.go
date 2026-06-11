package routers

import (
	"database-sync-go/apis"

	"github.com/gin-gonic/gin"
)

var RouterGroupApp = new(RouterGroup)

type RouterGroup struct {
	DataSourceRouter
	SyncRouter
}

var (
	dataSourceApi = apis.ApiGroupApp.DataSourceApi
	syncTaskApi   = apis.ApiGroupApp.SyncTaskApi
	syncRunApi    = apis.ApiGroupApp.SyncRunApi
)

func (r *RouterGroup) InitRouters(publicGroup *gin.RouterGroup, privateGroup *gin.RouterGroup) {
	r.InitDataSourceRouter(publicGroup)
	r.InitSyncRouter(publicGroup)
	_ = privateGroup
}
