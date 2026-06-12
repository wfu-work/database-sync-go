package services

import (
	"strings"

	"database-sync-go/utils"

	commonDomains "github.com/wfu-work/nav-common-go-lib/domains"
)

var ServiceGroupApp = new(ServiceGroup)

type ServiceGroup struct {
	DataSourceService
	SyncTaskService
	SyncRunService
	SyncErrorService
	DatabaseBackupService
	EventNotificationService
	SystemSettingService
}

func PageResult(items any, total int64, params map[string]string) commonDomains.PageResult {
	page := utils.Str2Int(params["page"])
	size := utils.Str2Int(params["size"])
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	return commonDomains.PageResult{
		Data:  items,
		Total: total,
		Page:  page,
		Size:  size,
	}
}

func allParam(params map[string]string) bool {
	value := strings.ToLower(utils.FirstNonEmpty(params["all"], params["noPage"]))
	return value == "1" || value == "true" || value == "yes"
}
