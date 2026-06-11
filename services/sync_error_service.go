package services

import (
	"strings"

	"database-sync-go/domains"
	"database-sync-go/utils"

	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type SyncErrorService struct {
	commonServices.CrudService[domains.SyncError]
}

func (s SyncErrorService) WithDB(db *gorm.DB) SyncErrorService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

func (s SyncErrorService) List(params map[string]string) ([]domains.SyncError, int64, error) {
	db := s.DB().Model(&domains.SyncError{})
	if runGuid := strings.TrimSpace(params["runGuid"]); runGuid != "" {
		db = db.Where("run_guid = ?", runGuid)
	}
	if taskGuid := strings.TrimSpace(params["taskGuid"]); taskGuid != "" {
		db = db.Where("task_guid = ?", taskGuid)
	}
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("source_pk LIKE ? OR error_message LIKE ?", like, like)
	}
	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	page := utils.Str2Int(params["page"])
	size := utils.Str2Int(params["size"])
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	var items []domains.SyncError
	err := db.Order("create_time DESC").Limit(size).Offset((page - 1) * size).Find(&items).Error
	return items, total, err
}
