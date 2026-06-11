package services

import (
	"errors"
	"strings"

	"database-sync-go/domains"
	"database-sync-go/syncer/manager"
	"database-sync-go/utils"

	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type SyncRunService struct {
	commonServices.CrudService[domains.SyncRun]
}

func (s SyncRunService) WithDB(db *gorm.DB) SyncRunService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

func (s SyncRunService) List(params map[string]string) ([]domains.SyncRun, int64, error) {
	db := s.DB().Model(&domains.SyncRun{})
	if taskGuid := strings.TrimSpace(params["taskGuid"]); taskGuid != "" {
		db = db.Where("task_guid = ?", taskGuid)
	}
	if status := strings.TrimSpace(params["status"]); status != "" {
		db = db.Where("status = ?", status)
	}
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("task_name LIKE ? OR guid LIKE ? OR last_error LIKE ?", like, like, like)
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
	var items []domains.SyncRun
	err := db.Order("start_time DESC, create_time DESC").Limit(size).Offset((page - 1) * size).Find(&items).Error
	return items, total, err
}

func (s SyncRunService) Get(guid string) (*domains.SyncRun, error) {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return nil, errors.New("run guid required")
	}
	var row domains.SyncRun
	if err := s.DB().Where("guid = ?", guid).First(&row).Error; err != nil {
		return nil, errors.New("sync run not found")
	}
	return &row, nil
}

func (s SyncRunService) CreateForTask(task domains.SyncTask) (*domains.SyncRun, error) {
	now := domains.NowMilli()
	run := domains.SyncRun{
		TaskGuid:    task.Guid,
		TaskName:    task.Name,
		Status:      domains.RunStatusPending,
		StartTime:   now,
		CursorStart: task.CursorValue,
	}
	if err := s.DB().Create(&run).Error; err != nil {
		return nil, err
	}
	return &run, nil
}

func (s SyncRunService) RetryErrors(guid string) (*domains.SyncRun, error) {
	return manager.DefaultManager.RetryRunErrors(guid)
}
