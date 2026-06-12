package services

import (
	"errors"
	"strings"

	"database-sync-go/domains"
	"database-sync-go/syncer/manager"
	"database-sync-go/syncer/mapper"
	"database-sync-go/utils"

	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type SyncTemplateService struct {
	commonServices.CrudService[domains.SyncTemplate]
}

func (s SyncTemplateService) WithDB(db *gorm.DB) SyncTemplateService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

type SaveSyncTemplateRequest struct {
	Guid                       string                `json:"guid"`
	Name                       string                `json:"name"`
	Description                string                `json:"description"`
	SourceGuid                 string                `json:"sourceGuid"`
	TargetGuid                 string                `json:"targetGuid"`
	SourceTable                string                `json:"sourceTable"`
	TargetTable                string                `json:"targetTable"`
	Mode                       string                `json:"mode"`
	CursorField                string                `json:"cursorField"`
	CursorValue                string                `json:"cursorValue"`
	BatchSize                  int                   `json:"batchSize"`
	Fields                     []mapper.FieldMapping `json:"fields"`
	FieldMapping               string                `json:"fieldMapping"`
	WriteMode                  string                `json:"writeMode"`
	ConflictKeys               string                `json:"conflictKeys"`
	WhereClause                string                `json:"whereClause"`
	SyncTimeField              string                `json:"syncTimeField"`
	SyncStartDate              string                `json:"syncStartDate"`
	SyncEndDate                string                `json:"syncEndDate"`
	TDengineChildTableTemplate string                `json:"tdengineChildTableTemplate"`
	TDengineChildTableField    string                `json:"tdengineChildTableField"`
	TDengineTags               string                `json:"tdengineTags"`
	TDengineTagMappings        []mapper.TagMapping   `json:"tdengineTagMappings"`
	CronExpr                   string                `json:"cronExpr"`
	ScheduleOn                 *int                  `json:"scheduleOn"`
	Remark                     string                `json:"remark"`
	Status                     *int                  `json:"status"`
}

type SaveTemplateFromTaskRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Remark      string `json:"remark"`
}

func (s SyncTemplateService) List(params map[string]string) ([]domains.SyncTemplate, int64, error) {
	db := s.DB().Model(&domains.SyncTemplate{})
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("name LIKE ? OR guid LIKE ? OR description LIKE ? OR source_table LIKE ? OR target_table LIKE ? OR remark LIKE ?", like, like, like, like, like, like)
	}
	if mode := strings.TrimSpace(params["mode"]); mode != "" {
		db = db.Where("mode = ?", normalizeSyncMode(mode))
	}
	if statusParam := strings.TrimSpace(params["status"]); statusParam != "" {
		db = db.Where("status = ?", utils.Str2Int(statusParam))
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var items []domains.SyncTemplate
	if allParam(params) {
		err := db.Order("update_time DESC").Find(&items).Error
		return items, total, err
	}

	page := utils.Str2Int(params["page"])
	size := utils.Str2Int(params["size"])
	if page <= 0 {
		page = 1
	}
	if size <= 0 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	err := db.Order("update_time DESC").Limit(size).Offset((page - 1) * size).Find(&items).Error
	return items, total, err
}

func (s SyncTemplateService) Save(req SaveSyncTemplateRequest) (*domains.SyncTemplate, error) {
	req = normalizeSyncTemplateRequest(req)
	if req.Name == "" {
		return nil, errors.New("name required")
	}
	if req.Mode != domains.SyncModeFull && req.Mode != domains.SyncModeIncremental {
		return nil, errors.New("unsupported sync mode")
	}
	if req.Mode == domains.SyncModeIncremental && req.CursorField == "" {
		return nil, errors.New("cursorField required for incremental template")
	}
	fieldMapping, err := normalizeFieldMapping(SaveSyncTaskRequest{
		Fields:       req.Fields,
		FieldMapping: req.FieldMapping,
	})
	if err != nil {
		return nil, err
	}
	tdengineTags, err := normalizeTagMapping(SaveSyncTaskRequest{
		TDengineTags:        req.TDengineTags,
		TDengineTagMappings: req.TDengineTagMappings,
	})
	if err != nil {
		return nil, err
	}
	status := int(domains.StatusEnabled)
	if req.Status != nil {
		status = *req.Status
	}
	scheduleOn := int(domains.StatusDisabled)
	if req.ScheduleOn != nil {
		scheduleOn = *req.ScheduleOn
	}
	if scheduleOn == int(domains.StatusEnabled) && req.CronExpr == "" {
		return nil, errors.New("cronExpr required when scheduleOn enabled")
	}
	if scheduleOn == int(domains.StatusEnabled) {
		if err := manager.ValidateCronExpr(req.CronExpr); err != nil {
			return nil, err
		}
	}

	now := domains.NowMilli()
	var row domains.SyncTemplate
	if req.Guid != "" {
		err = s.DB().Where("guid = ?", req.Guid).First(&row).Error
	} else {
		err = gorm.ErrRecordNotFound
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, err
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		row.CreateTime = now
	}

	row.Name = req.Name
	row.Description = req.Description
	row.SourceGuid = req.SourceGuid
	row.TargetGuid = req.TargetGuid
	row.SourceTable = req.SourceTable
	row.TargetTable = req.TargetTable
	row.Mode = req.Mode
	row.CursorField = req.CursorField
	row.CursorValue = req.CursorValue
	row.BatchSize = req.BatchSize
	row.FieldMapping = fieldMapping
	row.WriteMode = req.WriteMode
	row.ConflictKeys = req.ConflictKeys
	row.WhereClause = req.WhereClause
	row.SyncTimeField = req.SyncTimeField
	row.SyncStartDate = req.SyncStartDate
	row.SyncEndDate = req.SyncEndDate
	row.TDengineChildTableTemplate = req.TDengineChildTableTemplate
	row.TDengineChildTableField = req.TDengineChildTableField
	row.TDengineTags = tdengineTags
	row.CronExpr = req.CronExpr
	row.ScheduleOn = scheduleOn
	row.Remark = req.Remark
	row.Status = status
	row.UpdateTime = now

	if err := s.DB().Save(&row).Error; err != nil {
		return nil, err
	}
	return &row, nil
}

func (s SyncTemplateService) SaveFromTask(taskGuid string, req SaveTemplateFromTaskRequest) (*domains.SyncTemplate, error) {
	taskGuid = strings.TrimSpace(taskGuid)
	if taskGuid == "" {
		return nil, errors.New("task guid required")
	}
	var task domains.SyncTask
	if err := s.DB().Where("guid = ?", taskGuid).First(&task).Error; err != nil {
		return nil, errors.New("sync task not found")
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		name = task.Name + " 模板"
	}
	scheduleOn := task.ScheduleOn
	status := int(domains.StatusEnabled)
	return s.Save(SaveSyncTemplateRequest{
		Name:                       name,
		Description:                strings.TrimSpace(req.Description),
		SourceGuid:                 task.SourceGuid,
		TargetGuid:                 task.TargetGuid,
		SourceTable:                task.SourceTable,
		TargetTable:                task.TargetTable,
		Mode:                       task.Mode,
		CursorField:                task.CursorField,
		CursorValue:                task.CursorValue,
		BatchSize:                  task.BatchSize,
		FieldMapping:               task.FieldMapping,
		WriteMode:                  task.WriteMode,
		ConflictKeys:               task.ConflictKeys,
		WhereClause:                task.WhereClause,
		SyncTimeField:              task.SyncTimeField,
		SyncStartDate:              task.SyncStartDate,
		SyncEndDate:                task.SyncEndDate,
		TDengineChildTableTemplate: normalizeChildTableTemplate(task.TDengineChildTableTemplate, task.TDengineChildTableField),
		TDengineChildTableField:    task.TDengineChildTableField,
		TDengineTags:               task.TDengineTags,
		CronExpr:                   task.CronExpr,
		ScheduleOn:                 &scheduleOn,
		Remark:                     strings.TrimSpace(req.Remark),
		Status:                     &status,
	})
}

func (s SyncTemplateService) UpdateStatus(guid string, status int) error {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return errors.New("guid required")
	}
	if status != int(domains.StatusEnabled) && status != int(domains.StatusDisabled) {
		return errors.New("unsupported template status")
	}
	tx := s.DB().Model(&domains.SyncTemplate{}).Where("guid = ?", guid).Updates(map[string]any{
		"status":      status,
		"update_time": domains.NowMilli(),
	})
	if tx.Error != nil {
		return tx.Error
	}
	if tx.RowsAffected == 0 {
		return errors.New("sync template not found")
	}
	return nil
}

func (s SyncTemplateService) Delete(guid string) error {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return errors.New("guid required")
	}
	return s.DB().Unscoped().Where("guid = ?", guid).Delete(&domains.SyncTemplate{}).Error
}

func normalizeSyncTemplateRequest(req SaveSyncTemplateRequest) SaveSyncTemplateRequest {
	req.Guid = strings.TrimSpace(req.Guid)
	req.Name = strings.TrimSpace(req.Name)
	req.Description = strings.TrimSpace(req.Description)
	req.SourceGuid = strings.TrimSpace(req.SourceGuid)
	req.TargetGuid = strings.TrimSpace(req.TargetGuid)
	req.SourceTable = strings.TrimSpace(req.SourceTable)
	req.TargetTable = strings.TrimSpace(req.TargetTable)
	req.Mode = normalizeSyncMode(req.Mode)
	req.CursorField = strings.TrimSpace(req.CursorField)
	req.CursorValue = strings.TrimSpace(req.CursorValue)
	req.FieldMapping = strings.TrimSpace(req.FieldMapping)
	req.WriteMode = normalizeWriteMode(req.WriteMode)
	req.ConflictKeys = strings.TrimSpace(req.ConflictKeys)
	req.WhereClause = strings.TrimSpace(req.WhereClause)
	req.SyncTimeField = strings.TrimSpace(req.SyncTimeField)
	req.SyncStartDate = strings.TrimSpace(req.SyncStartDate)
	req.SyncEndDate = strings.TrimSpace(req.SyncEndDate)
	req.TDengineChildTableTemplate = strings.TrimSpace(req.TDengineChildTableTemplate)
	req.TDengineChildTableField = strings.TrimSpace(req.TDengineChildTableField)
	req.TDengineChildTableTemplate = normalizeChildTableTemplate(req.TDengineChildTableTemplate, req.TDengineChildTableField)
	req.TDengineTags = strings.TrimSpace(req.TDengineTags)
	req.CronExpr = strings.TrimSpace(req.CronExpr)
	req.Remark = strings.TrimSpace(req.Remark)
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}
	return req
}
