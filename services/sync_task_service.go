package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/connector"
	"database-sync-go/syncer/manager"
	"database-sync-go/syncer/mapper"
	"database-sync-go/utils"

	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type SyncTaskService struct {
	commonServices.CrudService[domains.SyncTask]
}

func (s SyncTaskService) WithDB(db *gorm.DB) SyncTaskService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

type SaveSyncTaskRequest struct {
	Guid         string                `json:"guid"`
	Name         string                `json:"name"`
	SourceGuid   string                `json:"sourceGuid"`
	TargetGuid   string                `json:"targetGuid"`
	SourceTable  string                `json:"sourceTable"`
	TargetTable  string                `json:"targetTable"`
	Mode         string                `json:"mode"`
	CursorField  string                `json:"cursorField"`
	CursorValue  string                `json:"cursorValue"`
	BatchSize    int                   `json:"batchSize"`
	Fields       []mapper.FieldMapping `json:"fields"`
	FieldMapping string                `json:"fieldMapping"`
	WriteMode    string                `json:"writeMode"`
	ConflictKeys string                `json:"conflictKeys"`
	WhereClause  string                `json:"whereClause"`
	CronExpr     string                `json:"cronExpr"`
	ScheduleOn   *int                  `json:"scheduleOn"`
	Remark       string                `json:"remark"`
	Status       *int                  `json:"status"`
}

type ValidateSyncTaskResult struct {
	Valid                bool                   `json:"valid"`
	Errors               []string               `json:"errors"`
	SourceColumns        []connector.ColumnInfo `json:"sourceColumns"`
	TargetColumns        []connector.ColumnInfo `json:"targetColumns"`
	MissingSourceFields  []string               `json:"missingSourceFields"`
	MissingTargetFields  []string               `json:"missingTargetFields"`
	EstimatedSourceCount int64                  `json:"estimatedSourceCount"`
	FieldMappingRowCount int                    `json:"fieldMappingRowCount"`
	SourceDatasourceGuid string                 `json:"sourceDatasourceGuid"`
	TargetDatasourceGuid string                 `json:"targetDatasourceGuid"`
	SourceTable          string                 `json:"sourceTable"`
	TargetTable          string                 `json:"targetTable"`
}

type SyncTaskPreviewRequest struct {
	Limit int `json:"limit" form:"limit"`
}

type SyncTaskPreviewResult struct {
	SourceRows []mapper.Row `json:"sourceRows"`
	MappedRows []mapper.Row `json:"mappedRows"`
	Count      int          `json:"count"`
}

func (s SyncTaskService) List(params map[string]string) ([]domains.SyncTask, int64, error) {
	db := s.DB().Model(&domains.SyncTask{})
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("name LIKE ? OR guid LIKE ? OR source_table LIKE ? OR target_table LIKE ? OR remark LIKE ?", like, like, like, like, like)
	}
	if mode := strings.TrimSpace(params["mode"]); mode != "" {
		db = db.Where("mode = ?", normalizeSyncMode(mode))
	}
	if statusParam := strings.TrimSpace(params["status"]); statusParam != "" {
		db = db.Where("status = ?", utils.Str2Int(statusParam))
	}
	if scheduleOn := strings.TrimSpace(params["scheduleOn"]); scheduleOn != "" {
		db = db.Where("schedule_on = ?", utils.Str2Int(scheduleOn))
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var items []domains.SyncTask
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
	err := db.Order("update_time DESC").Limit(size).Offset((page - 1) * size).Find(&items).Error
	return items, total, err
}

func (s SyncTaskService) Save(req SaveSyncTaskRequest) (*domains.SyncTask, error) {
	req.Guid = strings.TrimSpace(req.Guid)
	req.Name = strings.TrimSpace(req.Name)
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
	req.CronExpr = strings.TrimSpace(req.CronExpr)
	req.Remark = strings.TrimSpace(req.Remark)

	if req.Name == "" {
		return nil, errors.New("name required")
	}
	if req.SourceGuid == "" {
		return nil, errors.New("sourceGuid required")
	}
	if req.TargetGuid == "" {
		return nil, errors.New("targetGuid required")
	}
	if req.SourceTable == "" {
		return nil, errors.New("sourceTable required")
	}
	if req.TargetTable == "" {
		return nil, errors.New("targetTable required")
	}
	if req.Mode == "" {
		req.Mode = domains.SyncModeFull
	}
	if req.Mode != domains.SyncModeFull && req.Mode != domains.SyncModeIncremental {
		return nil, errors.New("unsupported sync mode")
	}
	if req.Mode == domains.SyncModeIncremental && req.CursorField == "" {
		return nil, errors.New("cursorField required for incremental task")
	}
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}
	if req.WriteMode == "" {
		req.WriteMode = domains.WriteModeInsert
	}

	fieldMapping, err := normalizeFieldMapping(req)
	if err != nil {
		return nil, err
	}
	if _, err := ServiceGroupApp.DataSourceService.GetEnabled(req.SourceGuid); err != nil {
		return nil, err
	}
	if _, err := ServiceGroupApp.DataSourceService.GetEnabled(req.TargetGuid); err != nil {
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
	var row domains.SyncTask
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
	row.CronExpr = req.CronExpr
	row.ScheduleOn = scheduleOn
	row.Remark = req.Remark
	row.Status = status
	row.UpdateTime = now

	if err := s.DB().Save(&row).Error; err != nil {
		return nil, err
	}
	if err := manager.DefaultManager.ReloadSchedules(); err != nil {
		return nil, err
	}
	return &row, nil
}

func (s SyncTaskService) Delete(guid string) error {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return errors.New("guid required")
	}
	return s.DB().Unscoped().Where("guid = ?", guid).Delete(&domains.SyncTask{}).Error
}

func (s SyncTaskService) Run(guid string) (*domains.SyncRun, error) {
	task, err := s.GetEnabled(guid)
	if err != nil {
		return nil, err
	}
	return manager.DefaultManager.RunTask(*task)
}

func (s SyncTaskService) ValidateRequest(req SaveSyncTaskRequest) (*ValidateSyncTaskResult, error) {
	result := &ValidateSyncTaskResult{}
	req = normalizeSyncTaskRequest(req)
	result.SourceDatasourceGuid = req.SourceGuid
	result.TargetDatasourceGuid = req.TargetGuid
	result.SourceTable = req.SourceTable
	result.TargetTable = req.TargetTable

	fields, err := fieldsFromRequest(req)
	if err != nil {
		result.Errors = append(result.Errors, err.Error())
	}
	result.FieldMappingRowCount = len(fields)
	validateRequiredTaskFields(req, result)
	if len(result.Errors) > 0 {
		result.Valid = false
		return result, nil
	}

	source, err := ServiceGroupApp.DataSourceService.GetEnabled(req.SourceGuid)
	if err != nil {
		result.Errors = append(result.Errors, "source datasource: "+err.Error())
	}
	target, err := ServiceGroupApp.DataSourceService.GetEnabled(req.TargetGuid)
	if err != nil {
		result.Errors = append(result.Errors, "target datasource: "+err.Error())
	}
	if len(result.Errors) > 0 {
		result.Valid = false
		return result, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	sourceConn, err := connector.New(*source)
	if err != nil {
		result.Errors = append(result.Errors, "source connector: "+err.Error())
		result.Valid = false
		return result, nil
	}
	defer sourceConn.Close()
	targetConn, err := connector.New(*target)
	if err != nil {
		result.Errors = append(result.Errors, "target connector: "+err.Error())
		result.Valid = false
		return result, nil
	}
	defer targetConn.Close()

	sourceColumns, err := sourceConn.DescribeTable(ctx, req.SourceTable)
	if err != nil {
		result.Errors = append(result.Errors, "source table: "+err.Error())
	} else {
		result.SourceColumns = sourceColumns
	}
	targetColumns, err := targetConn.DescribeTable(ctx, req.TargetTable)
	if err != nil {
		result.Errors = append(result.Errors, "target table: "+err.Error())
	} else {
		result.TargetColumns = targetColumns
	}
	if len(result.Errors) > 0 {
		result.Valid = false
		return result, nil
	}

	sourceSet := connector.ColumnSet(sourceColumns)
	targetSet := connector.ColumnSet(targetColumns)
	for _, field := range fields {
		sourceName := strings.TrimSpace(field.Source)
		if sourceName != "" && !sourceSet[strings.ToLower(sourceName)] {
			result.MissingSourceFields = append(result.MissingSourceFields, sourceName)
		}
		targetName := strings.TrimSpace(field.Target)
		if targetName != "" && !targetSet[strings.ToLower(targetName)] {
			result.MissingTargetFields = append(result.MissingTargetFields, targetName)
		}
	}
	if req.Mode == domains.SyncModeIncremental && !sourceSet[strings.ToLower(req.CursorField)] {
		result.MissingSourceFields = append(result.MissingSourceFields, req.CursorField)
	}
	if len(result.MissingSourceFields) > 0 {
		result.Errors = append(result.Errors, "source fields missing: "+strings.Join(uniqueStrings(result.MissingSourceFields), ", "))
	}
	if len(result.MissingTargetFields) > 0 {
		result.Errors = append(result.Errors, "target fields missing: "+strings.Join(uniqueStrings(result.MissingTargetFields), ", "))
	}

	count, err := sourceConn.Count(ctx, connector.QueryOptions{
		Table:       req.SourceTable,
		WhereClause: req.WhereClause,
		CursorField: cursorFieldForMode(req.Mode, req.CursorField),
		CursorValue: req.CursorValue,
	})
	if err != nil {
		result.Errors = append(result.Errors, "source count: "+err.Error())
	} else {
		result.EstimatedSourceCount = count
	}
	result.MissingSourceFields = uniqueStrings(result.MissingSourceFields)
	result.MissingTargetFields = uniqueStrings(result.MissingTargetFields)
	result.Valid = len(result.Errors) == 0
	return result, nil
}

func (s SyncTaskService) ValidateSaved(guid string) (*ValidateSyncTaskResult, error) {
	task, err := s.GetEnabled(guid)
	if err != nil {
		return nil, err
	}
	return s.ValidateRequest(requestFromTask(*task))
}

func (s SyncTaskService) Preview(guid string, req SyncTaskPreviewRequest) (*SyncTaskPreviewResult, error) {
	task, err := s.GetEnabled(guid)
	if err != nil {
		return nil, err
	}
	fields, err := fieldsFromRequest(requestFromTask(*task))
	if err != nil {
		return nil, err
	}
	if req.Limit <= 0 {
		req.Limit = 20
	}
	if req.Limit > 100 {
		req.Limit = 100
	}
	source, err := ServiceGroupApp.DataSourceService.GetEnabled(task.SourceGuid)
	if err != nil {
		return nil, err
	}
	conn, err := connector.New(*source)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	rows, err := conn.QueryBatch(ctx, connector.QueryOptions{
		Table:       task.SourceTable,
		WhereClause: task.WhereClause,
		CursorField: cursorFieldForMode(task.Mode, task.CursorField),
		CursorValue: task.CursorValue,
		Limit:       req.Limit,
	})
	if err != nil {
		return nil, err
	}
	mapped, err := mapper.MapRows(rows, fields)
	if err != nil {
		return nil, err
	}
	return &SyncTaskPreviewResult{SourceRows: rows, MappedRows: mapped, Count: len(rows)}, nil
}

func (s SyncTaskService) Stop(guid string) error {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return errors.New("task guid required")
	}
	return manager.DefaultManager.StopTask(guid)
}

func (s SyncTaskService) ReloadSchedules() error {
	return manager.DefaultManager.ReloadSchedules()
}

func (s SyncTaskService) ScheduleItems() []manager.ScheduleItem {
	return manager.DefaultManager.ScheduleItems()
}

func (s SyncTaskService) StartTaskRun(task domains.SyncTask) (*domains.SyncRun, error) {
	run, err := ServiceGroupApp.SyncRunService.CreateForTask(task)
	if err != nil {
		return nil, err
	}
	return run, nil
}

func (s SyncTaskService) GetEnabled(guid string) (*domains.SyncTask, error) {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return nil, errors.New("task guid required")
	}
	var row domains.SyncTask
	err := s.DB().Where("guid = ? AND status = ?", guid, int(domains.StatusEnabled)).First(&row).Error
	if err != nil {
		return nil, errors.New("sync task not found or disabled")
	}
	return &row, nil
}

func (s SyncTaskService) UpdateCursor(taskGuid string, cursorValue string, runGuid string, runStatus string) error {
	return s.DB().Model(&domains.SyncTask{}).Where("guid = ?", taskGuid).Updates(map[string]any{
		"cursor_value":    cursorValue,
		"last_run_guid":   runGuid,
		"last_run_status": runStatus,
		"update_time":     domains.NowMilli(),
	}).Error
}

func normalizeSyncMode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return domains.SyncModeFull
	}
	return value
}

func normalizeWriteMode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return domains.WriteModeInsert
	}
	return value
}

func normalizeFieldMapping(req SaveSyncTaskRequest) (string, error) {
	if len(req.Fields) > 0 {
		data, err := json.Marshal(req.Fields)
		if err != nil {
			return "", err
		}
		return string(data), mapper.Validate(req.Fields)
	}
	if req.FieldMapping == "" {
		return "", errors.New("fieldMapping required")
	}
	var fields []mapper.FieldMapping
	if err := json.Unmarshal([]byte(req.FieldMapping), &fields); err != nil {
		return "", err
	}
	if err := mapper.Validate(fields); err != nil {
		return "", err
	}
	data, err := json.Marshal(fields)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func normalizeSyncTaskRequest(req SaveSyncTaskRequest) SaveSyncTaskRequest {
	req.Guid = strings.TrimSpace(req.Guid)
	req.Name = strings.TrimSpace(req.Name)
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
	req.CronExpr = strings.TrimSpace(req.CronExpr)
	req.Remark = strings.TrimSpace(req.Remark)
	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}
	return req
}

func fieldsFromRequest(req SaveSyncTaskRequest) ([]mapper.FieldMapping, error) {
	if len(req.Fields) > 0 {
		return req.Fields, mapper.Validate(req.Fields)
	}
	if strings.TrimSpace(req.FieldMapping) == "" {
		return nil, errors.New("fieldMapping required")
	}
	var fields []mapper.FieldMapping
	if err := json.Unmarshal([]byte(req.FieldMapping), &fields); err != nil {
		return nil, err
	}
	if err := mapper.Validate(fields); err != nil {
		return nil, err
	}
	return fields, nil
}

func validateRequiredTaskFields(req SaveSyncTaskRequest, result *ValidateSyncTaskResult) {
	if req.SourceGuid == "" {
		result.Errors = append(result.Errors, "sourceGuid required")
	}
	if req.TargetGuid == "" {
		result.Errors = append(result.Errors, "targetGuid required")
	}
	if req.SourceTable == "" {
		result.Errors = append(result.Errors, "sourceTable required")
	}
	if req.TargetTable == "" {
		result.Errors = append(result.Errors, "targetTable required")
	}
	if req.Mode != domains.SyncModeFull && req.Mode != domains.SyncModeIncremental {
		result.Errors = append(result.Errors, fmt.Sprintf("unsupported sync mode %q", req.Mode))
	}
	if req.Mode == domains.SyncModeIncremental && req.CursorField == "" {
		result.Errors = append(result.Errors, "cursorField required for incremental task")
	}
}

func requestFromTask(task domains.SyncTask) SaveSyncTaskRequest {
	status := task.Status
	scheduleOn := task.ScheduleOn
	return SaveSyncTaskRequest{
		Guid:         task.Guid,
		Name:         task.Name,
		SourceGuid:   task.SourceGuid,
		TargetGuid:   task.TargetGuid,
		SourceTable:  task.SourceTable,
		TargetTable:  task.TargetTable,
		Mode:         task.Mode,
		CursorField:  task.CursorField,
		CursorValue:  task.CursorValue,
		BatchSize:    task.BatchSize,
		FieldMapping: task.FieldMapping,
		WriteMode:    task.WriteMode,
		ConflictKeys: task.ConflictKeys,
		WhereClause:  task.WhereClause,
		CronExpr:     task.CronExpr,
		ScheduleOn:   &scheduleOn,
		Remark:       task.Remark,
		Status:       &status,
	}
}

func cursorFieldForMode(mode string, cursorField string) string {
	if mode == domains.SyncModeIncremental {
		return strings.TrimSpace(cursorField)
	}
	return ""
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		key := strings.ToLower(value)
		if value == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
	}
	return out
}
