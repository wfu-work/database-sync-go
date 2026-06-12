package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/connector"
	"database-sync-go/syncer/mapper"
	"database-sync-go/syncer/timewindow"

	"github.com/wfu-work/nav-common-go-lib/global"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type Runner struct {
	db *gorm.DB
}

var childTableTemplateVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

func NewRunner(db *gorm.DB) Runner {
	return Runner{db: db}
}

func (r Runner) Run(ctx context.Context, task domains.SyncTask, run domains.SyncRun) {
	if r.db == nil {
		r.db = global.NAV_DB
	}
	start := domains.NowMilli()
	if err := r.db.Model(&domains.SyncRun{}).Where("guid = ?", run.Guid).Updates(map[string]any{
		"status":     domains.RunStatusRunning,
		"start_time": start,
	}).Error; err != nil {
		global.NAV_LOG.Error("update sync run status failed", zap.Error(err))
		return
	}

	err := r.execute(ctx, task, &run)
	cursorEnd := r.currentRunCursor(run.Guid)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finishRun(run.Guid, domains.RunStatusCanceled, cursorEnd, err)
			_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusCanceled)
			global.NAV_LOG.Info("sync task canceled", zap.String("task", task.Guid), zap.String("run", run.Guid))
			return
		}
		r.finishRun(run.Guid, domains.RunStatusFailed, cursorEnd, err)
		_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusFailed)
		r.notifySyncRunFinished(task, run.Guid, domains.RunStatusFailed, err)
		global.NAV_LOG.Error("sync task failed", zap.String("task", task.Guid), zap.String("run", run.Guid), zap.Error(err))
		return
	}

	if failedCount := r.currentRunFailedCount(run.Guid); failedCount > 0 {
		err := fmt.Errorf("sync finished with %d failed rows", failedCount)
		r.finishRun(run.Guid, domains.RunStatusFailed, cursorEnd, err)
		_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusFailed)
		r.notifySyncRunFinished(task, run.Guid, domains.RunStatusFailed, err)
		global.NAV_LOG.Warn("sync task finished with failed rows", zap.String("task", task.Guid), zap.String("run", run.Guid), zap.Int64("failed", failedCount))
		return
	}

	r.finishRun(run.Guid, domains.RunStatusSuccess, cursorEnd, nil)
	_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusSuccess)
	r.notifySyncRunFinished(task, run.Guid, domains.RunStatusSuccess, nil)
	global.NAV_LOG.Info("sync task success", zap.String("task", task.Guid), zap.String("run", run.Guid))
}

func (r Runner) RetryErrors(ctx context.Context, task domains.SyncTask, errorsToRetry []domains.SyncError, run domains.SyncRun) {
	if r.db == nil {
		r.db = global.NAV_DB
	}
	start := domains.NowMilli()
	if err := r.db.Model(&domains.SyncRun{}).Where("guid = ?", run.Guid).Updates(map[string]any{
		"status":      domains.RunStatusRunning,
		"start_time":  start,
		"total_count": int64(len(errorsToRetry)),
	}).Error; err != nil {
		global.NAV_LOG.Error("update retry run status failed", zap.Error(err))
		return
	}

	err := r.executeRetry(ctx, task, errorsToRetry, &run)
	cursorEnd := r.currentRunCursor(run.Guid)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			r.finishRun(run.Guid, domains.RunStatusCanceled, cursorEnd, err)
			_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusCanceled)
			global.NAV_LOG.Info("retry sync errors canceled", zap.String("task", task.Guid), zap.String("run", run.Guid))
			return
		}
		r.finishRun(run.Guid, domains.RunStatusFailed, cursorEnd, err)
		_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusFailed)
		r.notifySyncRunFinished(task, run.Guid, domains.RunStatusFailed, err)
		global.NAV_LOG.Error("retry sync errors failed", zap.String("task", task.Guid), zap.String("run", run.Guid), zap.Error(err))
		return
	}

	if failedCount := r.currentRunFailedCount(run.Guid); failedCount > 0 {
		err := fmt.Errorf("retry finished with %d failed rows", failedCount)
		r.finishRun(run.Guid, domains.RunStatusFailed, cursorEnd, err)
		_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusFailed)
		r.notifySyncRunFinished(task, run.Guid, domains.RunStatusFailed, err)
		global.NAV_LOG.Warn("retry sync errors finished with failed rows", zap.String("task", task.Guid), zap.String("run", run.Guid), zap.Int64("failed", failedCount))
		return
	}
	r.finishRun(run.Guid, domains.RunStatusSuccess, cursorEnd, nil)
	_ = r.updateTaskRun(task.Guid, firstNonEmpty(cursorEnd, task.CursorValue), run.Guid, domains.RunStatusSuccess)
	r.notifySyncRunFinished(task, run.Guid, domains.RunStatusSuccess, nil)
	global.NAV_LOG.Info("retry sync errors success", zap.String("task", task.Guid), zap.String("run", run.Guid))
}

func (r Runner) execute(ctx context.Context, task domains.SyncTask, run *domains.SyncRun) error {
	var fields []mapper.FieldMapping
	if err := json.Unmarshal([]byte(task.FieldMapping), &fields); err != nil {
		return err
	}
	if err := mapper.Validate(fields); err != nil {
		return err
	}
	writeOpts, err := writeOptions(task, fields)
	if err != nil {
		return err
	}

	var source domains.DataSource
	if err := r.db.Where("guid = ? AND status = ?", task.SourceGuid, int(domains.StatusEnabled)).First(&source).Error; err != nil {
		return fmt.Errorf("source datasource not found: %w", err)
	}
	var target domains.DataSource
	if err := r.db.Where("guid = ? AND status = ?", task.TargetGuid, int(domains.StatusEnabled)).First(&target).Error; err != nil {
		return fmt.Errorf("target datasource not found: %w", err)
	}

	sourceConn, err := connector.New(source)
	if err != nil {
		return err
	}
	defer sourceConn.Close()
	targetConn, err := connector.New(target)
	if err != nil {
		return err
	}
	defer targetConn.Close()
	if strings.EqualFold(strings.TrimSpace(source.Type), domains.DataSourceTypeTDengine) {
		return r.executeTDengine(ctx, task, run, sourceConn, targetConn, fields)
	}

	queryOpts := connector.QueryOptions{
		Table:       task.SourceTable,
		WhereClause: task.WhereClause,
		Limit:       task.BatchSize,
	}
	if task.Mode == domains.SyncModeIncremental {
		queryOpts.CursorField = task.CursorField
		queryOpts.CursorValue = task.CursorValue
	}
	total, err := sourceConn.Count(ctx, queryOpts)
	if err != nil {
		return err
	}
	if err := r.db.Model(&domains.SyncRun{}).Where("guid = ?", run.Guid).Updates(map[string]any{
		"total_count": total,
	}).Error; err != nil {
		return err
	}

	batchSize := task.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	offset := 0
	cursorValue := task.CursorValue
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		queryOpts.Limit = batchSize
		queryOpts.Offset = offset
		rows, err := sourceConn.QueryBatch(ctx, queryOpts)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}

		mappedRows, err := mapper.MapRows(rows, fields)
		if err != nil {
			if task.CursorField != "" {
				cursorValue = lastCursor(rows, task.CursorField, cursorValue)
				queryOpts.CursorValue = cursorValue
			}
			r.recordBatchError(run.Guid, task.Guid, rows, task.CursorField, err)
			r.incrementRun(run.Guid, int64(len(rows)), 0, int64(len(rows)), cursorValue, err.Error())
			if task.Mode == domains.SyncModeFull {
				offset += len(rows)
			}
			continue
		}

		affected, err := targetConn.WriteBatch(ctx, rowsForWrite(task, mappedRows, rows), writeOpts)
		if err != nil {
			if task.CursorField != "" {
				cursorValue = lastCursor(rows, task.CursorField, cursorValue)
				queryOpts.CursorValue = cursorValue
			}
			r.recordBatchError(run.Guid, task.Guid, rows, task.CursorField, err)
			r.incrementRun(run.Guid, int64(len(rows)), 0, int64(len(rows)), cursorValue, err.Error())
			if task.Mode == domains.SyncModeFull {
				offset += len(rows)
			}
			continue
		}

		if task.CursorField != "" {
			cursorValue = lastCursor(rows, task.CursorField, cursorValue)
			queryOpts.CursorValue = cursorValue
		}
		r.incrementRun(run.Guid, int64(len(rows)), affected, 0, cursorValue, "")
		if task.Mode == domains.SyncModeFull {
			offset += len(rows)
		}
		if len(rows) < batchSize {
			break
		}
	}
	return nil
}

func (r Runner) executeTDengine(ctx context.Context, task domains.SyncTask, run *domains.SyncRun, sourceConn connector.Connector, targetConn connector.Connector, fields []mapper.FieldMapping) error {
	writeOpts, err := writeOptions(task, fields)
	if err != nil {
		return err
	}
	syncRange, err := timewindow.ParseRange(task.SyncStartDate, task.SyncEndDate, time.Now())
	if err != nil {
		return err
	}
	timeField, err := tdengineTimeField(ctx, sourceConn, task)
	if err != nil {
		return err
	}
	batchSize := task.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	extraSelectFields := tdengineExtraSelectFields(task, writeOpts.TDengineTagMappings)
	cursorValue := task.CursorValue
	cursorCaptured := false
	total := int64(0)

	return timewindow.ForEachDayBackward(ctx, syncRange, func(window timewindow.Window) error {
		baseOpts := connector.QueryOptions{
			Table:             task.SourceTable,
			WhereClause:       task.WhereClause,
			CursorField:       timeField,
			CursorValue:       "",
			TimeField:         timeField,
			TimeStart:         timewindow.FormatSQL(window.Start),
			TimeEnd:           timewindow.FormatSQL(window.End),
			ExtraSelectFields: extraSelectFields,
		}
		count, err := sourceConn.Count(ctx, baseOpts)
		if err != nil {
			return err
		}
		total += count
		if err := r.db.Model(&domains.SyncRun{}).Where("guid = ?", run.Guid).Updates(map[string]any{
			"total_count": total,
			"update_time": domains.NowMilli(),
		}).Error; err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		captureCursor := !cursorCaptured

		for offset := 0; ; {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			queryOpts := baseOpts
			queryOpts.Limit = batchSize
			queryOpts.Offset = offset
			rows, err := sourceConn.QueryBatch(ctx, queryOpts)
			if err != nil {
				return err
			}
			if len(rows) == 0 {
				break
			}

			mappedRows, err := mapper.MapRows(rows, fields)
			if err != nil {
				if captureCursor {
					cursorValue = lastCursor(rows, timeField, cursorValue)
					cursorCaptured = true
				}
				r.recordBatchError(run.Guid, task.Guid, rows, timeField, err)
				r.incrementRun(run.Guid, int64(len(rows)), 0, int64(len(rows)), cursorValue, err.Error())
				offset += len(rows)
				continue
			}
			affected, err := targetConn.WriteBatch(ctx, rowsForWrite(task, mappedRows, rows), writeOpts)
			if err != nil {
				if captureCursor {
					cursorValue = lastCursor(rows, timeField, cursorValue)
					cursorCaptured = true
				}
				r.recordBatchError(run.Guid, task.Guid, rows, timeField, err)
				r.incrementRun(run.Guid, int64(len(rows)), 0, int64(len(rows)), cursorValue, err.Error())
				offset += len(rows)
				continue
			}

			if captureCursor {
				cursorValue = lastCursor(rows, timeField, cursorValue)
				cursorCaptured = true
			}
			r.incrementRun(run.Guid, int64(len(rows)), affected, 0, cursorValue, "")
			offset += len(rows)
			if len(rows) < batchSize {
				break
			}
		}
		return nil
	})
}

func (r Runner) executeRetry(ctx context.Context, task domains.SyncTask, errorsToRetry []domains.SyncError, run *domains.SyncRun) error {
	var fields []mapper.FieldMapping
	if err := json.Unmarshal([]byte(task.FieldMapping), &fields); err != nil {
		return err
	}
	if err := mapper.Validate(fields); err != nil {
		return err
	}
	writeOpts, err := writeOptions(task, fields)
	if err != nil {
		return err
	}

	var target domains.DataSource
	if err := r.db.Where("guid = ? AND status = ?", task.TargetGuid, int(domains.StatusEnabled)).First(&target).Error; err != nil {
		return fmt.Errorf("target datasource not found: %w", err)
	}
	targetConn, err := connector.New(target)
	if err != nil {
		return err
	}
	defer targetConn.Close()

	batchSize := task.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}
	for offset := 0; offset < len(errorsToRetry); offset += batchSize {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		end := offset + batchSize
		if end > len(errorsToRetry) {
			end = len(errorsToRetry)
		}
		batch := errorsToRetry[offset:end]
		rows := make([]mapper.Row, 0, len(batch))
		for _, item := range batch {
			var row mapper.Row
			if err := json.Unmarshal([]byte(item.SourceData), &row); err != nil {
				r.recordRetryError(run.Guid, task.Guid, item, err)
				r.incrementRun(run.Guid, 1, 0, 1, "", err.Error())
				continue
			}
			rows = append(rows, row)
		}
		if len(rows) == 0 {
			continue
		}

		mappedRows, err := mapper.MapRows(rows, fields)
		if err != nil {
			r.recordBatchError(run.Guid, task.Guid, rows, task.CursorField, err)
			r.incrementRun(run.Guid, int64(len(rows)), 0, int64(len(rows)), "", err.Error())
			continue
		}
		affected, err := targetConn.WriteBatch(ctx, rowsForWrite(task, mappedRows, rows), writeOpts)
		if err != nil {
			r.recordBatchError(run.Guid, task.Guid, rows, task.CursorField, err)
			r.incrementRun(run.Guid, int64(len(rows)), 0, int64(len(rows)), "", err.Error())
			continue
		}
		r.incrementRun(run.Guid, int64(len(rows)), affected, 0, "", "")
	}
	return nil
}

func (r Runner) finishRun(runGuid string, status string, cursorEnd string, err error) {
	now := domains.NowMilli()
	updates := map[string]any{
		"status":      status,
		"end_time":    now,
		"duration_ms": gorm.Expr("? - start_time", now),
		"update_time": now,
	}
	if cursorEnd != "" {
		updates["cursor_end"] = cursorEnd
	}
	if err != nil {
		updates["last_error"] = err.Error()
	}
	_ = r.db.Model(&domains.SyncRun{}).Where("guid = ?", runGuid).Updates(updates).Error
}

func (r Runner) incrementRun(runGuid string, processed int64, success int64, failed int64, cursorEnd string, lastError string) {
	updates := map[string]any{
		"processed_count": gorm.Expr("processed_count + ?", processed),
		"success_count":   gorm.Expr("success_count + ?", success),
		"failed_count":    gorm.Expr("failed_count + ?", failed),
		"update_time":     domains.NowMilli(),
	}
	if cursorEnd != "" {
		updates["cursor_end"] = cursorEnd
	}
	if lastError != "" {
		updates["last_error"] = lastError
	}
	_ = r.db.Model(&domains.SyncRun{}).Where("guid = ?", runGuid).Updates(updates).Error
}

func (r Runner) recordBatchError(runGuid string, taskGuid string, rows []mapper.Row, cursorField string, err error) {
	for _, row := range rows {
		data, _ := json.Marshal(row)
		sourcePK := ""
		if cursorField != "" {
			if value, ok := mapper.Lookup(row, cursorField); ok {
				sourcePK = fmt.Sprint(value)
			}
		}
		item := domains.SyncError{
			RunGuid:      runGuid,
			TaskGuid:     taskGuid,
			SourcePK:     sourcePK,
			SourceData:   string(data),
			ErrorMessage: err.Error(),
		}
		_ = r.db.Create(&item).Error
	}
}

func (r Runner) recordRetryError(runGuid string, taskGuid string, item domains.SyncError, err error) {
	row := domains.SyncError{
		RunGuid:      runGuid,
		TaskGuid:     taskGuid,
		SourcePK:     item.SourcePK,
		SourceData:   item.SourceData,
		ErrorMessage: err.Error(),
	}
	_ = r.db.Create(&row).Error
}

func (r Runner) updateTaskRun(taskGuid string, cursorValue string, runGuid string, status string) error {
	return r.db.Model(&domains.SyncTask{}).Where("guid = ?", taskGuid).Updates(map[string]any{
		"cursor_value":    cursorValue,
		"last_run_guid":   runGuid,
		"last_run_status": status,
		"update_time":     domains.NowMilli(),
	}).Error
}

func (r Runner) notifySyncRunFinished(task domains.SyncTask, runGuid string, status string, err error) {
	level := domains.EventLevelInfo
	eventType := domains.EventTypeSyncRunSuccess
	title := "同步任务完成"
	content := fmt.Sprintf("同步任务 %s 已完成。", task.Name)
	if status != domains.RunStatusSuccess {
		level = domains.EventLevelError
		eventType = domains.EventTypeSyncRunFailed
		title = "同步任务失败"
		content = fmt.Sprintf("同步任务 %s 执行失败。", task.Name)
		if err != nil {
			content += "原因：" + err.Error()
		}
	}
	now := domains.NowMilli()
	row := domains.EventNotification{
		Type:       eventType,
		Level:      level,
		Title:      title,
		Content:    content,
		SourceType: domains.EventSourceSyncRun,
		SourceGuid: runGuid,
		SourceName: task.Name,
		Read:       0,
		EventTime:  now,
	}
	row.CreateTime = now
	row.UpdateTime = now
	if createErr := r.db.Create(&row).Error; createErr != nil {
		global.NAV_LOG.Warn("create sync run notification failed", zap.String("run", runGuid), zap.Error(createErr))
	}
}

func (r Runner) currentRunCursor(runGuid string) string {
	var run domains.SyncRun
	if err := r.db.Select("cursor_end").Where("guid = ?", runGuid).First(&run).Error; err != nil {
		return ""
	}
	return run.CursorEnd
}

func (r Runner) currentRunFailedCount(runGuid string) int64 {
	var run domains.SyncRun
	if err := r.db.Select("failed_count").Where("guid = ?", runGuid).First(&run).Error; err != nil {
		return 0
	}
	return run.FailedCount
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func writeOptions(task domains.SyncTask, fields []mapper.FieldMapping) (connector.WriteOptions, error) {
	tags, err := taskTagMappings(task)
	if err != nil {
		return connector.WriteOptions{}, err
	}
	return connector.WriteOptions{
		Table:                      task.TargetTable,
		WriteMode:                  task.WriteMode,
		ConflictKeys:               splitCSV(task.ConflictKeys),
		InsertColumns:              mappedTargetColumns(fields),
		TDengineChildTableTemplate: childTableTemplate(task),
		TDengineChildTableField:    task.TDengineChildTableField,
		TDengineTagMappings:        tags,
	}, nil
}

func mappedTargetColumns(fields []mapper.FieldMapping) []string {
	columns := make([]string, 0, len(fields))
	seen := map[string]bool{}
	for _, field := range fields {
		target := strings.TrimSpace(field.Target)
		key := strings.ToLower(target)
		if target == "" || seen[key] {
			continue
		}
		seen[key] = true
		columns = append(columns, target)
	}
	return columns
}

func taskTagMappings(task domains.SyncTask) ([]mapper.TagMapping, error) {
	var tags []mapper.TagMapping
	if strings.TrimSpace(task.TDengineTags) == "" {
		return tags, nil
	}
	if err := json.Unmarshal([]byte(task.TDengineTags), &tags); err != nil {
		return nil, err
	}
	if err := mapper.ValidateTags(tags); err != nil {
		return nil, err
	}
	return tags, nil
}

func tdengineExtraSelectFields(task domains.SyncTask, tags []mapper.TagMapping) []string {
	fields := make([]string, 0, len(tags)+1)
	seen := map[string]bool{}
	add := func(value string) {
		value = strings.TrimSpace(value)
		key := strings.ToLower(value)
		if value == "" || seen[key] {
			return
		}
		seen[key] = true
		fields = append(fields, value)
	}
	for _, field := range childTableTemplateSourceFields(childTableTemplate(task), task.TDengineChildTableField) {
		add(field)
	}
	for _, tag := range tags {
		add(tag.Source)
	}
	return fields
}

func mergeSourceRows(mappedRows []mapper.Row, sourceRows []mapper.Row) []mapper.Row {
	if len(mappedRows) == 0 || len(sourceRows) == 0 {
		return mappedRows
	}
	out := make([]mapper.Row, 0, len(mappedRows))
	for i, mapped := range mappedRows {
		merged := mapper.Row{}
		for key, value := range mapped {
			merged[key] = value
		}
		if i < len(sourceRows) {
			for key, value := range sourceRows[i] {
				if _, exists := merged[key]; !exists {
					merged[key] = value
				}
			}
		}
		out = append(out, merged)
	}
	return out
}

func rowsForWrite(task domains.SyncTask, mappedRows []mapper.Row, sourceRows []mapper.Row) []mapper.Row {
	if childTableTemplate(task) == "" {
		return mappedRows
	}
	return mergeSourceRows(mappedRows, sourceRows)
}

func childTableTemplate(task domains.SyncTask) string {
	template := strings.TrimSpace(task.TDengineChildTableTemplate)
	if template != "" {
		return template
	}
	return strings.TrimSpace(task.TDengineChildTableField)
}

func childTableTemplateFields(template string) []string {
	template = strings.TrimSpace(template)
	if template == "" {
		return nil
	}
	matches := childTableTemplateVarPattern.FindAllStringSubmatch(template, -1)
	if len(matches) == 0 {
		return nil
	}
	fields := make([]string, 0, len(matches))
	seen := map[string]bool{}
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		field := strings.TrimSpace(match[1])
		key := strings.ToLower(field)
		if field == "" || seen[key] {
			continue
		}
		seen[key] = true
		fields = append(fields, field)
	}
	return fields
}

func childTableTemplateSourceFields(template string, legacyField string) []string {
	fields := childTableTemplateFields(template)
	if len(fields) > 0 {
		return fields
	}
	template = strings.TrimSpace(template)
	legacyField = strings.TrimSpace(legacyField)
	if legacyField != "" && strings.EqualFold(template, legacyField) {
		return []string{legacyField}
	}
	return nil
}

func lastCursor(rows []mapper.Row, cursorField string, fallback string) string {
	if len(rows) == 0 {
		return fallback
	}
	value, ok := mapper.Lookup(rows[len(rows)-1], cursorField)
	if !ok || value == nil {
		return fallback
	}
	return fmt.Sprint(value)
}

func tdengineTimeField(ctx context.Context, sourceConn connector.Connector, task domains.SyncTask) (string, error) {
	syncTimeField := strings.TrimSpace(task.SyncTimeField)
	if syncTimeField == "" {
		return "", errors.New("syncTimeField required for tdengine source")
	}
	columns, err := sourceConn.DescribeTable(ctx, task.SourceTable)
	if err != nil {
		return "", err
	}
	if len(columns) == 0 {
		return "", errors.New("tdengine source columns not found")
	}
	for _, column := range columns {
		if strings.EqualFold(column.Name, syncTimeField) {
			return column.Name, nil
		}
	}
	return "", fmt.Errorf("syncTimeField not found in source fields: %s", syncTimeField)
}
