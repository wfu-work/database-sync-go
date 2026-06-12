package services

import (
	"archive/zip"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/connector"
	"database-sync-go/syncer/mapper"
	"database-sync-go/utils"

	commonDomains "github.com/wfu-work/nav-common-go-lib/domains"
	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type DatabaseRestoreService struct {
	commonServices.CrudService[domains.DatabaseRestore]
}

func (s DatabaseRestoreService) WithDB(db *gorm.DB) DatabaseRestoreService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

type StartDatabaseRestoreRequest struct {
	BackupGuid            string   `json:"backupGuid"`
	TargetDataSourceGuid  string   `json:"targetDataSourceGuid"`
	Tables                []string `json:"tables"`
	BatchSize             int      `json:"batchSize"`
	WriteMode             string   `json:"writeMode"`
	CreateTable           *bool    `json:"createTable"`
	TruncateBeforeRestore *bool    `json:"truncateBeforeRestore"`
	RetryTimes            int      `json:"retryTimes"`
	RetryIntervalMs       int      `json:"retryIntervalMs"`
	Remark                string   `json:"remark"`
}

type restoreRunOptions struct {
	Tables                []string
	BatchSize             int
	WriteMode             string
	CreateTable           bool
	TruncateBeforeRestore bool
	RetryTimes            int
	RetryInterval         time.Duration
}

func (s DatabaseRestoreService) List(params map[string]string) ([]domains.DatabaseRestore, int64, error) {
	db := s.DB().Model(&domains.DatabaseRestore{})
	if backupGuid := strings.TrimSpace(params["backupGuid"]); backupGuid != "" {
		db = db.Where("backup_guid = ?", backupGuid)
	}
	if targetGuid := strings.TrimSpace(params["targetDataSourceGuid"]); targetGuid != "" {
		db = db.Where("target_data_source_guid = ?", targetGuid)
	}
	if status := strings.TrimSpace(params["status"]); status != "" {
		db = db.Where("status = ?", status)
	}
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("guid LIKE ? OR backup_guid LIKE ? OR backup_name LIKE ? OR target_data_source_name LIKE ? OR tables LIKE ? OR remark LIKE ? OR last_error LIKE ?", like, like, like, like, like, like, like)
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
	var items []domains.DatabaseRestore
	err := db.Order("start_time DESC, create_time DESC").Limit(size).Offset((page - 1) * size).Find(&items).Error
	return items, total, err
}

func (s DatabaseRestoreService) Get(guid string) (*domains.DatabaseRestore, error) {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return nil, errors.New("restore guid required")
	}
	var row domains.DatabaseRestore
	if err := s.DB().Where("guid = ?", guid).First(&row).Error; err != nil {
		return nil, errors.New("database restore not found")
	}
	return &row, nil
}

func (s DatabaseRestoreService) Start(req StartDatabaseRestoreRequest) (*domains.DatabaseRestore, error) {
	req.BackupGuid = strings.TrimSpace(req.BackupGuid)
	req.TargetDataSourceGuid = strings.TrimSpace(req.TargetDataSourceGuid)
	req.Tables = normalizeBackupTables(req.Tables)
	req.Remark = strings.TrimSpace(req.Remark)
	if req.BackupGuid == "" {
		return nil, errors.New("backupGuid required")
	}
	setting := DefaultSyncSetting()
	if got, err := ServiceGroupApp.SystemSettingService.GetSyncSetting(); err == nil {
		setting = got
	}
	if req.BatchSize <= 0 {
		req.BatchSize = defaultRestoreBatchSize()
	}
	if strings.TrimSpace(req.WriteMode) == "" {
		req.WriteMode = setting.RestoreWriteMode
	}
	req.WriteMode = normalizeRestoreWriteMode(req.WriteMode)
	createTable := setting.RestoreCreateTable
	if req.CreateTable != nil {
		createTable = *req.CreateTable
	}
	truncateBeforeRestore := setting.RestoreTruncateBefore
	if req.TruncateBeforeRestore != nil {
		truncateBeforeRestore = *req.TruncateBeforeRestore
	}
	if req.RetryTimes < 0 {
		req.RetryTimes = 0
	}
	if req.RetryIntervalMs <= 0 {
		req.RetryIntervalMs = defaultBackupRetryIntervalMs()
	}

	backup, err := ServiceGroupApp.DatabaseBackupService.Get(req.BackupGuid)
	if err != nil {
		return nil, err
	}
	if backup.Status != domains.BackupStatusSuccess {
		return nil, errors.New("only successful backup can be restored")
	}
	if strings.TrimSpace(backup.FilePath) == "" {
		return nil, errors.New("backup file missing")
	}
	targetGuid := req.TargetDataSourceGuid
	if targetGuid == "" {
		targetGuid = backup.DataSourceGuid
	}
	target, err := ServiceGroupApp.DataSourceService.GetEnabled(targetGuid)
	if err != nil {
		return nil, err
	}
	tablesJSON, err := json.Marshal(req.Tables)
	if err != nil {
		return nil, err
	}

	now := domains.NowMilli()
	row := domains.DatabaseRestore{
		BaseDataEntity: commonDomains.BaseDataEntity{
			Guid: restoreGuid(backup, target, now),
		},
		BackupGuid:            backup.Guid,
		BackupName:            utils.FirstNonEmpty(backup.FileName, backup.Guid),
		SourceDataSourceGuid:  backup.DataSourceGuid,
		SourceDataSourceName:  backup.DataSourceName,
		TargetDataSourceGuid:  target.Guid,
		TargetDataSourceName:  target.Name,
		TargetDataSourceType:  target.Type,
		TargetDatabase:        target.Database,
		Tables:                string(tablesJSON),
		BatchSize:             req.BatchSize,
		WriteMode:             req.WriteMode,
		CreateTable:           createTable,
		TruncateBeforeRestore: truncateBeforeRestore,
		RetryTimes:            req.RetryTimes,
		RetryIntervalMs:       req.RetryIntervalMs,
		Status:                domains.RestoreStatusPending,
		StartTime:             now,
		Remark:                req.Remark,
	}
	row.CreateTime = now
	row.UpdateTime = now
	if err := s.DB().Create(&row).Error; err != nil {
		return nil, err
	}

	go s.runRestore(row.Guid, *backup, *target, restoreRunOptions{
		Tables:                req.Tables,
		BatchSize:             req.BatchSize,
		WriteMode:             req.WriteMode,
		CreateTable:           createTable,
		TruncateBeforeRestore: truncateBeforeRestore,
		RetryTimes:            req.RetryTimes,
		RetryInterval:         time.Duration(req.RetryIntervalMs) * time.Millisecond,
	})
	return &row, nil
}

func (s DatabaseRestoreService) RecoverStaleRestores() error {
	db := s.DB()
	if db == nil {
		return errors.New("database not initialized")
	}
	now := domains.NowMilli()
	return db.Model(&domains.DatabaseRestore{}).
		Where("status IN ?", []string{domains.RestoreStatusPending, domains.RestoreStatusRunning}).
		Updates(map[string]any{
			"status":      domains.RestoreStatusFailed,
			"end_time":    now,
			"duration_ms": gorm.Expr("CASE WHEN start_time > 0 THEN ? - start_time ELSE 0 END", now),
			"last_error":  "服务重启或恢复进程中断，恢复未完成",
			"update_time": now,
		}).Error
}

func (s DatabaseRestoreService) runRestore(restoreGuid string, backup domains.DatabaseBackup, target domains.DataSource, opts restoreRunOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), backupTimeout())
	defer cancel()
	opts = normalizeRestoreRunOptions(opts)
	start := domains.NowMilli()
	_ = s.DB().Model(&domains.DatabaseRestore{}).Where("guid = ?", restoreGuid).Updates(map[string]any{
		"status":          domains.RestoreStatusRunning,
		"start_time":      start,
		"finished_tables": 0,
		"current_table":   "",
		"current_rows":    0,
		"current_total":   0,
		"current_batch":   0,
		"total_rows":      0,
		"success_rows":    0,
		"failed_rows":     0,
		"last_error":      "",
		"update_time":     start,
	}).Error

	totalTables, totalRows, successRows, failedRows, err := s.restoreBackup(ctx, restoreGuid, backup, target, opts)
	if err != nil {
		s.finishRestore(restoreGuid, domains.RestoreStatusFailed, totalTables, totalRows, successRows, failedRows, err)
		return
	}
	s.finishRestore(restoreGuid, domains.RestoreStatusSuccess, totalTables, totalRows, successRows, failedRows, nil)
}

func (s DatabaseRestoreService) restoreBackup(ctx context.Context, restoreGuid string, backup domains.DatabaseBackup, target domains.DataSource, opts restoreRunOptions) (int, int64, int64, int64, error) {
	reader, err := zip.OpenReader(backup.FilePath)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	defer reader.Close()

	manifest, err := readBackupManifest(&reader.Reader)
	if err != nil {
		return 0, 0, 0, 0, err
	}
	tables := selectRestoreTables(manifest.Tables, opts.Tables)
	if len(tables) == 0 {
		return 0, 0, 0, 0, errors.New("no tables to restore")
	}
	tablesJSON, _ := json.Marshal(tableManifestNames(tables))
	_ = s.DB().Model(&domains.DatabaseRestore{}).Where("guid = ?", restoreGuid).Updates(map[string]any{
		"tables":       string(tablesJSON),
		"total_tables": len(tables),
		"total_rows":   sumBackupTableRows(tables),
		"update_time":  domains.NowMilli(),
	}).Error

	conn, err := connector.New(target)
	if err != nil {
		return len(tables), sumBackupTableRows(tables), 0, 0, err
	}
	defer conn.Close()

	fileMap := backupFileMap(reader.File)
	var totalRows int64
	var successRows int64
	var failedRows int64
	truncated := map[string]bool{}
	for i, table := range tables {
		select {
		case <-ctx.Done():
			return len(tables), totalRows, successRows, failedRows, ctx.Err()
		default:
		}
		_ = s.DB().Model(&domains.DatabaseRestore{}).Where("guid = ?", restoreGuid).Updates(map[string]any{
			"current_table": table.Name,
			"current_rows":  0,
			"current_total": table.RowCount,
			"current_batch": 0,
			"update_time":   domains.NowMilli(),
		}).Error
		if opts.CreateTable {
			if err := conn.EnsureTable(ctx, table.Name, table.Columns); err != nil {
				return len(tables), totalRows, successRows, failedRows, fmt.Errorf("ensure table %s failed: %w", table.Name, err)
			}
		}
		if opts.TruncateBeforeRestore && !truncated[strings.ToLower(table.Name)] {
			if err := conn.TruncateTable(ctx, table.Name); err != nil {
				return len(tables), totalRows, successRows, failedRows, fmt.Errorf("truncate table %s failed: %w", table.Name, err)
			}
			truncated[strings.ToLower(table.Name)] = true
		}

		tableRows, tableSuccess, tableFailed, err := s.restoreTable(ctx, restoreGuid, conn, fileMap, table, opts)
		totalRows += tableRows
		successRows += tableSuccess
		failedRows += tableFailed
		if err != nil {
			return len(tables), totalRows, successRows, failedRows, err
		}
		_ = s.DB().Model(&domains.DatabaseRestore{}).Where("guid = ?", restoreGuid).Updates(map[string]any{
			"finished_tables": i + 1,
			"total_rows":      totalRows,
			"success_rows":    successRows,
			"failed_rows":     failedRows,
			"current_rows":    tableRows,
			"current_total":   table.RowCount,
			"update_time":     domains.NowMilli(),
		}).Error
	}
	return len(tables), totalRows, successRows, failedRows, nil
}

func (s DatabaseRestoreService) restoreTable(ctx context.Context, restoreGuid string, conn connector.Connector, files map[string]*zip.File, table backupTableManifest, opts restoreRunOptions) (int64, int64, int64, error) {
	file := files[table.Data]
	if file == nil {
		return 0, 0, 0, fmt.Errorf("backup data file %s missing", table.Data)
	}
	body, err := file.Open()
	if err != nil {
		return 0, 0, 0, err
	}
	defer body.Close()

	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 0, 1024*1024), 64*1024*1024)
	batch := make([]mapper.Row, 0, opts.BatchSize)
	var readRows int64
	var successRows int64
	var failedRows int64
	batchIndex := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		batchIndex += 1
		_, err := writeRestoreBatch(ctx, conn, table.Name, batch, opts)
		if err != nil {
			failedRows += int64(len(batch))
			return fmt.Errorf("restore table %s batch %d failed: %w", table.Name, batchIndex, err)
		}
		restoredRows := int64(len(batch))
		successRows += restoredRows
		_ = s.DB().Model(&domains.DatabaseRestore{}).Where("guid = ?", restoreGuid).Updates(map[string]any{
			"current_rows":  readRows,
			"current_batch": batchIndex,
			"success_rows":  gorm.Expr("success_rows + ?", restoredRows),
			"update_time":   domains.NowMilli(),
		}).Error
		batch = batch[:0]
		return nil
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return readRows, successRows, failedRows, ctx.Err()
		default:
		}
		var row mapper.Row
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			failedRows += 1
			return readRows, successRows, failedRows, fmt.Errorf("parse table %s row failed: %w", table.Name, err)
		}
		batch = append(batch, row)
		readRows += 1
		if len(batch) >= opts.BatchSize {
			if err := flush(); err != nil {
				return readRows, successRows, failedRows, err
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return readRows, successRows, failedRows, err
	}
	if err := flush(); err != nil {
		return readRows, successRows, failedRows, err
	}
	return readRows, successRows, failedRows, nil
}

func (s DatabaseRestoreService) finishRestore(restoreGuid string, status string, totalTables int, totalRows int64, successRows int64, failedRows int64, err error) {
	now := domains.NowMilli()
	updates := map[string]any{
		"status":       status,
		"end_time":     now,
		"duration_ms":  gorm.Expr("? - start_time", now),
		"total_tables": totalTables,
		"total_rows":   totalRows,
		"success_rows": successRows,
		"failed_rows":  failedRows,
		"update_time":  now,
	}
	if status == domains.RestoreStatusSuccess {
		updates["finished_tables"] = totalTables
		updates["current_table"] = ""
		updates["current_rows"] = 0
		updates["current_total"] = 0
		updates["current_batch"] = 0
	}
	if err != nil {
		updates["last_error"] = err.Error()
	}
	_ = s.DB().Model(&domains.DatabaseRestore{}).Where("guid = ?", restoreGuid).Updates(updates).Error
}

func readBackupManifest(reader *zip.Reader) (backupManifest, error) {
	file := backupFileMap(reader.File)["manifest.json"]
	if file == nil {
		return backupManifest{}, errors.New("manifest.json missing")
	}
	body, err := file.Open()
	if err != nil {
		return backupManifest{}, err
	}
	defer body.Close()
	data, err := io.ReadAll(body)
	if err != nil {
		return backupManifest{}, err
	}
	var manifest backupManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return backupManifest{}, err
	}
	if manifest.Format != "" && manifest.Format != domains.BackupFormatJSONLZip {
		return backupManifest{}, fmt.Errorf("unsupported backup format %q", manifest.Format)
	}
	return manifest, nil
}

func backupFileMap(files []*zip.File) map[string]*zip.File {
	out := make(map[string]*zip.File, len(files))
	for _, file := range files {
		out[file.Name] = file
	}
	return out
}

func selectRestoreTables(all []backupTableManifest, selected []string) []backupTableManifest {
	if len(selected) == 0 {
		return all
	}
	allowed := map[string]bool{}
	for _, table := range selected {
		allowed[strings.ToLower(strings.TrimSpace(table))] = true
	}
	out := make([]backupTableManifest, 0, len(selected))
	for _, table := range all {
		if allowed[strings.ToLower(strings.TrimSpace(table.Name))] {
			out = append(out, table)
		}
	}
	return out
}

func tableManifestNames(tables []backupTableManifest) []string {
	out := make([]string, 0, len(tables))
	for _, table := range tables {
		out = append(out, table.Name)
	}
	return out
}

func sumBackupTableRows(tables []backupTableManifest) int64 {
	var total int64
	for _, table := range tables {
		total += table.RowCount
	}
	return total
}

func normalizeRestoreRunOptions(opts restoreRunOptions) restoreRunOptions {
	opts.Tables = normalizeBackupTables(opts.Tables)
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultRestoreBatchSize()
	}
	opts.WriteMode = normalizeRestoreWriteMode(opts.WriteMode)
	if opts.RetryTimes < 0 {
		opts.RetryTimes = 0
	}
	if opts.RetryInterval <= 0 {
		opts.RetryInterval = time.Duration(defaultBackupRetryIntervalMs()) * time.Millisecond
	}
	return opts
}

func normalizeRestoreWriteMode(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case domains.WriteModeReplace:
		return domains.WriteModeReplace
	case domains.WriteModeUpsert:
		return domains.WriteModeUpsert
	default:
		return domains.WriteModeInsert
	}
}

func writeRestoreBatch(ctx context.Context, conn connector.Connector, table string, rows []mapper.Row, opts restoreRunOptions) (int64, error) {
	var lastErr error
	for attempt := 0; attempt <= opts.RetryTimes; attempt++ {
		affected, err := conn.WriteBatch(ctx, rows, connector.WriteOptions{
			Table:     table,
			WriteMode: opts.WriteMode,
		})
		if err == nil {
			return affected, nil
		}
		lastErr = err
		if attempt >= opts.RetryTimes {
			break
		}
		timer := time.NewTimer(opts.RetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return 0, ctx.Err()
		case <-timer.C:
		}
	}
	return 0, lastErr
}

func restoreGuid(backup *domains.DatabaseBackup, target *domains.DataSource, now int64) string {
	timestamp := time.UnixMilli(now).Format("20060102_150405")
	parts := []string{"rs", timestamp}
	if backup != nil {
		if source := safeGuidPart(backup.Guid, 12); source != "" {
			parts = append(parts, source)
		}
	}
	if target != nil {
		if targetName := safeGuidPart(target.Name, 10); targetName != "" {
			parts = append(parts, targetName)
		}
		if targetGuid := safeGuidPart(target.Guid, 6); targetGuid != "" {
			parts = append(parts, targetGuid)
		}
	}
	return strings.Join(parts, "_")
}

func defaultRestoreBatchSize() int {
	setting, err := ServiceGroupApp.SystemSettingService.GetSyncSetting()
	if err == nil && setting.RestoreBatchSize > 0 {
		return setting.RestoreBatchSize
	}
	return defaultBackupBatchSize()
}
