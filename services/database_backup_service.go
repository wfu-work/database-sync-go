package services

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/connector"
	"database-sync-go/syncer/mapper"
	"database-sync-go/utils"

	commonDomains "github.com/wfu-work/nav-common-go-lib/domains"
	"github.com/wfu-work/nav-common-go-lib/global"
	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type DatabaseBackupService struct {
	commonServices.CrudService[domains.DatabaseBackup]
}

func (s DatabaseBackupService) WithDB(db *gorm.DB) DatabaseBackupService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

type StartDatabaseBackupRequest struct {
	DataSourceGuid   string   `json:"dataSourceGuid"`
	Tables           []string `json:"tables"`
	BatchSize        int      `json:"batchSize"`
	ConnectionParams string   `json:"connectionParams"`
	RetryTimes       int      `json:"retryTimes"`
	RetryIntervalMs  int      `json:"retryIntervalMs"`
	Remark           string   `json:"remark"`
}

type backupRunOptions struct {
	BatchSize        int
	ConnectionParams string
	RetryTimes       int
	RetryInterval    time.Duration
}

type backupManifest struct {
	BackupGuid     string                `json:"backupGuid"`
	DataSourceGuid string                `json:"dataSourceGuid"`
	DataSourceName string                `json:"dataSourceName"`
	DataSourceType string                `json:"dataSourceType"`
	Database       string                `json:"database"`
	Format         string                `json:"format"`
	CreateTime     int64                 `json:"createTime"`
	TotalTables    int                   `json:"totalTables"`
	TotalRows      int64                 `json:"totalRows"`
	Tables         []backupTableManifest `json:"tables"`
}

type backupTableManifest struct {
	Name      string                 `json:"name"`
	Schema    string                 `json:"schema"`
	Data      string                 `json:"data"`
	Columns   []connector.ColumnInfo `json:"columns"`
	RowCount  int64                  `json:"rowCount"`
	BackupEnd int64                  `json:"backupEnd"`
}

func (s DatabaseBackupService) List(params map[string]string) ([]domains.DatabaseBackup, int64, error) {
	db := s.DB().Model(&domains.DatabaseBackup{})
	if dataSourceGuid := strings.TrimSpace(params["dataSourceGuid"]); dataSourceGuid != "" {
		db = db.Where("data_source_guid = ?", dataSourceGuid)
	}
	if status := strings.TrimSpace(params["status"]); status != "" {
		db = db.Where("status = ?", status)
	}
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("guid LIKE ? OR data_source_name LIKE ? OR database LIKE ? OR tables LIKE ? OR remark LIKE ? OR last_error LIKE ?", like, like, like, like, like, like)
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
	var items []domains.DatabaseBackup
	err := db.Order("start_time DESC, create_time DESC").Limit(size).Offset((page - 1) * size).Find(&items).Error
	return items, total, err
}

func (s DatabaseBackupService) Get(guid string) (*domains.DatabaseBackup, error) {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return nil, errors.New("backup guid required")
	}
	var row domains.DatabaseBackup
	if err := s.DB().Where("guid = ?", guid).First(&row).Error; err != nil {
		return nil, errors.New("database backup not found")
	}
	return &row, nil
}

func (s DatabaseBackupService) Start(req StartDatabaseBackupRequest) (*domains.DatabaseBackup, error) {
	req.DataSourceGuid = strings.TrimSpace(req.DataSourceGuid)
	req.Tables = normalizeBackupTables(req.Tables)
	req.ConnectionParams = strings.TrimSpace(req.ConnectionParams)
	req.Remark = strings.TrimSpace(req.Remark)
	if req.DataSourceGuid == "" {
		return nil, errors.New("dataSourceGuid required")
	}
	if req.BatchSize <= 0 {
		req.BatchSize = defaultBackupBatchSize()
	}
	if req.RetryTimes < 0 {
		req.RetryTimes = 0
	}
	if req.RetryIntervalMs <= 0 {
		req.RetryIntervalMs = defaultBackupRetryIntervalMs()
	}
	if _, err := parseBackupParams(req.ConnectionParams); err != nil {
		return nil, err
	}

	source, err := ServiceGroupApp.DataSourceService.GetEnabled(req.DataSourceGuid)
	if err != nil {
		return nil, err
	}
	tablesJSON, err := json.Marshal(req.Tables)
	if err != nil {
		return nil, err
	}
	now := domains.NowMilli()
	row := domains.DatabaseBackup{
		BaseDataEntity: commonDomains.BaseDataEntity{
			Guid: backupGuid(source, now),
		},
		DataSourceGuid:   source.Guid,
		DataSourceName:   source.Name,
		DataSourceType:   source.Type,
		Database:         source.Database,
		Tables:           string(tablesJSON),
		ConnectionParams: req.ConnectionParams,
		BatchSize:        req.BatchSize,
		RetryTimes:       req.RetryTimes,
		RetryIntervalMs:  req.RetryIntervalMs,
		Format:           domains.BackupFormatJSONLZip,
		Status:           domains.BackupStatusPending,
		TotalTables:      len(req.Tables),
		StartTime:        now,
		Remark:           req.Remark,
	}
	row.CreateTime = now
	row.UpdateTime = now
	if err := s.DB().Create(&row).Error; err != nil {
		return nil, err
	}

	go s.runBackup(row.Guid, *source, req.Tables, backupRunOptions{
		BatchSize:        req.BatchSize,
		ConnectionParams: req.ConnectionParams,
		RetryTimes:       req.RetryTimes,
		RetryInterval:    time.Duration(req.RetryIntervalMs) * time.Millisecond,
	})
	return &row, nil
}

func (s DatabaseBackupService) Retry(guid string) (*domains.DatabaseBackup, error) {
	row, err := s.Get(guid)
	if err != nil {
		return nil, err
	}
	if row.Status != domains.BackupStatusFailed {
		return nil, errors.New("only failed backup can be retried")
	}
	var tables []string
	if strings.TrimSpace(row.Tables) != "" {
		if err := json.Unmarshal([]byte(row.Tables), &tables); err != nil {
			return nil, fmt.Errorf("parse backup tables failed: %w", err)
		}
	}
	source, err := ServiceGroupApp.DataSourceService.GetEnabled(row.DataSourceGuid)
	if err != nil {
		return nil, err
	}
	now := domains.NowMilli()
	tables = normalizeBackupTables(tables)
	tablesJSON, _ := json.Marshal(tables)
	batchSize := row.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBackupBatchSize()
	}
	retryIntervalMs := row.RetryIntervalMs
	if retryIntervalMs <= 0 {
		retryIntervalMs = defaultBackupRetryIntervalMs()
	}
	if err := s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", row.Guid).Updates(map[string]any{
		"data_source_name":  source.Name,
		"data_source_type":  source.Type,
		"database":          source.Database,
		"tables":            string(tablesJSON),
		"format":            domains.BackupFormatJSONLZip,
		"status":            domains.BackupStatusPending,
		"batch_size":        batchSize,
		"retry_times":       row.RetryTimes,
		"retry_interval_ms": retryIntervalMs,
		"total_tables":      len(tables),
		"finished_tables":   0,
		"current_table":     "",
		"current_rows":      0,
		"current_total":     0,
		"current_batch":     0,
		"current_started":   0,
		"total_rows":        0,
		"file_name":         "",
		"file_path":         "",
		"file_size":         0,
		"start_time":        now,
		"end_time":          0,
		"duration_ms":       0,
		"last_error":        "",
		"update_time":       now,
	}).Error; err != nil {
		return nil, err
	}
	go s.runBackup(row.Guid, *source, tables, backupRunOptions{
		BatchSize:        batchSize,
		ConnectionParams: row.ConnectionParams,
		RetryTimes:       row.RetryTimes,
		RetryInterval:    time.Duration(retryIntervalMs) * time.Millisecond,
	})
	return s.Get(row.Guid)
}

func (s DatabaseBackupService) DownloadInfo(guid string) (string, string, error) {
	row, err := s.Get(guid)
	if err != nil {
		return "", "", err
	}
	if row.Status != domains.BackupStatusSuccess {
		return "", "", errors.New("database backup is not ready")
	}
	if strings.TrimSpace(row.FilePath) == "" {
		return "", "", errors.New("database backup file missing")
	}
	if _, err := os.Stat(row.FilePath); err != nil {
		return "", "", err
	}
	return row.FilePath, row.FileName, nil
}

func (s DatabaseBackupService) Delete(guid string) error {
	row, err := s.Get(guid)
	if err != nil {
		return err
	}
	if row.Status == domains.BackupStatusPending || row.Status == domains.BackupStatusRunning {
		return errors.New("backup is running, cannot delete")
	}
	if strings.TrimSpace(row.FilePath) != "" {
		if err := os.Remove(row.FilePath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return s.DB().Unscoped().Where("guid = ?", row.Guid).Delete(&domains.DatabaseBackup{}).Error
}

func (s DatabaseBackupService) RecoverStaleBackups() error {
	db := s.DB()
	if db == nil {
		return errors.New("database not initialized")
	}
	now := domains.NowMilli()
	return db.Model(&domains.DatabaseBackup{}).
		Where("status IN ?", []string{domains.BackupStatusPending, domains.BackupStatusRunning}).
		Updates(map[string]any{
			"status":      domains.BackupStatusFailed,
			"end_time":    now,
			"duration_ms": gorm.Expr("CASE WHEN start_time > 0 THEN ? - start_time ELSE 0 END", now),
			"last_error":  "服务重启或备份进程中断，备份未完成",
			"update_time": now,
		}).Error
}

func (s DatabaseBackupService) runBackup(backupGuid string, source domains.DataSource, tables []string, opts backupRunOptions) {
	ctx, cancel := context.WithTimeout(context.Background(), backupTimeout())
	defer cancel()
	opts = normalizeBackupRunOptions(opts)
	source.Params = mergeBackupParams(source.Params, opts.ConnectionParams)

	db := s.DB()
	start := domains.NowMilli()
	_ = db.Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
		"status":          domains.BackupStatusRunning,
		"start_time":      start,
		"finished_tables": 0,
		"current_table":   "",
		"current_rows":    0,
		"current_total":   0,
		"current_batch":   0,
		"current_started": 0,
		"update_time":     start,
	}).Error

	filePath, fileName, totalTables, totalRows, err := s.writeBackup(ctx, backupGuid, source, tables, opts)
	if err != nil {
		removePartialBackupFile(filePath)
		s.finishBackup(backupGuid, domains.BackupStatusFailed, "", "", 0, totalTables, totalRows, err)
		return
	}
	fileSize := int64(0)
	if info, statErr := os.Stat(filePath); statErr == nil {
		fileSize = info.Size()
	}
	s.finishBackup(backupGuid, domains.BackupStatusSuccess, filePath, fileName, fileSize, totalTables, totalRows, nil)
}

func (s DatabaseBackupService) writeBackup(ctx context.Context, backupGuid string, source domains.DataSource, tables []string, opts backupRunOptions) (string, string, int, int64, error) {
	conn, err := connector.New(source)
	if err != nil {
		return "", "", len(tables), 0, err
	}
	defer conn.Close()

	if len(tables) == 0 {
		tableInfos, err := conn.ListTables(ctx)
		if err != nil {
			return "", "", 0, 0, err
		}
		tables = make([]string, 0, len(tableInfos))
		for _, table := range tableInfos {
			if strings.TrimSpace(table.Name) != "" {
				tables = append(tables, table.Name)
			}
		}
	}
	if len(tables) == 0 {
		return "", "", 0, 0, errors.New("no tables to backup")
	}
	tablesJSON, _ := json.Marshal(tables)
	_ = s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
		"tables":          string(tablesJSON),
		"total_tables":    len(tables),
		"finished_tables": 0,
		"current_table":   "",
		"current_rows":    0,
		"current_total":   0,
		"current_batch":   0,
		"current_started": 0,
		"update_time":     domains.NowMilli(),
	}).Error

	dir := backupDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", len(tables), 0, err
	}
	fileName := backupFileName(source, backupGuid)
	filePath := filepath.Join(dir, fileName)
	file, err := os.Create(filePath)
	if err != nil {
		return "", "", len(tables), 0, err
	}
	closeFile := true
	defer func() {
		if closeFile {
			_ = file.Close()
		}
	}()

	zipWriter := zip.NewWriter(file)
	manifest := backupManifest{
		BackupGuid:     backupGuid,
		DataSourceGuid: source.Guid,
		DataSourceName: source.Name,
		DataSourceType: source.Type,
		Database:       source.Database,
		Format:         domains.BackupFormatJSONLZip,
		CreateTime:     domains.NowMilli(),
		TotalTables:    len(tables),
		Tables:         make([]backupTableManifest, 0, len(tables)),
	}
	var totalRows int64
	for i, table := range tables {
		select {
		case <-ctx.Done():
			_ = zipWriter.Close()
			return filePath, fileName, len(tables), totalRows, ctx.Err()
		default:
		}
		_ = s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
			"current_table":   table,
			"current_rows":    0,
			"current_total":   0,
			"current_batch":   0,
			"current_started": domains.NowMilli(),
			"update_time":     domains.NowMilli(),
		}).Error
		tableMeta, err := s.writeTableBackup(ctx, backupGuid, zipWriter, conn, table, i, opts)
		if err != nil {
			_ = zipWriter.Close()
			return filePath, fileName, len(tables), totalRows, err
		}
		manifest.Tables = append(manifest.Tables, tableMeta)
		totalRows += tableMeta.RowCount
		_ = s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
			"finished_tables": i + 1,
			"total_rows":      totalRows,
			"current_rows":    tableMeta.RowCount,
			"current_total":   tableMeta.RowCount,
			"update_time":     domains.NowMilli(),
		}).Error
	}
	manifest.TotalRows = totalRows
	if err := writeZipJSON(zipWriter, "manifest.json", manifest); err != nil {
		_ = zipWriter.Close()
		return filePath, fileName, len(tables), totalRows, err
	}
	if err := zipWriter.Close(); err != nil {
		return filePath, fileName, len(tables), totalRows, err
	}
	if err := file.Close(); err != nil {
		return filePath, fileName, len(tables), totalRows, err
	}
	closeFile = false
	return filePath, fileName, len(tables), totalRows, nil
}

func (s DatabaseBackupService) writeTableBackup(ctx context.Context, backupGuid string, zipWriter *zip.Writer, conn connector.Connector, table string, index int, opts backupRunOptions) (backupTableManifest, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return backupTableManifest{}, errors.New("table required")
	}
	columns, err := conn.DescribeTable(ctx, table)
	if err != nil {
		return backupTableManifest{}, fmt.Errorf("describe table %s failed: %w", table, err)
	}
	basePath := fmt.Sprintf("tables/%03d_%s", index+1, safeZipName(table))
	schemaPath := basePath + "/schema.json"
	dataPath := basePath + "/data.jsonl"
	if err := writeZipJSON(zipWriter, schemaPath, columns); err != nil {
		return backupTableManifest{}, err
	}

	dataWriter, err := zipWriter.Create(dataPath)
	if err != nil {
		return backupTableManifest{}, err
	}
	encoder := json.NewEncoder(dataWriter)
	estimatedRows, countErr := conn.Count(ctx, connector.QueryOptions{Table: table})
	if countErr == nil {
		_ = s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
			"current_total": estimatedRows,
			"update_time":   domains.NowMilli(),
		}).Error
	}
	var rowCount int64
	offset := 0
	batchIndex := 0
	for {
		select {
		case <-ctx.Done():
			return backupTableManifest{}, ctx.Err()
		default:
		}
		rows, err := queryBackupBatch(ctx, conn, connector.QueryOptions{
			Table:  table,
			Limit:  opts.BatchSize,
			Offset: offset,
		}, opts)
		if err != nil {
			return backupTableManifest{}, fmt.Errorf("query table %s failed: %w", table, err)
		}
		for _, row := range rows {
			if err := encoder.Encode(row); err != nil {
				return backupTableManifest{}, err
			}
		}
		rowCount += int64(len(rows))
		batchIndex += 1
		if len(rows) > 0 || batchIndex == 1 {
			_ = s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
				"current_rows":  rowCount,
				"current_total": estimatedRows,
				"current_batch": batchIndex,
				"update_time":   domains.NowMilli(),
			}).Error
		}
		if len(rows) < opts.BatchSize {
			break
		}
		offset += len(rows)
	}

	return backupTableManifest{
		Name:      table,
		Schema:    schemaPath,
		Data:      dataPath,
		Columns:   columns,
		RowCount:  rowCount,
		BackupEnd: domains.NowMilli(),
	}, nil
}

func (s DatabaseBackupService) finishBackup(backupGuid string, status string, filePath string, fileName string, fileSize int64, totalTables int, totalRows int64, err error) {
	now := domains.NowMilli()
	updates := map[string]any{
		"status":       status,
		"end_time":     now,
		"duration_ms":  gorm.Expr("? - start_time", now),
		"total_tables": totalTables,
		"total_rows":   totalRows,
		"update_time":  now,
	}
	if status == domains.BackupStatusSuccess {
		updates["finished_tables"] = totalTables
		updates["current_table"] = ""
		updates["current_rows"] = 0
		updates["current_total"] = 0
		updates["current_batch"] = 0
		updates["current_started"] = 0
	}
	if filePath != "" {
		updates["file_path"] = filePath
	}
	if fileName != "" {
		updates["file_name"] = fileName
	}
	if fileSize > 0 {
		updates["file_size"] = fileSize
	}
	if err != nil {
		updates["last_error"] = err.Error()
	}
	_ = s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(updates).Error
	var backup domains.DatabaseBackup
	if findErr := s.DB().Where("guid = ?", backupGuid).First(&backup).Error; findErr == nil {
		ServiceGroupApp.EventNotificationService.NotifyBackupFinished(backup, err)
	}
}

func removePartialBackupFile(filePath string) {
	filePath = strings.TrimSpace(filePath)
	if filePath == "" {
		return
	}
	_ = os.Remove(filePath)
}

func writeZipJSON(zipWriter *zip.Writer, name string, value any) error {
	writer, err := zipWriter.Create(name)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func normalizeBackupTables(tables []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(tables))
	for _, table := range tables {
		table = strings.TrimSpace(table)
		key := strings.ToLower(table)
		if table == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, table)
	}
	return out
}

func normalizeBackupRunOptions(opts backupRunOptions) backupRunOptions {
	if opts.BatchSize <= 0 {
		opts.BatchSize = defaultBackupBatchSize()
	}
	if opts.RetryTimes < 0 {
		opts.RetryTimes = 0
	}
	if opts.RetryInterval <= 0 {
		opts.RetryInterval = time.Duration(defaultBackupRetryIntervalMs()) * time.Millisecond
	}
	return opts
}

func queryBackupBatch(ctx context.Context, conn connector.Connector, query connector.QueryOptions, opts backupRunOptions) ([]mapper.Row, error) {
	var lastErr error
	for attempt := 0; attempt <= opts.RetryTimes; attempt++ {
		rows, err := conn.QueryBatch(ctx, query)
		if err == nil {
			return rows, nil
		}
		lastErr = err
		if attempt >= opts.RetryTimes {
			break
		}
		timer := time.NewTimer(opts.RetryInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, lastErr
}

func parseBackupParams(raw string) (map[string]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return map[string]string{}, nil
	}
	var parsed map[string]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("connectionParams must be valid JSON object: %w", err)
	}
	for key, value := range parsed {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, errors.New("connectionParams contains empty key")
		}
		parsed[key] = strings.TrimSpace(value)
	}
	return parsed, nil
}

func mergeBackupParams(base string, override string) string {
	baseParams, _ := parseBackupParams(base)
	overrideParams, _ := parseBackupParams(override)
	for key, value := range overrideParams {
		if strings.TrimSpace(value) == "" {
			delete(baseParams, key)
			continue
		}
		baseParams[key] = value
	}
	if len(baseParams) == 0 {
		return ""
	}
	data, err := json.Marshal(baseParams)
	if err != nil {
		return override
	}
	return string(data)
}

func backupFileName(source domains.DataSource, backupGuid string) string {
	name := safeZipName(strings.TrimSpace(source.Name))
	if name == "" {
		name = safeZipName(source.Guid)
	}
	return fmt.Sprintf("%s_%s_%s.zip", name, time.Now().Format("20060102_150405"), safeZipName(backupGuid))
}

func backupGuid(source *domains.DataSource, now int64) string {
	timestamp := time.UnixMilli(now).Format("20060102_150405")
	parts := []string{"bk", timestamp}
	if source != nil {
		if name := safeGuidPart(source.Name, 12); name != "" {
			parts = append(parts, name)
		}
		if database := safeGuidPart(source.Database, 10); database != "" {
			parts = append(parts, database)
		}
		if guid := safeGuidPart(source.Guid, 6); guid != "" {
			parts = append(parts, guid)
		}
	}
	return strings.Join(parts, "_")
}

func safeGuidPart(value string, maxLen int) string {
	value = strings.ToLower(safeZipName(value))
	if maxLen > 0 && len(value) > maxLen {
		value = value[:maxLen]
	}
	return strings.Trim(value, "_-")
}

var unsafeZipNamePattern = regexp.MustCompile(`[^A-Za-z0-9_.-]+`)

func safeZipName(value string) string {
	value = strings.TrimSpace(value)
	value = strings.ReplaceAll(value, string(filepath.Separator), "_")
	value = unsafeZipNamePattern.ReplaceAllString(value, "_")
	value = strings.Trim(value, "._-")
	if len(value) > 80 {
		value = value[:80]
	}
	return value
}

func backupDir() string {
	if global.NAV_VIPER != nil {
		if value := strings.TrimSpace(global.NAV_VIPER.GetString("backup.path")); value != "" {
			return value
		}
		if value := strings.TrimSpace(global.NAV_VIPER.GetString("local.backup-path")); value != "" {
			return value
		}
	}
	return "./data/backups"
}

func defaultBackupBatchSize() int {
	if global.NAV_VIPER != nil {
		if value := global.NAV_VIPER.GetInt("backup.default-batch-size"); value > 0 {
			return value
		}
		if value := global.NAV_VIPER.GetInt("sync.default-batch-size"); value > 0 {
			return value
		}
	}
	return 1000
}

func defaultBackupRetryIntervalMs() int {
	if global.NAV_VIPER != nil {
		if value := global.NAV_VIPER.GetInt("backup.retry-interval-ms"); value > 0 {
			return value
		}
	}
	return 1500
}

func backupTimeout() time.Duration {
	if global.NAV_VIPER != nil {
		if value := global.NAV_VIPER.GetDuration("backup.timeout"); value > 0 {
			return value
		}
	}
	return 6 * time.Hour
}
