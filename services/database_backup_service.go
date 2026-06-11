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
	"database-sync-go/utils"

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
	DataSourceGuid string   `json:"dataSourceGuid"`
	Tables         []string `json:"tables"`
	BatchSize      int      `json:"batchSize"`
	Remark         string   `json:"remark"`
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
	req.Remark = strings.TrimSpace(req.Remark)
	if req.DataSourceGuid == "" {
		return nil, errors.New("dataSourceGuid required")
	}
	if req.BatchSize <= 0 {
		req.BatchSize = defaultBackupBatchSize()
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
		DataSourceGuid: source.Guid,
		DataSourceName: source.Name,
		DataSourceType: source.Type,
		Database:       source.Database,
		Tables:         string(tablesJSON),
		Format:         domains.BackupFormatJSONLZip,
		Status:         domains.BackupStatusPending,
		TotalTables:    len(req.Tables),
		StartTime:      now,
		Remark:         req.Remark,
	}
	row.CreateTime = now
	row.UpdateTime = now
	if err := s.DB().Create(&row).Error; err != nil {
		return nil, err
	}

	go s.runBackup(row.Guid, *source, req.Tables, req.BatchSize)
	return &row, nil
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

func (s DatabaseBackupService) runBackup(backupGuid string, source domains.DataSource, tables []string, batchSize int) {
	ctx, cancel := context.WithTimeout(context.Background(), backupTimeout())
	defer cancel()

	db := s.DB()
	start := domains.NowMilli()
	_ = db.Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
		"status":      domains.BackupStatusRunning,
		"start_time":  start,
		"update_time": start,
	}).Error

	filePath, fileName, totalTables, totalRows, err := s.writeBackup(ctx, backupGuid, source, tables, batchSize)
	if err != nil {
		s.finishBackup(backupGuid, domains.BackupStatusFailed, "", "", 0, totalTables, totalRows, err)
		return
	}
	fileSize := int64(0)
	if info, statErr := os.Stat(filePath); statErr == nil {
		fileSize = info.Size()
	}
	s.finishBackup(backupGuid, domains.BackupStatusSuccess, filePath, fileName, fileSize, totalTables, totalRows, nil)
}

func (s DatabaseBackupService) writeBackup(ctx context.Context, backupGuid string, source domains.DataSource, tables []string, batchSize int) (string, string, int, int64, error) {
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
		"tables":       string(tablesJSON),
		"total_tables": len(tables),
		"update_time":  domains.NowMilli(),
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
		tableMeta, err := s.writeTableBackup(ctx, zipWriter, conn, table, i, batchSize)
		if err != nil {
			_ = zipWriter.Close()
			return filePath, fileName, len(tables), totalRows, err
		}
		manifest.Tables = append(manifest.Tables, tableMeta)
		totalRows += tableMeta.RowCount
		_ = s.DB().Model(&domains.DatabaseBackup{}).Where("guid = ?", backupGuid).Updates(map[string]any{
			"total_rows":  totalRows,
			"update_time": domains.NowMilli(),
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

func (s DatabaseBackupService) writeTableBackup(ctx context.Context, zipWriter *zip.Writer, conn connector.Connector, table string, index int, batchSize int) (backupTableManifest, error) {
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
	var rowCount int64
	offset := 0
	for {
		select {
		case <-ctx.Done():
			return backupTableManifest{}, ctx.Err()
		default:
		}
		rows, err := conn.QueryBatch(ctx, connector.QueryOptions{
			Table:  table,
			Limit:  batchSize,
			Offset: offset,
		})
		if err != nil {
			return backupTableManifest{}, fmt.Errorf("query table %s failed: %w", table, err)
		}
		for _, row := range rows {
			if err := encoder.Encode(row); err != nil {
				return backupTableManifest{}, err
			}
		}
		rowCount += int64(len(rows))
		if len(rows) < batchSize {
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

func backupFileName(source domains.DataSource, backupGuid string) string {
	name := safeZipName(strings.TrimSpace(source.Name))
	if name == "" {
		name = safeZipName(source.Guid)
	}
	return fmt.Sprintf("%s_%s_%s.zip", name, time.Now().Format("20060102_150405"), safeZipName(backupGuid))
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

func backupTimeout() time.Duration {
	if global.NAV_VIPER != nil {
		if value := global.NAV_VIPER.GetDuration("backup.timeout"); value > 0 {
			return value
		}
	}
	return 6 * time.Hour
}
