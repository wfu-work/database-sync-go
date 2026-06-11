package services

import (
	"context"
	"errors"
	"strings"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/connector"
	"database-sync-go/syncer/mapper"
	"database-sync-go/utils"

	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type DataSourceService struct {
	commonServices.CrudService[domains.DataSource]
}

func (s DataSourceService) WithDB(db *gorm.DB) DataSourceService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

type SaveDataSourceRequest struct {
	Guid     string `json:"guid"`
	Name     string `json:"name"`
	Type     string `json:"type"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
	Params   string `json:"params"`
	Remark   string `json:"remark"`
	Status   *int   `json:"status"`
}

type PublicDataSource struct {
	Guid       string `json:"guid"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	Username   string `json:"username"`
	Password   string `json:"password"`
	Database   string `json:"database"`
	Params     string `json:"params"`
	Remark     string `json:"remark"`
	Status     int    `json:"status"`
	CreateTime int64  `json:"createTime"`
	UpdateTime int64  `json:"updateTime"`
}

type PreviewTableRequest struct {
	Table       string `json:"table" form:"table"`
	WhereClause string `json:"whereClause" form:"whereClause"`
	Limit       int    `json:"limit" form:"limit"`
}

type TablePreviewResult struct {
	Rows  []mapper.Row `json:"rows"`
	Count int          `json:"count"`
}

func (s DataSourceService) List(params map[string]string) ([]PublicDataSource, int64, error) {
	db := s.DB().Model(&domains.DataSource{})
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("name LIKE ? OR guid LIKE ? OR host LIKE ? OR database LIKE ? OR remark LIKE ?", like, like, like, like, like)
	}
	if sourceType := strings.TrimSpace(params["type"]); sourceType != "" {
		db = db.Where("type = ?", normalizeDataSourceType(sourceType))
	}
	if statusParam := strings.TrimSpace(params["status"]); statusParam != "" {
		db = db.Where("status = ?", utils.Str2Int(statusParam))
	}

	var total int64
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var items []domains.DataSource
	if allParam(params) {
		err := db.Order("update_time DESC").Find(&items).Error
		return publicDataSources(items), total, err
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
	return publicDataSources(items), total, err
}

func (s DataSourceService) Save(req SaveDataSourceRequest) (*PublicDataSource, error) {
	req.Guid = strings.TrimSpace(req.Guid)
	req.Name = strings.TrimSpace(req.Name)
	req.Type = normalizeDataSourceType(req.Type)
	req.Host = strings.TrimSpace(req.Host)
	req.Username = strings.TrimSpace(req.Username)
	req.Database = strings.TrimSpace(req.Database)
	req.Params = strings.TrimSpace(req.Params)
	req.Remark = strings.TrimSpace(req.Remark)

	if req.Name == "" {
		return nil, errors.New("name required")
	}
	if req.Type == "" {
		return nil, errors.New("type required")
	}
	if req.Type != domains.DataSourceTypeMySQL && req.Type != domains.DataSourceTypeTDengine {
		return nil, errors.New("unsupported datasource type")
	}
	if req.Host == "" {
		return nil, errors.New("host required")
	}
	if req.Port <= 0 {
		req.Port = defaultDataSourcePort(req.Type)
	}
	status := int(domains.StatusEnabled)
	if req.Status != nil {
		status = *req.Status
	}

	now := domains.NowMilli()
	var row domains.DataSource
	var err error
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
	row.Type = req.Type
	row.Host = req.Host
	row.Port = req.Port
	row.Username = req.Username
	if shouldUpdatePassword(req.Password, row) {
		encrypted, err := utils.EncryptSecret(req.Password)
		if err != nil {
			return nil, err
		}
		row.Password = encrypted
	}
	row.Database = req.Database
	row.Params = req.Params
	row.Remark = req.Remark
	row.Status = status
	row.UpdateTime = now

	if err := s.DB().Save(&row).Error; err != nil {
		return nil, err
	}
	return publicDataSource(row), nil
}

func (s DataSourceService) Delete(guid string) error {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return errors.New("guid required")
	}
	var count int64
	if err := s.DB().Model(&domains.SyncTask{}).Where("source_guid = ? OR target_guid = ?", guid, guid).Count(&count).Error; err != nil {
		return err
	}
	if count > 0 {
		return errors.New("datasource is used by sync task")
	}
	return s.DB().Unscoped().Where("guid = ?", guid).Delete(&domains.DataSource{}).Error
}

func (s DataSourceService) GetEnabled(guid string) (*domains.DataSource, error) {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return nil, errors.New("datasource guid required")
	}
	var row domains.DataSource
	err := s.DB().Where("guid = ? AND status = ?", guid, int(domains.StatusEnabled)).First(&row).Error
	if err != nil {
		return nil, errors.New("datasource not found or disabled")
	}
	return &row, nil
}

func (s DataSourceService) MigratePlaintextPasswords() error {
	var items []domains.DataSource
	if err := s.DB().Where("password != '' AND password NOT LIKE ?", "enc:v1:%").Find(&items).Error; err != nil {
		return err
	}
	for _, item := range items {
		encrypted, err := utils.EncryptSecret(item.Password)
		if err != nil {
			return err
		}
		if err := s.DB().Model(&domains.DataSource{}).Where("guid = ?", item.Guid).Updates(map[string]any{
			"password":    encrypted,
			"update_time": domains.NowMilli(),
		}).Error; err != nil {
			return err
		}
	}
	return nil
}

func (s DataSourceService) TestConnection(guid string) error {
	source, err := s.GetEnabled(guid)
	if err != nil {
		return err
	}
	conn, err := connector.New(*source)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Test()
}

func (s DataSourceService) Tables(guid string) ([]connector.TableInfo, error) {
	source, err := s.GetEnabled(guid)
	if err != nil {
		return nil, err
	}
	conn, err := connector.New(*source)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return conn.ListTables(ctx)
}

func (s DataSourceService) Columns(guid string, table string) ([]connector.ColumnInfo, error) {
	table = strings.TrimSpace(table)
	if table == "" {
		return nil, errors.New("table required")
	}
	source, err := s.GetEnabled(guid)
	if err != nil {
		return nil, err
	}
	conn, err := connector.New(*source)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return conn.DescribeTable(ctx, table)
}

func (s DataSourceService) Preview(guid string, req PreviewTableRequest) (*TablePreviewResult, error) {
	req.Table = strings.TrimSpace(req.Table)
	req.WhereClause = strings.TrimSpace(req.WhereClause)
	if req.Table == "" {
		return nil, errors.New("table required")
	}
	if req.Limit <= 0 {
		req.Limit = 20
	}
	if req.Limit > 100 {
		req.Limit = 100
	}
	source, err := s.GetEnabled(guid)
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
		Table:       req.Table,
		WhereClause: req.WhereClause,
		Limit:       req.Limit,
		Offset:      0,
	})
	if err != nil {
		return nil, err
	}
	return &TablePreviewResult{Rows: rows, Count: len(rows)}, nil
}

func normalizeDataSourceType(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	switch value {
	case "taos", "td", "tdengine":
		return domains.DataSourceTypeTDengine
	default:
		return value
	}
}

func defaultDataSourcePort(sourceType string) int {
	switch sourceType {
	case domains.DataSourceTypeTDengine:
		return 6041
	default:
		return 3306
	}
}

func shouldUpdatePassword(password string, row domains.DataSource) bool {
	password = strings.TrimSpace(password)
	if password == "" {
		return row.Guid == "" && row.Password != ""
	}
	if password == utils.MaskSecret(row.Password) {
		return false
	}
	return true
}

func publicDataSources(items []domains.DataSource) []PublicDataSource {
	out := make([]PublicDataSource, 0, len(items))
	for _, item := range items {
		out = append(out, *publicDataSource(item))
	}
	return out
}

func publicDataSource(item domains.DataSource) *PublicDataSource {
	return &PublicDataSource{
		Guid:       item.Guid,
		Name:       item.Name,
		Type:       item.Type,
		Host:       item.Host,
		Port:       item.Port,
		Username:   item.Username,
		Password:   utils.MaskSecret(item.Password),
		Database:   item.Database,
		Params:     item.Params,
		Remark:     item.Remark,
		Status:     item.Status,
		CreateTime: item.CreateTime,
		UpdateTime: item.UpdateTime,
	}
}
