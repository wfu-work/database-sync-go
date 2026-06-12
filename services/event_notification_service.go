package services

import (
	"fmt"
	"strings"

	"database-sync-go/domains"
	"database-sync-go/utils"

	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	"gorm.io/gorm"
)

type EventNotificationService struct {
	commonServices.CrudService[domains.EventNotification]
}

func (s EventNotificationService) WithDB(db *gorm.DB) EventNotificationService {
	s.CrudService = *s.CrudService.WithDB(db)
	return s
}

type CreateEventNotificationRequest struct {
	Type       string
	Level      string
	Title      string
	Content    string
	SourceType string
	SourceGuid string
	SourceName string
}

func (s EventNotificationService) List(params map[string]string) ([]domains.EventNotification, int64, error) {
	db := s.DB().Model(&domains.EventNotification{})
	if level := strings.TrimSpace(params["level"]); level != "" {
		db = db.Where("level = ?", level)
	}
	if read := strings.TrimSpace(params["read"]); read != "" {
		db = db.Where("read = ?", utils.Str2Int(read))
	}
	if sourceType := strings.TrimSpace(params["sourceType"]); sourceType != "" {
		db = db.Where("source_type = ?", sourceType)
	}
	if sourceGuid := strings.TrimSpace(params["sourceGuid"]); sourceGuid != "" {
		db = db.Where("source_guid = ?", sourceGuid)
	}
	if keyword := strings.TrimSpace(utils.FirstNonEmpty(params["keyword"], params["content"])); keyword != "" {
		like := "%" + keyword + "%"
		db = db.Where("title LIKE ? OR content LIKE ? OR source_name LIKE ?", like, like, like)
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
	if size > 100 {
		size = 100
	}
	var items []domains.EventNotification
	err := db.Order("event_time DESC, create_time DESC").Limit(size).Offset((page - 1) * size).Find(&items).Error
	return items, total, err
}

func (s EventNotificationService) CreateEvent(req CreateEventNotificationRequest) error {
	req.Type = strings.TrimSpace(req.Type)
	req.Level = strings.TrimSpace(req.Level)
	req.Title = strings.TrimSpace(req.Title)
	req.Content = strings.TrimSpace(req.Content)
	req.SourceType = strings.TrimSpace(req.SourceType)
	req.SourceGuid = strings.TrimSpace(req.SourceGuid)
	req.SourceName = strings.TrimSpace(req.SourceName)
	if req.Type == "" || req.Title == "" {
		return nil
	}
	if req.Level == "" {
		req.Level = domains.EventLevelInfo
	}
	now := domains.NowMilli()
	row := domains.EventNotification{
		Type:       req.Type,
		Level:      req.Level,
		Title:      req.Title,
		Content:    req.Content,
		SourceType: req.SourceType,
		SourceGuid: req.SourceGuid,
		SourceName: req.SourceName,
		Read:       0,
		EventTime:  now,
	}
	row.CreateTime = now
	row.UpdateTime = now
	if err := s.DB().Create(&row).Error; err != nil {
		return err
	}
	ServiceGroupApp.WebSocketService.Broadcast(WebSocketEventNotificationCreated, row)
	return nil
}

func (s EventNotificationService) MarkRead(guid string) error {
	guid = strings.TrimSpace(guid)
	if guid == "" {
		return nil
	}
	return s.DB().Model(&domains.EventNotification{}).Where("guid = ?", guid).Updates(map[string]any{
		"read":        1,
		"update_time": domains.NowMilli(),
	}).Error
}

func (s EventNotificationService) MarkAllRead() error {
	return s.DB().Model(&domains.EventNotification{}).Where("read = ?", 0).Updates(map[string]any{
		"read":        1,
		"update_time": domains.NowMilli(),
	}).Error
}

func (s EventNotificationService) NotifyDataSourceConnectionFailed(source domains.DataSource, err error) {
	content := fmt.Sprintf("数据源 %s（%s:%d）连接检查失败。", source.Name, source.Host, source.Port)
	if err != nil {
		content += "原因：" + err.Error()
	}
	_ = s.CreateEvent(CreateEventNotificationRequest{
		Type:       domains.EventTypeDataSourceConnectionFailed,
		Level:      domains.EventLevelError,
		Title:      "数据源连接失败",
		Content:    content,
		SourceType: domains.EventSourceDataSource,
		SourceGuid: source.Guid,
		SourceName: source.Name,
	})
}

func (s EventNotificationService) NotifyDataSourceConnectionRecovered(source domains.DataSource) {
	_ = s.CreateEvent(CreateEventNotificationRequest{
		Type:       domains.EventTypeDataSourceConnectionRecovered,
		Level:      domains.EventLevelInfo,
		Title:      "数据源连接已恢复",
		Content:    fmt.Sprintf("数据源 %s（%s:%d）连接检查已恢复正常。", source.Name, source.Host, source.Port),
		SourceType: domains.EventSourceDataSource,
		SourceGuid: source.Guid,
		SourceName: source.Name,
	})
}

func (s EventNotificationService) NotifySyncRunFinished(task domains.SyncTask, runGuid string, status string, err error) {
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
	_ = s.CreateEvent(CreateEventNotificationRequest{
		Type:       eventType,
		Level:      level,
		Title:      title,
		Content:    content,
		SourceType: domains.EventSourceSyncRun,
		SourceGuid: runGuid,
		SourceName: task.Name,
	})
}

func (s EventNotificationService) NotifyBackupFinished(backup domains.DatabaseBackup, err error) {
	level := domains.EventLevelInfo
	eventType := domains.EventTypeBackupSuccess
	title := "数据库备份完成"
	content := fmt.Sprintf("数据源 %s 的备份已完成，共 %d 张表、%d 行。", backup.DataSourceName, backup.TotalTables, backup.TotalRows)
	if backup.Status != domains.BackupStatusSuccess {
		level = domains.EventLevelError
		eventType = domains.EventTypeBackupFailed
		title = "数据库备份失败"
		content = fmt.Sprintf("数据源 %s 的备份执行失败。", backup.DataSourceName)
		if err != nil {
			content += "原因：" + err.Error()
		} else if strings.TrimSpace(backup.LastError) != "" {
			content += "原因：" + backup.LastError
		}
	}
	_ = s.CreateEvent(CreateEventNotificationRequest{
		Type:       eventType,
		Level:      level,
		Title:      title,
		Content:    content,
		SourceType: domains.EventSourceBackup,
		SourceGuid: backup.Guid,
		SourceName: backup.DataSourceName,
	})
}
