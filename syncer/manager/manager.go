package manager

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/worker"

	"github.com/robfig/cron/v3"
	"github.com/wfu-work/nav-common-go-lib/global"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

var DefaultManager = NewManager(nil)

type Manager struct {
	mu        sync.Mutex
	db        *gorm.DB
	running   map[string]runState
	scheduler *cron.Cron
	entries   map[string]scheduleState
	started   bool
}

type runState struct {
	TaskGuid  string
	RunGuid   string
	StartedAt int64
	Cancel    context.CancelFunc
}

type scheduleState struct {
	TaskGuid string
	TaskName string
	CronExpr string
	EntryID  cron.EntryID
}

type ScheduleItem struct {
	TaskGuid string `json:"taskGuid"`
	TaskName string `json:"taskName"`
	CronExpr string `json:"cronExpr"`
	EntryID  int    `json:"entryId"`
}

func NewManager(db *gorm.DB) *Manager {
	return &Manager{
		db:      db,
		running: map[string]runState{},
		entries: map[string]scheduleState{},
	}
}

func ValidateCronExpr(expr string) error {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return errors.New("cron expr required")
	}
	_, err := cronParser().Parse(expr)
	return err
}

func (m *Manager) RunTask(task domains.SyncTask) (*domains.SyncRun, error) {
	task.Guid = strings.TrimSpace(task.Guid)
	if task.Guid == "" {
		return nil, errors.New("task guid required")
	}
	db := m.DB()
	if db == nil {
		return nil, errors.New("database not initialized")
	}

	m.mu.Lock()
	if _, ok := m.running[task.Guid]; ok {
		m.mu.Unlock()
		return nil, errors.New("sync task is already running")
	}
	if maxWorkers := m.maxWorkers(); maxWorkers > 0 && len(m.running) >= maxWorkers {
		m.mu.Unlock()
		return nil, fmt.Errorf("max running sync tasks reached: %d", maxWorkers)
	}

	now := domains.NowMilli()
	run := domains.SyncRun{
		TaskGuid:    task.Guid,
		TaskName:    task.Name,
		Status:      domains.RunStatusPending,
		StartTime:   now,
		CursorStart: task.CursorValue,
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		return m.markTaskRunStarted(tx, task.Guid, run.Guid, now)
	}); err != nil {
		m.mu.Unlock()
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), m.runTimeout())
	m.running[task.Guid] = runState{
		TaskGuid:  task.Guid,
		RunGuid:   run.Guid,
		StartedAt: now,
		Cancel:    cancel,
	}
	m.mu.Unlock()

	go func() {
		defer cancel()
		defer m.removeRunning(task.Guid)
		worker.NewRunner(db).Run(ctx, task, run)
	}()
	return &run, nil
}

func (m *Manager) RetryRunErrors(sourceRunGuid string) (*domains.SyncRun, error) {
	sourceRunGuid = strings.TrimSpace(sourceRunGuid)
	if sourceRunGuid == "" {
		return nil, errors.New("run guid required")
	}
	db := m.DB()
	if db == nil {
		return nil, errors.New("database not initialized")
	}

	var sourceRun domains.SyncRun
	if err := db.Where("guid = ?", sourceRunGuid).First(&sourceRun).Error; err != nil {
		return nil, errors.New("source sync run not found")
	}
	var task domains.SyncTask
	if err := db.Where("guid = ? AND status = ?", sourceRun.TaskGuid, int(domains.StatusEnabled)).First(&task).Error; err != nil {
		return nil, errors.New("sync task not found or disabled")
	}

	var errorsToRetry []domains.SyncError
	if err := db.Where("run_guid = ?", sourceRunGuid).Order("create_time ASC").Find(&errorsToRetry).Error; err != nil {
		return nil, err
	}
	if len(errorsToRetry) == 0 {
		return nil, errors.New("source sync run has no failed rows")
	}

	m.mu.Lock()
	if _, ok := m.running[task.Guid]; ok {
		m.mu.Unlock()
		return nil, errors.New("sync task is already running")
	}
	if maxWorkers := m.maxWorkers(); maxWorkers > 0 && len(m.running) >= maxWorkers {
		m.mu.Unlock()
		return nil, fmt.Errorf("max running sync tasks reached: %d", maxWorkers)
	}

	now := domains.NowMilli()
	run := domains.SyncRun{
		TaskGuid:    task.Guid,
		TaskName:    task.Name + " retry",
		Status:      domains.RunStatusPending,
		StartTime:   now,
		CursorStart: task.CursorValue,
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&run).Error; err != nil {
			return err
		}
		return m.markTaskRunStarted(tx, task.Guid, run.Guid, now)
	}); err != nil {
		m.mu.Unlock()
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.runTimeout())
	m.running[task.Guid] = runState{
		TaskGuid:  task.Guid,
		RunGuid:   run.Guid,
		StartedAt: now,
		Cancel:    cancel,
	}
	m.mu.Unlock()

	go func() {
		defer cancel()
		defer m.removeRunning(task.Guid)
		worker.NewRunner(db).RetryErrors(ctx, task, errorsToRetry, run)
	}()
	return &run, nil
}

func (m *Manager) StopTask(taskGuid string) error {
	taskGuid = strings.TrimSpace(taskGuid)
	if taskGuid == "" {
		return errors.New("task guid required")
	}
	m.mu.Lock()
	state, ok := m.running[taskGuid]
	m.mu.Unlock()
	if !ok {
		return errors.New("sync task is not running")
	}
	state.Cancel()
	return nil
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(m.running))
	for _, state := range m.running {
		cancels = append(cancels, state.Cancel)
	}
	m.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

func (m *Manager) StartScheduler() error {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return nil
	}
	m.scheduler = newCron()
	m.started = true
	m.scheduler.Start()
	m.mu.Unlock()
	return m.ReloadSchedules()
}

func (m *Manager) StopScheduler() {
	m.mu.Lock()
	scheduler := m.scheduler
	m.scheduler = nil
	m.entries = map[string]scheduleState{}
	m.started = false
	m.mu.Unlock()
	if scheduler != nil {
		ctx := scheduler.Stop()
		<-ctx.Done()
	}
}

func (m *Manager) ReloadSchedules() error {
	db := m.DB()
	if db == nil {
		return errors.New("database not initialized")
	}
	m.mu.Lock()
	if !m.started {
		m.mu.Unlock()
		return nil
	}
	old := m.scheduler
	m.scheduler = newCron()
	m.entries = map[string]scheduleState{}
	m.scheduler.Start()
	m.mu.Unlock()

	if old != nil {
		ctx := old.Stop()
		<-ctx.Done()
	}

	var tasks []domains.SyncTask
	if err := db.Where("status = ? AND schedule_on = ? AND cron_expr != ''", int(domains.StatusEnabled), int(domains.StatusEnabled)).
		Order("update_time DESC").
		Find(&tasks).Error; err != nil {
		return err
	}

	for _, task := range tasks {
		task := task
		entryID, err := m.scheduler.AddFunc(task.CronExpr, func() {
			var latest domains.SyncTask
			if err := db.Where("guid = ? AND status = ?", task.Guid, int(domains.StatusEnabled)).First(&latest).Error; err != nil {
				global.NAV_LOG.Warn("scheduled sync task not found", zap.String("task", task.Guid), zap.Error(err))
				return
			}
			if latest.ScheduleOn != int(domains.StatusEnabled) || strings.TrimSpace(latest.CronExpr) == "" {
				return
			}
			if _, err := m.RunTask(latest); err != nil {
				global.NAV_LOG.Warn("scheduled sync task skipped", zap.String("task", latest.Guid), zap.Error(err))
			}
		})
		if err != nil {
			return fmt.Errorf("add schedule for task %s failed: %w", task.Guid, err)
		}
		m.mu.Lock()
		m.entries[task.Guid] = scheduleState{
			TaskGuid: task.Guid,
			TaskName: task.Name,
			CronExpr: task.CronExpr,
			EntryID:  entryID,
		}
		m.mu.Unlock()
	}
	return nil
}

func (m *Manager) ScheduleItems() []ScheduleItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := make([]ScheduleItem, 0, len(m.entries))
	for _, state := range m.entries {
		items = append(items, ScheduleItem{
			TaskGuid: state.TaskGuid,
			TaskName: state.TaskName,
			CronExpr: state.CronExpr,
			EntryID:  int(state.EntryID),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		return items[i].TaskGuid < items[j].TaskGuid
	})
	return items
}

func (m *Manager) RecoverStaleRuns() error {
	db := m.DB()
	if db == nil {
		return errors.New("database not initialized")
	}
	now := domains.NowMilli()
	if err := db.Model(&domains.SyncRun{}).
		Where("status IN ?", []string{domains.RunStatusPending, domains.RunStatusRunning}).
		Updates(map[string]any{
			"status":      domains.RunStatusFailed,
			"end_time":    now,
			"duration_ms": gorm.Expr("CASE WHEN start_time > 0 THEN ? - start_time ELSE 0 END", now),
			"last_error":  "server restarted before sync run finished",
			"update_time": now,
		}).Error; err != nil {
		return err
	}
	return db.Model(&domains.SyncTask{}).
		Where("last_run_status IN ?", []string{domains.RunStatusPending, domains.RunStatusRunning}).
		Updates(map[string]any{
			"last_run_status": domains.RunStatusFailed,
			"update_time":     now,
		}).Error
}

func (m *Manager) DB() *gorm.DB {
	if m.db != nil {
		return m.db
	}
	return global.NAV_DB
}

func (m *Manager) removeRunning(taskGuid string) {
	m.mu.Lock()
	delete(m.running, taskGuid)
	m.mu.Unlock()
}

func (m *Manager) markTaskRunStarted(db *gorm.DB, taskGuid string, runGuid string, now int64) error {
	return db.Model(&domains.SyncTask{}).Where("guid = ?", taskGuid).Updates(map[string]any{
		"last_run_guid":   runGuid,
		"last_run_status": domains.RunStatusRunning,
		"update_time":     now,
	}).Error
}

func (m *Manager) maxWorkers() int {
	if global.NAV_VIPER == nil {
		return 4
	}
	value := global.NAV_VIPER.GetInt("sync.max-workers")
	if value <= 0 {
		return 4
	}
	return value
}

func (m *Manager) runTimeout() time.Duration {
	if global.NAV_VIPER == nil {
		return 2 * time.Hour
	}
	value := global.NAV_VIPER.GetDuration("sync.run-timeout")
	if value <= 0 {
		return 2 * time.Hour
	}
	return value
}

func newCron() *cron.Cron {
	return cron.New(cron.WithParser(cronParser()))
}

func cronParser() cron.Parser {
	return cron.NewParser(
		cron.SecondOptional |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor,
	)
}
