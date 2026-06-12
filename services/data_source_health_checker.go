package services

import (
	"sync"
	"time"

	"database-sync-go/domains"
	"database-sync-go/syncer/connector"

	"github.com/wfu-work/nav-common-go-lib/global"
	"go.uber.org/zap"
)

var dataSourceHealthChecker = struct {
	sync.Mutex
	stop chan struct{}
}{}

func StartDataSourceHealthChecker() error {
	dataSourceHealthChecker.Lock()
	defer dataSourceHealthChecker.Unlock()
	if dataSourceHealthChecker.stop != nil {
		return nil
	}
	dataSourceHealthChecker.stop = make(chan struct{})
	go runDataSourceHealthChecker(dataSourceHealthChecker.stop)
	return nil
}

func StopDataSourceHealthChecker() {
	dataSourceHealthChecker.Lock()
	stop := dataSourceHealthChecker.stop
	dataSourceHealthChecker.stop = nil
	dataSourceHealthChecker.Unlock()
	if stop != nil {
		close(stop)
	}
}

func runDataSourceHealthChecker(stop <-chan struct{}) {
	interval := dataSourceHealthCheckInterval()
	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			checkEnabledDataSourceConnections()
			timer.Reset(interval)
		case <-stop:
			return
		}
	}
}

func checkEnabledDataSourceConnections() {
	db := ServiceGroupApp.DataSourceService.DB()
	var items []domains.DataSource
	if err := db.Where("status = ?", int(domains.StatusEnabled)).Find(&items).Error; err != nil {
		global.NAV_LOG.Warn("list enabled datasource for health check failed", zap.Error(err))
		return
	}
	for _, item := range items {
		checkDataSourceConnection(item)
	}
}

func checkDataSourceConnection(source domains.DataSource) error {
	db := ServiceGroupApp.DataSourceService.DB()
	previousStatus := source.ConnectionStatus
	now := domains.NowMilli()
	_ = db.Model(&domains.DataSource{}).Where("guid = ?", source.Guid).Updates(map[string]any{
		"connection_status":     domains.DataSourceConnectionChecking,
		"connection_checked_at": now,
		"update_time":           now,
	}).Error
	broadcastDataSourceByGuid(source.Guid)

	err := testDataSourceConnection(source)
	status := domains.DataSourceConnectionConnected
	message := ""
	if err != nil {
		status = domains.DataSourceConnectionFailed
		message = err.Error()
	}
	checkedAt := domains.NowMilli()
	if updateErr := db.Model(&domains.DataSource{}).Where("guid = ?", source.Guid).Updates(map[string]any{
		"connection_status":     status,
		"connection_checked_at": checkedAt,
		"connection_error":      message,
		"update_time":           checkedAt,
	}).Error; updateErr != nil {
		global.NAV_LOG.Warn("update datasource connection status failed", zap.String("datasource", source.Guid), zap.Error(updateErr))
	} else {
		broadcastDataSourceByGuid(source.Guid)
	}

	source.ConnectionStatus = status
	source.ConnectionCheckedAt = checkedAt
	source.ConnectionError = message
	if err != nil {
		if previousStatus != domains.DataSourceConnectionFailed {
			ServiceGroupApp.EventNotificationService.NotifyDataSourceConnectionFailed(source, err)
		}
		return err
	}
	if previousStatus == domains.DataSourceConnectionFailed {
		ServiceGroupApp.EventNotificationService.NotifyDataSourceConnectionRecovered(source)
	}
	return nil
}

func testDataSourceConnection(source domains.DataSource) error {
	conn, err := connector.New(source)
	if err != nil {
		return err
	}
	defer conn.Close()
	return conn.Test()
}

func dataSourceHealthCheckInterval() time.Duration {
	if global.NAV_VIPER != nil {
		if value := global.NAV_VIPER.GetDuration("datasource.health-check-interval"); value > 0 {
			return value
		}
		if value := global.NAV_VIPER.GetDuration("dataSource.health-check-interval"); value > 0 {
			return value
		}
	}
	return 5 * time.Minute
}
