package inits

import (
	"database-sync-go/domains"
	"database-sync-go/routers"
	"database-sync-go/services"
	"database-sync-go/syncer/manager"
	"database-sync-go/utils"
	"database-sync-go/webs"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/wfu-work/nav-common-go-lib/global"
	commonInits "github.com/wfu-work/nav-common-go-lib/inits"
	"go.uber.org/zap"
)

//go:embed config.yaml
var defaultConfig []byte

func Init() {
	if err := utils.NewDefaultConfigManager(defaultConfig).Ensure(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "prepare config failed: %v\n", err)
		os.Exit(1)
	}
	sysInit := commonInits.SysInit{}
	sysInit.OnTableInit(registerTables)
	sysInit.OnRouterInit(func(publicGroup *gin.RouterGroup, privateGroup *gin.RouterGroup) {
		routers.RouterGroupApp.InitRouters(publicGroup, privateGroup)
	})
	sysInit.OnOtherInit(startBackgroundServices)
	sysInit.OnShutInit(stopBackgroundServices)
	sysInit.OnWebInit(func(router *gin.Engine) {
		_ = webs.InitStatic(router)
	})
	sysInit.Init()
}

func registerTables() {
	if err := ensureDataDirs(); err != nil {
		global.NAV_LOG.Error("ensure datasync data dir failed", zap.Error(err))
	}
	err := global.NAV_DB.AutoMigrate(
		domains.DataSource{},
		domains.SyncTask{},
		domains.SyncTemplate{},
		domains.SyncRun{},
		domains.SyncError{},
		domains.DatabaseBackup{},
		domains.DatabaseRestore{},
		domains.EventNotification{},
		domains.SystemSetting{},
	)
	if err != nil {
		global.NAV_LOG.Error("register datasync business table failed", zap.Error(err))
		return
	}
	if err := services.ServiceGroupApp.DataSourceService.MigratePlaintextPasswords(); err != nil {
		global.NAV_LOG.Warn("migrate datasync datasource passwords failed", zap.Error(err))
	}
	global.NAV_LOG.Info("register datasync business table success")
}

func startBackgroundServices() {
	if err := manager.DefaultManager.RecoverStaleRuns(); err != nil {
		global.NAV_LOG.Warn("recover datasync stale runs failed", zap.Error(err))
	}
	if err := services.ServiceGroupApp.DatabaseBackupService.RecoverStaleBackups(); err != nil {
		global.NAV_LOG.Warn("recover datasync stale backups failed", zap.Error(err))
	}
	if err := services.ServiceGroupApp.DatabaseRestoreService.RecoverStaleRestores(); err != nil {
		global.NAV_LOG.Warn("recover datasync stale restores failed", zap.Error(err))
	}
	if err := manager.DefaultManager.StartScheduler(); err != nil {
		global.NAV_LOG.Warn("start datasync scheduler failed", zap.Error(err))
	}
	if err := services.StartDataSourceHealthChecker(); err != nil {
		global.NAV_LOG.Warn("start datasync datasource health checker failed", zap.Error(err))
	}
}

func stopBackgroundServices() {
	services.StopDataSourceHealthChecker()
	manager.DefaultManager.StopScheduler()
	manager.DefaultManager.StopAll()
}

func ensureDataDirs() error {
	ossDir := "./data/oss"
	if global.NAV_VIPER != nil {
		if value := strings.TrimSpace(global.NAV_VIPER.GetString("local.oss-path")); value != "" {
			ossDir = value
		}
	}
	for _, dir := range []string{"./data", ossDir} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	cachePath := "./data/cache.json"
	if global.NAV_VIPER != nil {
		if value := global.NAV_VIPER.GetString("local.cache-path"); value != "" {
			cachePath = value
		}
	}
	if err := os.MkdirAll(filepath.Dir(cachePath), 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(cachePath); os.IsNotExist(err) {
		if err := os.WriteFile(cachePath, []byte("{}"), 0o644); err != nil {
			return err
		}
	}
	return nil
}
