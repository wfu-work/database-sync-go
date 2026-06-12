package services

import (
	"os"
	"path/filepath"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/wfu-work/nav-common-go-lib/global"
	commonServices "github.com/wfu-work/nav-common-go-lib/services"
	commonUtils "github.com/wfu-work/nav-common-go-lib/utils"
)

var serviceStartedAt = time.Now()

type SystemMonitorService struct{}

type ServiceRuntimeInfo struct {
	Name            string `json:"name"`
	Status          string `json:"status"`
	PID             int    `json:"pid"`
	StartedAt       int64  `json:"startedAt"`
	UptimeSeconds   int64  `json:"uptimeSeconds"`
	WorkingDir      string `json:"workingDir"`
	Executable      string `json:"executable"`
	GoVersion       string `json:"goVersion"`
	GoOS            string `json:"goos"`
	Compiler        string `json:"compiler"`
	NumCPU          int    `json:"numCpu"`
	NumGoroutine    int    `json:"numGoroutine"`
	AllocBytes      uint64 `json:"allocBytes"`
	SysBytes        uint64 `json:"sysBytes"`
	HeapAllocBytes  uint64 `json:"heapAllocBytes"`
	HeapInuseBytes  uint64 `json:"heapInuseBytes"`
	LastGCPauseNano uint64 `json:"lastGcPauseNano"`
}

type ServerDiskInfo struct {
	MountPoint  string `json:"mountPoint"`
	UsedMB      int    `json:"usedMb"`
	UsedGB      int    `json:"usedGb"`
	TotalMB     int    `json:"totalMb"`
	TotalGB     int    `json:"totalGb"`
	UsedPercent int    `json:"usedPercent"`
}

type SystemMonitorInfo struct {
	Service   ServiceRuntimeInfo `json:"service"`
	OS        commonUtils.Os     `json:"os"`
	CPU       commonUtils.Cpu    `json:"cpu"`
	RAM       commonUtils.Ram    `json:"ram"`
	Disk      []ServerDiskInfo   `json:"disk"`
	Warnings  []string           `json:"warnings"`
	CheckedAt int64              `json:"checkedAt"`
}

func (s SystemMonitorService) Runtime() (*SystemMonitorInfo, error) {
	result := &SystemMonitorInfo{
		Service:   serviceRuntimeInfo(),
		CheckedAt: time.Now().UnixMilli(),
		Warnings:  make([]string, 0),
	}
	server, err := commonServices.OsServiceApp.GetServerInfo()
	if err != nil {
		result.Warnings = append(result.Warnings, err.Error())
	} else if server != nil {
		result.OS = server.Os
		result.CPU = server.Cpu
		result.RAM = server.Ram
		result.Disk = commonDisks(server.Disk)
	}
	if len(result.Disk) == 0 {
		diskInfo, err := fallbackDisk()
		if err != nil {
			result.Warnings = append(result.Warnings, err.Error())
		} else {
			result.Disk = []ServerDiskInfo{diskInfo}
		}
	}
	return result, nil
}

func serviceRuntimeInfo() ServiceRuntimeInfo {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	workingDir, _ := os.Getwd()
	executable, _ := os.Executable()
	return ServiceRuntimeInfo{
		Name:            serviceName(),
		Status:          "running",
		PID:             os.Getpid(),
		StartedAt:       serviceStartedAt.UnixMilli(),
		UptimeSeconds:   int64(time.Since(serviceStartedAt).Seconds()),
		WorkingDir:      workingDir,
		Executable:      executable,
		GoVersion:       runtime.Version(),
		GoOS:            runtime.GOOS,
		Compiler:        runtime.Compiler,
		NumCPU:          runtime.NumCPU(),
		NumGoroutine:    runtime.NumGoroutine(),
		AllocBytes:      memory.Alloc,
		SysBytes:        memory.Sys,
		HeapAllocBytes:  memory.HeapAlloc,
		HeapInuseBytes:  memory.HeapInuse,
		LastGCPauseNano: memory.PauseNs[(memory.NumGC+255)%256],
	}
}

func serviceName() string {
	if global.NAV_CONFIG.System.AppName != "" {
		return global.NAV_CONFIG.System.AppName
	}
	return "datasync"
}

func commonDisks(items []commonUtils.Disk) []ServerDiskInfo {
	out := make([]ServerDiskInfo, 0, len(items))
	for _, item := range items {
		out = append(out, ServerDiskInfo{
			MountPoint:  item.MountPoint,
			UsedMB:      item.UsedMB,
			UsedGB:      item.UsedGB,
			TotalMB:     item.TotalMB,
			TotalGB:     item.TotalGB,
			UsedPercent: item.UsedPercent,
		})
	}
	return out
}

func fallbackDisk() (ServerDiskInfo, error) {
	workingDir, err := os.Getwd()
	if err != nil {
		return ServerDiskInfo{}, err
	}
	usage, err := disk.Usage(workingDir)
	if err != nil {
		usage, err = disk.Usage(filepath.VolumeName(workingDir) + string(os.PathSeparator))
	}
	if err != nil {
		return ServerDiskInfo{}, err
	}
	return ServerDiskInfo{
		MountPoint:  usage.Path,
		UsedMB:      int(usage.Used) / commonUtils.MB,
		UsedGB:      int(usage.Used) / commonUtils.GB,
		TotalMB:     int(usage.Total) / commonUtils.MB,
		TotalGB:     int(usage.Total) / commonUtils.GB,
		UsedPercent: int(usage.UsedPercent),
	}, nil
}
