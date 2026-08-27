package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"turbocache/internal/cache/value"
	"turbocache/internal/cluster/discovery"
	"turbocache/internal/utils/logger"
)

// SyncManager 同步管理器
type SyncManager struct {
	mu         sync.RWMutex
	config     *SyncConfig
	discovery  discovery.DiscoveryService
	replicator *Replicator
	logger     logger.Logger

	// 同步状态
	lastSyncTime   time.Time
	syncInProgress bool

	// 生命周期
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// SyncConfig 同步配置
type SyncConfig struct {
	Enabled       bool          `yaml:"enabled"`
	SyncInterval  time.Duration `yaml:"sync_interval"`
	SyncTimeout   time.Duration `yaml:"sync_timeout"`
	BatchSize     int           `yaml:"batch_size"`
	RetryAttempts int           `yaml:"retry_attempts"`
}

// DefaultSyncConfig 默认同步配置
func DefaultSyncConfig() *SyncConfig {
	return &SyncConfig{
		Enabled:       true,
		SyncInterval:  30 * time.Second,
		SyncTimeout:   10 * time.Second,
		BatchSize:     100,
		RetryAttempts: 3,
	}
}

// NewSyncManager 创建同步管理器
func NewSyncManager(config *SyncConfig, discovery discovery.DiscoveryService, replicator *Replicator) *SyncManager {
	if config == nil {
		config = DefaultSyncConfig()
	}

	return &SyncManager{
		config:     config,
		discovery:  discovery,
		replicator: replicator,
		logger:     logger.WithField("component", "sync_manager"),
		stopCh:     make(chan struct{}),
	}
}

// Start 启动同步管理器
func (sm *SyncManager) Start(ctx context.Context) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.running {
		return fmt.Errorf("sync manager already running")
	}

	if !sm.config.Enabled {
		sm.logger.Info("Sync manager is disabled")
		return nil
	}

	sm.running = true
	sm.logger.Info("Starting sync manager")

	// 启动同步循环
	sm.wg.Add(1)
	go sm.syncLoop()

	return nil
}

// Stop 停止同步管理器
func (sm *SyncManager) Stop() error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.running {
		return nil
	}

	sm.logger.Info("Stopping sync manager")
	sm.running = false
	close(sm.stopCh)

	// 等待同步循环完成
	sm.wg.Wait()

	sm.logger.Info("Sync manager stopped")
	return nil
}

// syncLoop 同步循环
func (sm *SyncManager) syncLoop() {
	defer sm.wg.Done()

	ticker := time.NewTicker(sm.config.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sm.performSync(); err != nil {
				sm.logger.Errorf("Sync failed: %v", err)
			}
		case <-sm.stopCh:
			return
		}
	}
}

// performSync 执行同步
func (sm *SyncManager) performSync() error {
	sm.mu.Lock()
	if sm.syncInProgress {
		sm.mu.Unlock()
		sm.logger.Warn("Sync already in progress, skipping")
		return nil
	}
	sm.syncInProgress = true
	sm.mu.Unlock()

	defer func() {
		sm.mu.Lock()
		sm.syncInProgress = false
		sm.lastSyncTime = time.Now()
		sm.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), sm.config.SyncTimeout)
	defer cancel()

	// 获取集群节点
	nodes, err := sm.discovery.GetNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get nodes: %w", err)
	}

	if len(nodes) == 0 {
		return nil // 没有其他节点
	}

	sm.logger.Infof("Starting sync with %d nodes", len(nodes))

	// 这里实现实际的数据同步逻辑
	// 简化版本：只记录日志
	for _, node := range nodes {
		if node.Status == "healthy" {
			sm.logger.Debugf("Would sync with node: %s", node.Address)
		}
	}

	return nil
}

// SyncEntry 同步单个条目
func (sm *SyncManager) SyncEntry(group, key string, entry *value.Entry, ttl time.Duration) error {
	if !sm.config.Enabled {
		return nil
	}

	// 复制到其他节点
	return sm.replicator.ReplicateSet(group, key, entry, ttl)
}

// GetStats 获取同步统计信息
func (sm *SyncManager) GetStats() *SyncStats {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return &SyncStats{
		Enabled:        sm.config.Enabled,
		Running:        sm.running,
		SyncInProgress: sm.syncInProgress,
		LastSyncTime:   sm.lastSyncTime,
	}
}

// SyncStats 同步统计信息
type SyncStats struct {
	Enabled        bool      `json:"enabled"`
	Running        bool      `json:"running"`
	SyncInProgress bool      `json:"sync_in_progress"`
	LastSyncTime   time.Time `json:"last_sync_time"`
}
