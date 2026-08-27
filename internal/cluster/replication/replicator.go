package replication

import (
	"context"
	"fmt"
	"sync"
	"time"

	"turbocache/internal/cache/value"
	"turbocache/internal/cluster/discovery"
	"turbocache/internal/transport/grpc"
	"turbocache/internal/utils/logger"
)

// Replicator 数据复制器
type Replicator struct {
	mu            sync.RWMutex
	config        *Config
	discovery     discovery.DiscoveryService
	grpcClient    *grpc.Client
	replicationCh chan *ReplicationTask
	logger        logger.Logger

	// 状态管理
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// Config 复制配置
type Config struct {
	Factor        int           `yaml:"factor"`         // 复制因子
	SyncMode      string        `yaml:"sync_mode"`      // sync/async
	SyncTimeout   time.Duration `yaml:"sync_timeout"`   // 同步超时
	RetryInterval time.Duration `yaml:"retry_interval"` // 重试间隔
	MaxRetries    int           `yaml:"max_retries"`    // 最大重试次数
	BufferSize    int           `yaml:"buffer_size"`    // 缓冲区大小
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Factor:        2,
		SyncMode:      "async",
		SyncTimeout:   5 * time.Second,
		RetryInterval: time.Second,
		MaxRetries:    3,
		BufferSize:    1000,
	}
}

// ReplicationTask 复制任务
type ReplicationTask struct {
	Operation string        // set/delete/clear
	Group     string        // 缓存组
	Key       string        // 键
	Entry     *value.Entry  // 条目（用于set操作）
	TTL       time.Duration // 过期时间
	Replicas  []string      // 目标副本节点
	Callback  func(error)   // 回调函数
	Timestamp time.Time     // 任务时间戳
}

// NewReplicator 创建复制器
func NewReplicator(config *Config, discovery discovery.DiscoveryService, grpcClient *grpc.Client) *Replicator {
	if config == nil {
		config = DefaultConfig()
	}

	return &Replicator{
		config:        config,
		discovery:     discovery,
		grpcClient:    grpcClient,
		replicationCh: make(chan *ReplicationTask, config.BufferSize),
		logger:        logger.WithField("component", "replicator"),
		stopCh:        make(chan struct{}),
	}
}

// Start 启动复制器
func (r *Replicator) Start(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return fmt.Errorf("replicator already running")
	}

	r.running = true
	r.logger.Info("Starting replicator")

	// 启动复制工作协程
	for i := 0; i < 3; i++ { // 启动3个工作协程
		r.wg.Add(1)
		go r.replicationWorker()
	}

	return nil
}

// Stop 停止复制器
func (r *Replicator) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	r.logger.Info("Stopping replicator")
	r.running = false
	close(r.stopCh)

	// 等待工作协程完成
	r.wg.Wait()

	r.logger.Info("Replicator stopped")
	return nil
}

// ReplicateSet 复制设置操作
func (r *Replicator) ReplicateSet(group, key string, entry *value.Entry, ttl time.Duration) error {
	replicas, err := r.selectReplicas(key)
	if err != nil {
		return fmt.Errorf("failed to select replicas: %w", err)
	}

	if len(replicas) == 0 {
		return nil // 没有副本节点，跳过复制
	}

	task := &ReplicationTask{
		Operation: "set",
		Group:     group,
		Key:       key,
		Entry:     entry,
		TTL:       ttl,
		Replicas:  replicas,
		Timestamp: time.Now(),
	}

	return r.submitTask(task)
}

// ReplicateDelete 复制删除操作
func (r *Replicator) ReplicateDelete(group, key string) error {
	replicas, err := r.selectReplicas(key)
	if err != nil {
		return fmt.Errorf("failed to select replicas: %w", err)
	}

	if len(replicas) == 0 {
		return nil
	}

	task := &ReplicationTask{
		Operation: "delete",
		Group:     group,
		Key:       key,
		Replicas:  replicas,
		Timestamp: time.Now(),
	}

	return r.submitTask(task)
}

// ReplicateClear 复制清空操作
func (r *Replicator) ReplicateClear(group string) error {
	// 获取所有健康节点
	nodes, err := r.discovery.GetNodes(context.Background())
	if err != nil {
		return fmt.Errorf("failed to get nodes: %w", err)
	}

	var replicas []string
	for _, node := range nodes {
		if node.Status == "healthy" {
			replicas = append(replicas, node.Address)
		}
	}

	if len(replicas) == 0 {
		return nil
	}

	task := &ReplicationTask{
		Operation: "clear",
		Group:     group,
		Replicas:  replicas,
		Timestamp: time.Now(),
	}

	return r.submitTask(task)
}

// submitTask 提交复制任务
func (r *Replicator) submitTask(task *ReplicationTask) error {
	if !r.running {
		return fmt.Errorf("replicator is not running")
	}

	if r.config.SyncMode == "sync" {
		// 同步模式：直接执行
		return r.executeTask(task)
	}

	// 异步模式：提交到队列
	select {
	case r.replicationCh <- task:
		return nil
	default:
		r.logger.Warn("Replication queue is full, dropping task")
		return fmt.Errorf("replication queue is full")
	}
}

// selectReplicas 选择副本节点
func (r *Replicator) selectReplicas(key string) ([]string, error) {
	nodes, err := r.discovery.GetNodes(context.Background())
	if err != nil {
		return nil, err
	}

	// 过滤健康节点
	var healthyNodes []*discovery.NodeInfo
	for _, node := range nodes {
		if node.Status == "healthy" {
			healthyNodes = append(healthyNodes, node)
		}
	}

	if len(healthyNodes) <= 1 {
		return nil, nil // 没有足够的节点进行复制
	}

	// 简单的副本选择策略：基于键的哈希选择相邻节点
	var replicas []string
	replicaCount := r.config.Factor - 1 // 减去主节点
	if replicaCount > len(healthyNodes)-1 {
		replicaCount = len(healthyNodes) - 1
	}

	// 这里简化实现，实际应该使用一致性哈希
	for i := 0; i < replicaCount && i < len(healthyNodes); i++ {
		replicas = append(replicas, healthyNodes[i].Address)
	}

	return replicas, nil
}

// replicationWorker 复制工作协程
func (r *Replicator) replicationWorker() {
	defer r.wg.Done()

	for {
		select {
		case task := <-r.replicationCh:
			if task != nil {
				if err := r.executeTask(task); err != nil {
					r.logger.Errorf("Failed to execute replication task: %v", err)
					if task.Callback != nil {
						task.Callback(err)
					}
				} else if task.Callback != nil {
					task.Callback(nil)
				}
			}
		case <-r.stopCh:
			return
		}
	}
}

// executeTask 执行复制任务
func (r *Replicator) executeTask(task *ReplicationTask) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(task.Replicas))

	for _, replica := range task.Replicas {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()

			var err error
			ctx, cancel := context.WithTimeout(context.Background(), r.config.SyncTimeout)
			defer cancel()

			switch task.Operation {
			case "set":
				err = r.replicateSetToNode(ctx, addr, task)
			case "delete":
				err = r.replicateDeleteToNode(ctx, addr, task)
			case "clear":
				err = r.replicateClearToNode(ctx, addr, task)
			default:
				err = fmt.Errorf("unknown operation: %s", task.Operation)
			}

			if err != nil {
				r.logger.Warnf("Failed to replicate %s to %s: %v", task.Operation, addr, err)
				errCh <- err
			}
		}(replica)
	}

	wg.Wait()
	close(errCh)

	// 收集错误
	var errors []error
	for err := range errCh {
		errors = append(errors, err)
	}

	if len(errors) > 0 {
		return fmt.Errorf("replication failed for %d/%d replicas", len(errors), len(task.Replicas))
	}

	return nil
}

// replicateSetToNode 复制设置操作到节点
func (r *Replicator) replicateSetToNode(ctx context.Context, address string, task *ReplicationTask) error {
	return r.retryOperation(func() error {
		_, err := r.grpcClient.Set(ctx, address, task.Key, task.Group, task.Entry.Value.ByteSlice(), task.TTL)
		return err
	})
}

// replicateDeleteToNode 复制删除操作到节点
func (r *Replicator) replicateDeleteToNode(ctx context.Context, address string, task *ReplicationTask) error {
	return r.retryOperation(func() error {
		_, err := r.grpcClient.Delete(ctx, address, task.Key, task.Group)
		return err
	})
}

// replicateClearToNode 复制清空操作到节点
func (r *Replicator) replicateClearToNode(ctx context.Context, address string, task *ReplicationTask) error {
	return r.retryOperation(func() error {
		// 注意：这里需要实现Clear的gRPC调用
		// 当前grpc.Client可能没有Clear方法，需要添加
		return fmt.Errorf("clear operation not implemented")
	})
}

// retryOperation 重试操作
func (r *Replicator) retryOperation(operation func() error) error {
	var lastErr error

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(r.config.RetryInterval)
		}

		if err := operation(); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}

	return fmt.Errorf("operation failed after %d attempts: %w", r.config.MaxRetries+1, lastErr)
}

// GetStats 获取复制统计信息
func (r *Replicator) GetStats() *Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return &Stats{
		Running:     r.running,
		QueueLength: len(r.replicationCh),
		QueueCap:    cap(r.replicationCh),
	}
}

// Stats 复制统计信息
type Stats struct {
	Running     bool `json:"running"`
	QueueLength int  `json:"queue_length"`
	QueueCap    int  `json:"queue_cap"`
}
