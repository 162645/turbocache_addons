package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health/grpc_health_v1"

	"turbocache/internal/utils/logger"
)

// HealthChecker 健康检查器
type HealthChecker struct {
	mu     sync.RWMutex
	nodes  map[string]*NodeHealth
	config *HealthCheckConfig
	logger logger.Logger

	// 健康状态回调
	onHealthChange func(nodeID string, healthy bool)

	// 生命周期管理
	ticker *time.Ticker
	stopCh chan struct{}
	closed bool
}

// HealthCheckConfig 健康检查配置
type HealthCheckConfig struct {
	Interval         time.Duration `yaml:"interval" json:"interval"`
	Timeout          time.Duration `yaml:"timeout" json:"timeout"`
	FailureThreshold int           `yaml:"failure_threshold" json:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold" json:"success_threshold"`
	MaxConcurrency   int           `yaml:"max_concurrency" json:"max_concurrency"`
}

// NodeHealth 节点健康状态
type NodeHealth struct {
	NodeID       string        `json:"node_id"`
	Address      string        `json:"address"`
	Healthy      bool          `json:"healthy"`
	LastCheck    time.Time     `json:"last_check"`
	LastSuccess  time.Time     `json:"last_success"`
	LastFailure  time.Time     `json:"last_failure"`
	FailureCount int           `json:"failure_count"`
	SuccessCount int           `json:"success_count"`
	LastError    string        `json:"last_error"`
	ResponseTime time.Duration `json:"response_time"`

	// 连接信息
	conn   *grpc.ClientConn
	client grpc_health_v1.HealthClient
	// A node can be checked immediately on AddNode and concurrently by the
	// periodic sweep. Serialize those checks so connection and counters agree.
	checkMu *sync.Mutex
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(config *HealthCheckConfig) *HealthChecker {
	if config == nil {
		config = &HealthCheckConfig{
			Interval:         10 * time.Second,
			Timeout:          5 * time.Second,
			FailureThreshold: 3,
			SuccessThreshold: 2,
			MaxConcurrency:   10,
		}
	}

	hc := &HealthChecker{
		nodes:  make(map[string]*NodeHealth),
		config: config,
		logger: logger.WithField("component", "health_checker"),
		stopCh: make(chan struct{}),
	}

	return hc
}

// Start 启动健康检查
func (hc *HealthChecker) Start() {
	if hc.config.Interval <= 0 {
		hc.logger.Warn("Health check disabled (interval <= 0)")
		return
	}

	hc.ticker = time.NewTicker(hc.config.Interval)

	go func() {
		defer hc.ticker.Stop()

		hc.logger.Infof("Health checker started with interval: %v", hc.config.Interval)

		for {
			select {
			case <-hc.ticker.C:
				hc.checkAllNodes()
			case <-hc.stopCh:
				return
			}
		}
	}()
}

// Stop 停止健康检查
func (hc *HealthChecker) Stop() {
	hc.mu.Lock()
	if hc.closed {
		hc.mu.Unlock()
		return
	}

	hc.closed = true
	close(hc.stopCh)
	nodes := make([]*NodeHealth, 0, len(hc.nodes))
	for _, node := range hc.nodes {
		nodes = append(nodes, node)
	}
	hc.mu.Unlock()

	// 关闭所有连接
	for _, node := range nodes {
		if node.checkMu != nil {
			node.checkMu.Lock()
		}
		if node.conn != nil {
			node.conn.Close()
		}
		if node.checkMu != nil {
			node.checkMu.Unlock()
		}
	}

	hc.logger.Info("Health checker stopped")
}

// AddNode 添加需要检查的节点
func (hc *HealthChecker) AddNode(nodeID, address string) error {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	if hc.closed {
		return fmt.Errorf("health checker is closed")
	}

	// 检查是否已存在
	if _, exists := hc.nodes[nodeID]; exists {
		return nil
	}

	// 创建gRPC连接
	ctx, cancel := context.WithTimeout(context.Background(), hc.config.Timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		hc.logger.Warnf("Failed to connect to node %s (%s): %v", nodeID, address, err)
		// 即使连接失败也要添加节点，后续会重试
		conn = nil
	}

	var client grpc_health_v1.HealthClient
	if conn != nil {
		client = grpc_health_v1.NewHealthClient(conn)
	}

	nodeHealth := &NodeHealth{
		NodeID:  nodeID,
		Address: address,
		Healthy: false, // 初始状态为不健康，等待第一次检查
		conn:    conn,
		client:  client,
		checkMu: &sync.Mutex{},
	}

	hc.nodes[nodeID] = nodeHealth

	hc.logger.Debugf("Added node to health check: %s (%s)", nodeID, address)

	// 立即进行一次健康检查
	go hc.checkNode(nodeHealth)

	return nil
}

// RemoveNode 移除节点
func (hc *HealthChecker) RemoveNode(nodeID string) {
	hc.mu.Lock()
	node, exists := hc.nodes[nodeID]
	if !exists {
		hc.mu.Unlock()
		return
	}
	delete(hc.nodes, nodeID)
	hc.mu.Unlock()

	if node.checkMu != nil {
		node.checkMu.Lock()
		defer node.checkMu.Unlock()
	}
	if node.conn != nil {
		node.conn.Close()
	}

	hc.logger.Debugf("Removed node from health check: %s", nodeID)
}

// GetNodeHealth 获取节点健康状态
func (hc *HealthChecker) GetNodeHealth(nodeID string) (*NodeHealth, bool) {
	hc.mu.RLock()
	node, exists := hc.nodes[nodeID]
	hc.mu.RUnlock()
	if !exists {
		return nil, false
	}

	// 返回副本
	if node.checkMu != nil {
		node.checkMu.Lock()
		defer node.checkMu.Unlock()
	}
	nodeCopy := *node
	nodeCopy.conn = nil   // 不暴露连接
	nodeCopy.client = nil // 不暴露客户端
	nodeCopy.checkMu = nil

	return &nodeCopy, true
}

// GetAllHealth 获取所有节点健康状态
func (hc *HealthChecker) GetAllHealth() map[string]*NodeHealth {
	hc.mu.RLock()
	nodes := make(map[string]*NodeHealth, len(hc.nodes))
	for id, node := range hc.nodes {
		nodes[id] = node
	}
	hc.mu.RUnlock()

	result := make(map[string]*NodeHealth)
	for nodeID, node := range nodes {
		// 返回副本
		if node.checkMu != nil {
			node.checkMu.Lock()
		}
		nodeCopy := *node
		if node.checkMu != nil {
			node.checkMu.Unlock()
		}
		nodeCopy.conn = nil   // 不暴露连接
		nodeCopy.client = nil // 不暴露客户端
		nodeCopy.checkMu = nil
		result[nodeID] = &nodeCopy
	}

	return result
}

// GetHealthyNodes 获取健康节点列表
func (hc *HealthChecker) GetHealthyNodes() []string {
	hc.mu.RLock()
	nodes := make(map[string]*NodeHealth, len(hc.nodes))
	for id, node := range hc.nodes {
		nodes[id] = node
	}
	hc.mu.RUnlock()

	var healthyNodes []string
	for nodeID, node := range nodes {
		if node.checkMu != nil {
			node.checkMu.Lock()
		}
		if node.Healthy {
			healthyNodes = append(healthyNodes, nodeID)
		}
		if node.checkMu != nil {
			node.checkMu.Unlock()
		}
	}

	return healthyNodes
}

// SetHealthChangeCallback 设置健康状态变更回调
func (hc *HealthChecker) SetHealthChangeCallback(callback func(nodeID string, healthy bool)) {
	hc.mu.Lock()
	defer hc.mu.Unlock()

	hc.onHealthChange = callback
}

// GetStats 获取健康检查统计信息
func (hc *HealthChecker) GetStats() *HealthCheckStats {
	hc.mu.RLock()
	nodes := make([]*NodeHealth, 0, len(hc.nodes))
	for _, node := range hc.nodes {
		nodes = append(nodes, node)
	}
	hc.mu.RUnlock()

	stats := &HealthCheckStats{
		TotalNodes:     len(nodes),
		HealthyNodes:   0,
		UnhealthyNodes: 0,
		CheckInterval:  hc.config.Interval,
		CheckTimeout:   hc.config.Timeout,
	}

	for _, node := range nodes {
		if node.checkMu != nil {
			node.checkMu.Lock()
		}
		if node.Healthy {
			stats.HealthyNodes++
		} else {
			stats.UnhealthyNodes++
		}
		if node.checkMu != nil {
			node.checkMu.Unlock()
		}
	}

	return stats
}

// 私有方法

// checkAllNodes 检查所有节点
func (hc *HealthChecker) checkAllNodes() {
	hc.mu.RLock()
	nodes := make([]*NodeHealth, 0, len(hc.nodes))
	for _, node := range hc.nodes {
		nodes = append(nodes, node)
	}
	hc.mu.RUnlock()

	if len(nodes) == 0 {
		return
	}

	// 控制并发数量
	concurrency := hc.config.MaxConcurrency
	if concurrency <= 0 {
		concurrency = 10
	}
	if concurrency > len(nodes) {
		concurrency = len(nodes)
	}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(n *NodeHealth) {
			defer wg.Done()

			sem <- struct{}{}        // 获取信号量
			defer func() { <-sem }() // 释放信号量

			hc.checkNode(n)
		}(node)
	}

	wg.Wait()
}

// checkNode 检查单个节点
func (hc *HealthChecker) checkNode(node *NodeHealth) {
	if node.checkMu != nil {
		node.checkMu.Lock()
		defer node.checkMu.Unlock()
	}
	start := time.Now()

	// 如果连接不存在或已关闭，尝试重新连接
	if node.conn == nil || node.conn.GetState().String() == "SHUTDOWN" {
		if err := hc.reconnectNode(node); err != nil {
			hc.handleCheckResult(node, false, err, time.Since(start))
			return
		}
	}

	// 执行健康检查
	ctx, cancel := context.WithTimeout(context.Background(), hc.config.Timeout)
	defer cancel()

	// NodeServer registers the aggregate (empty-name) status with the standard
	// gRPC health service. Asking for an unregistered symbolic service makes a
	// healthy peer return NotFound and eventually removes it from the hash ring.
	req := &grpc_health_v1.HealthCheckRequest{Service: ""}

	_, err := node.client.Check(ctx, req)
	responseTime := time.Since(start)

	// 处理检查结果
	hc.handleCheckResult(node, err == nil, err, responseTime)
}

// reconnectNode 重新连接节点
func (hc *HealthChecker) reconnectNode(node *NodeHealth) error {
	if node.conn != nil {
		node.conn.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), hc.config.Timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, node.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("failed to reconnect: %v", err)
	}

	node.conn = conn
	node.client = grpc_health_v1.NewHealthClient(conn)

	hc.logger.Debugf("Reconnected to node: %s (%s)", node.NodeID, node.Address)
	return nil
}

// handleCheckResult 处理检查结果
func (hc *HealthChecker) handleCheckResult(node *NodeHealth, success bool, err error, responseTime time.Duration) {
	node.LastCheck = time.Now()
	node.ResponseTime = responseTime

	previousHealthy := node.Healthy

	if success {
		node.LastSuccess = node.LastCheck
		node.SuccessCount++
		node.FailureCount = 0 // 重置失败计数
		node.LastError = ""

		// 达到成功阈值才标记为健康
		if node.SuccessCount >= hc.config.SuccessThreshold {
			node.Healthy = true
		}

		hc.logger.Debugf("Health check success for node %s: %v", node.NodeID, responseTime)

	} else {
		node.LastFailure = node.LastCheck
		node.FailureCount++
		node.SuccessCount = 0 // 重置成功计数
		if err != nil {
			node.LastError = err.Error()
		}

		// 达到失败阈值才标记为不健康
		if node.FailureCount >= hc.config.FailureThreshold {
			node.Healthy = false
		}

		hc.logger.Warnf("Health check failed for node %s: %v (failures: %d)",
			node.NodeID, err, node.FailureCount)
	}

	// 如果健康状态发生变化，触发回调
	hc.mu.RLock()
	callback := hc.onHealthChange
	hc.mu.RUnlock()
	if node.Healthy != previousHealthy && callback != nil {
		go callback(node.NodeID, node.Healthy)

		if node.Healthy {
			hc.logger.Infof("Node %s is now healthy", node.NodeID)
		} else {
			hc.logger.Warnf("Node %s is now unhealthy", node.NodeID)
		}
	}
}

// HealthCheckStats 健康检查统计信息
type HealthCheckStats struct {
	TotalNodes     int           `json:"total_nodes"`
	HealthyNodes   int           `json:"healthy_nodes"`
	UnhealthyNodes int           `json:"unhealthy_nodes"`
	CheckInterval  time.Duration `json:"check_interval"`
	CheckTimeout   time.Duration `json:"check_timeout"`
}

// HealthAwareDiscovery 健康感知的服务发现包装器
type HealthAwareDiscovery struct {
	discovery     DiscoveryService
	healthChecker *HealthChecker
	logger        logger.Logger
}

// NewHealthAwareDiscovery 创建健康感知的服务发现
func NewHealthAwareDiscovery(discovery DiscoveryService, healthConfig *HealthCheckConfig) *HealthAwareDiscovery {
	hc := NewHealthChecker(healthConfig)

	had := &HealthAwareDiscovery{
		discovery:     discovery,
		healthChecker: hc,
		logger:        logger.WithField("component", "health_aware_discovery"),
	}

	// 设置健康状态变更回调
	hc.SetHealthChangeCallback(func(nodeID string, healthy bool) {
		// 这里可以添加额外的处理逻辑
		had.logger.Infof("Node %s health changed to: %v", nodeID, healthy)
	})

	// 启动健康检查
	hc.Start()

	return had
}

// Register 注册节点并开始健康检查
func (had *HealthAwareDiscovery) Register(ctx context.Context, node *NodeInfo) error {
	err := had.discovery.Register(ctx, node)
	if err != nil {
		return err
	}

	// 添加到健康检查
	return had.healthChecker.AddNode(node.ID, node.Address)
}

// Unregister 注销节点并停止健康检查
func (had *HealthAwareDiscovery) Unregister(ctx context.Context, nodeID string) error {
	err := had.discovery.Unregister(ctx, nodeID)
	if err != nil {
		return err
	}

	// 从健康检查中移除
	had.healthChecker.RemoveNode(nodeID)
	return nil
}

// GetNodes 获取健康的节点
func (had *HealthAwareDiscovery) GetNodes(ctx context.Context) (map[string]*NodeInfo, error) {
	// 先获取所有节点
	allNodes, err := had.discovery.GetNodes(ctx)
	if err != nil {
		return nil, err
	}

	// 过滤出健康的节点
	healthyNodeIDs := had.healthChecker.GetHealthyNodes()
	healthyNodeIDMap := make(map[string]bool)
	for _, nodeID := range healthyNodeIDs {
		healthyNodeIDMap[nodeID] = true
	}

	result := make(map[string]*NodeInfo)
	for nodeID, node := range allNodes {
		if healthyNodeIDMap[nodeID] {
			result[nodeID] = node
		}
	}

	return result, nil
}

// Watch 监听节点变更
func (had *HealthAwareDiscovery) Watch(ctx context.Context) (<-chan *NodeEvent, error) {
	eventCh, err := had.discovery.Watch(ctx)
	if err != nil {
		return nil, err
	}

	// 监听节点变更，自动添加/移除健康检查
	go func() {
		for event := range eventCh {
			switch event.Type {
			case EventNodeAdded:
				had.healthChecker.AddNode(event.Node.ID, event.Node.Address)
			case EventNodeRemoved:
				had.healthChecker.RemoveNode(event.Node.ID)
			}
		}
	}()

	return eventCh, nil
}

// UpdateNode 更新节点信息
func (had *HealthAwareDiscovery) UpdateNode(ctx context.Context, node *NodeInfo) error {
	return had.discovery.UpdateNode(ctx, node)
}

// Heartbeat 节点心跳
func (had *HealthAwareDiscovery) Heartbeat(ctx context.Context, nodeID string) error {
	return had.discovery.Heartbeat(ctx, nodeID)
}

// Close 关闭健康感知服务发现
func (had *HealthAwareDiscovery) Close() error {
	had.healthChecker.Stop()
	return had.discovery.Close()
}
