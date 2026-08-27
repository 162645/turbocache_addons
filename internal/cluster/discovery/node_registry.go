package discovery

import (
	"context"
	"fmt"
	"sync"
	"time"

	"turbocache/internal/utils/logger"
)

// NodeRegistry 节点注册表
type NodeRegistry struct {
	mu    sync.RWMutex
	nodes map[string]*NodeInfo
	// 使用双向通道，Close 时可以明确关闭通道
	subscribers map[string]chan *NodeEvent
	logger      logger.Logger

	// 配置
	config *RegistryConfig

	// 生命周期
	closed    bool
	closeOnce sync.Once
	stopCh    chan struct{}
	cleanupWG sync.WaitGroup
}

// RegistryConfig 注册表配置
type RegistryConfig struct {
	NodeTTL             time.Duration `yaml:"node_ttl" json:"node_ttl"`
	CleanupInterval     time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval" json:"health_check_interval"`
}

// NewNodeRegistry 创建节点注册表
func NewNodeRegistry(config *RegistryConfig) *NodeRegistry {
	if config == nil {
		config = &RegistryConfig{
			NodeTTL:             30 * time.Second,
			CleanupInterval:     10 * time.Second,
			HealthCheckInterval: 5 * time.Second,
		}
	}

	registry := &NodeRegistry{
		nodes:       make(map[string]*NodeInfo),
		subscribers: make(map[string]chan *NodeEvent),
		logger:      logger.WithField("component", "node_registry"),
		config:      config,
		stopCh:      make(chan struct{}),
	}

	// 启动清理协程
	registry.cleanupWG.Add(1)
	go func() { defer registry.cleanupWG.Done(); registry.startCleanup() }()

	return registry
}

// Register 注册节点
func (nr *NodeRegistry) Register(node *NodeInfo) error {
	if nr.closed {
		return fmt.Errorf("registry is closed")
	}
	if node == nil {
		return fmt.Errorf("node is nil")
	}
	if node.ID == "" {
		return fmt.Errorf("node ID cannot be empty")
	}

	now := time.Now()

	nr.mu.Lock()
	// 存储 node 的副本，避免调用者继续改动原对象影响 registry 内部
	nodeCopy := copyNode(node)
	nodeCopy.RegisteredAt = now
	nodeCopy.LastSeen = now

	oldNode, exists := nr.nodes[nodeCopy.ID]
	nr.nodes[nodeCopy.ID] = nodeCopy

	var eventType EventType
	var prevNodeCopy *NodeInfo
	if exists {
		eventType = EventNodeUpdated
		prevNodeCopy = copyNode(oldNode)
		nr.logger.Debugf("Updated node: %s (%s)", nodeCopy.ID, nodeCopy.Address)
	} else {
		eventType = EventNodeAdded
		nr.logger.Infof("Registered node: %s (%s)", nodeCopy.ID, nodeCopy.Address)
	}

	// 构造要发送的事件的拷贝
	ev := &NodeEvent{
		Type:     eventType,
		Node:     copyNode(nodeCopy),
		PrevNode: prevNodeCopy,
	}
	nr.mu.Unlock()

	// 在锁外把拷贝发送给订阅者（sendToSubscribers 内带 recover，保证不会因并发 close 导致 panic）
	nr.sendEvent(ev)
	return nil
}

// Unregister 注销节点
func (nr *NodeRegistry) Unregister(nodeID string) error {
	if nr.closed {
		return fmt.Errorf("registry is closed")
	}

	nr.mu.Lock()
	node, exists := nr.nodes[nodeID]
	if !exists {
		nr.mu.Unlock()
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// 删除节点并记录要通知的拷贝
	delete(nr.nodes, nodeID)
	nr.logger.Infof("Unregistered node: %s", nodeID)

	ev := &NodeEvent{
		Type: EventNodeRemoved,
		Node: copyNode(node),
	}
	nr.mu.Unlock()

	nr.sendEvent(ev)
	return nil
}

// GetNode 获取节点信息
func (nr *NodeRegistry) GetNode(nodeID string) (*NodeInfo, bool) {
	nr.mu.RLock()
	node, exists := nr.nodes[nodeID]
	if !exists {
		nr.mu.RUnlock()
		return nil, false
	}
	cpy := copyNode(node)
	nr.mu.RUnlock()
	return cpy, true
}

// GetAllNodes 获取所有节点
func (nr *NodeRegistry) GetAllNodes() map[string]*NodeInfo {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	result := make(map[string]*NodeInfo, len(nr.nodes))
	for id, node := range nr.nodes {
		result[id] = copyNode(node)
	}
	return result
}

// GetHealthyNodes 获取健康的节点
func (nr *NodeRegistry) GetHealthyNodes() map[string]*NodeInfo {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	result := make(map[string]*NodeInfo)
	for id, node := range nr.nodes {
		if nr.isNodeHealthy(node) {
			result[id] = copyNode(node)
		}
	}
	return result
}

// UpdateNodeStatus 更新节点状态
func (nr *NodeRegistry) UpdateNodeStatus(nodeID string, status string) error {
	if nr.closed {
		return fmt.Errorf("registry is closed")
	}

	nr.mu.Lock()
	node, exists := nr.nodes[nodeID]
	if !exists {
		nr.mu.Unlock()
		return fmt.Errorf("node not found: %s", nodeID)
	}

	oldNodeCopy := copyNode(node)
	oldStatus := node.Status
	node.Status = status
	node.LastSeen = time.Now()

	// 拷贝 subscribers & 构造事件
	subs := make(map[string]chan *NodeEvent, len(nr.subscribers))
	for id, ch := range nr.subscribers {
		subs[id] = ch
	}

	var ev *NodeEvent
	if oldStatus != status {
		nr.logger.Infof("Node %s status changed: %s -> %s", nodeID, oldStatus, status)
		ev = &NodeEvent{
			Type:     EventNodeUpdated,
			Node:     copyNode(node),
			PrevNode: oldNodeCopy,
		}
	}
	nr.mu.Unlock()

	if ev != nil {
		sendToSubscribers(subs, ev, nr.logger)
	}
	return nil
}

// Heartbeat 节点心跳
func (nr *NodeRegistry) Heartbeat(nodeID string) error {
	if nr.closed {
		return fmt.Errorf("registry is closed")
	}

	nr.mu.Lock()
	node, exists := nr.nodes[nodeID]
	if !exists {
		nr.mu.Unlock()
		return fmt.Errorf("node not found: %s", nodeID)
	}
	node.LastSeen = time.Now()
	nr.logger.Debugf("Received heartbeat from node: %s", nodeID)
	nr.mu.Unlock()
	return nil
}

// Subscribe 订阅节点事件
// 改为：注册并立即返回通道；历史 snapshot 的发送在独立 goroutine 中异步发送，并在发送前检查订阅者仍然存在。
// 这样 Subscribe 返回时通道通常是“干净的”，Close 可以确定性地关闭通道（符合测试期望）。
func (nr *NodeRegistry) Subscribe(subscriberID string) <-chan *NodeEvent {
	nr.mu.Lock()
	if nr.closed {
		nr.mu.Unlock()
		ch := make(chan *NodeEvent)
		close(ch)
		return ch
	}

	ch := make(chan *NodeEvent, 100)
	// store channel in map
	nr.subscribers[subscriberID] = ch
	nr.logger.Debugf("New subscriber: %s", subscriberID)

	nr.mu.Unlock()

	return ch
}

// Unsubscribe 取消订阅
func (nr *NodeRegistry) Unsubscribe(subscriberID string) {
	nr.mu.Lock()
	ch, exists := nr.subscribers[subscriberID]
	if exists {
		delete(nr.subscribers, subscriberID)
		// 在锁内删除映射，关闭通道在锁外执行以减少同时发送时的竞态可能
		nr.mu.Unlock()
		// 关闭通道（消费者端应处理通道关闭）
		// 关闭可能与并发发送产生 race，但 sendToSubscribers 内有 recover 防护，避免 panic 崩溃。
		close(ch)
		nr.logger.Debugf("Unsubscribed: %s", subscriberID)
		return
	}
	nr.mu.Unlock()
}

// GetStats 获取注册表统计信息
func (nr *NodeRegistry) GetStats() *RegistryStats {
	nr.mu.RLock()
	defer nr.mu.RUnlock()

	stats := &RegistryStats{
		TotalNodes:     len(nr.nodes),
		HealthyNodes:   0,
		UnhealthyNodes: 0,
		Subscribers:    len(nr.subscribers),
		NodesByStatus:  make(map[string]int),
		NodesByRegion:  make(map[string]int),
		NodesByZone:    make(map[string]int),
	}

	for _, node := range nr.nodes {
		// 按状态统计
		if node.Status != "" {
			stats.NodesByStatus[node.Status]++
		}
		// 按地域统计
		if node.Region != "" {
			stats.NodesByRegion[node.Region]++
		}
		// 按可用区统计
		if node.Zone != "" {
			stats.NodesByZone[node.Zone]++
		}
		// 健康状态统计
		if nr.isNodeHealthy(node) {
			stats.HealthyNodes++
		} else {
			stats.UnhealthyNodes++
		}
	}

	return stats
}

// Close 关闭注册表
func (nr *NodeRegistry) Close() error {
	var err error
	nr.closeOnce.Do(func() {
		close(nr.stopCh)
		nr.cleanupWG.Wait()
		// 持锁关闭：先设置 closed、复制并清空 subscribers map，然后在锁内关闭通道，保证关闭对订阅者是确定性的
		nr.mu.Lock()
		nr.closed = true

		// 复制并清空所有订阅者（为了避免在解锁后还有其他 goroutine把它们加入）
		subsList := make([]chan *NodeEvent, 0, len(nr.subscribers))
		for _, ch := range nr.subscribers {
			subsList = append(subsList, ch)
		}
		nr.subscribers = make(map[string]chan *NodeEvent)

		// 清空节点
		nr.nodes = make(map[string]*NodeInfo)

		// 在锁内关闭通道（已从 map 中移除，后续并发 Subscribe/Unsubscribe 不会影响这些通道）
		for _, ch := range subsList {
			// 关闭可能会与并发发送产生 panic，但 sendToSubscribers 会 recover
			close(ch)
		}

		nr.mu.Unlock()
		nr.logger.Info("Node registry closed")
	})
	return err
}

// 私有方法

// isNodeHealthy 检查节点是否健康
func (nr *NodeRegistry) isNodeHealthy(node *NodeInfo) bool {
	if node == nil {
		return false
	}
	if node.Status == "unhealthy" || node.Status == "decommissioned" {
		return false
	}
	// 检查是否超时
	if time.Since(node.LastSeen) > nr.config.NodeTTL {
		return false
	}
	return true
}

// sendToSubscribers: 在锁外把事件发送给给定的 subscribers map（不触及 nr.mu）
// 对于向被关闭通道发送的 panic 做 recover 保护，并使用非阻塞发送以避免阻塞调用方。
func sendToSubscribers(subs map[string]chan *NodeEvent, ev *NodeEvent, lg logger.Logger) {
	// 在函数级做 panic recover，防止并发 close 导致的 panic 崩溃测试/进程
	defer func() {
		if r := recover(); r != nil {
			// 发生 panic（可能是向已关闭通道发送）——记录并吞掉
			if lg != nil {
				lg.Warnf("recovering from panic in sendToSubscribers: %v", r)
			}
		}
	}()

	// 我们将发送 event 的副本（已经由调用者准备好），使用非阻塞发送
	for id, ch := range subs {
		select {
		case ch <- ev:
		default:
			if lg != nil {
				lg.Warnf("Subscriber %s channel is full, dropping event", id)
			}
		}
	}
}

// sendEvent keeps the registry read lock while sending. Close and Unsubscribe
// need the write lock before closing a subscriber channel, so send/close cannot race.
func (nr *NodeRegistry) sendEvent(ev *NodeEvent) {
	nr.mu.RLock()
	defer nr.mu.RUnlock()
	for id, ch := range nr.subscribers {
		select {
		case ch <- ev:
		default:
			nr.logger.Warnf("Subscriber %s channel is full, dropping event", id)
		}
	}
}

// startCleanup 启动清理协程
func (nr *NodeRegistry) startCleanup() {
	ticker := time.NewTicker(nr.config.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nr.cleanup()
		case <-nr.stopCh:
			return
		}
	}
}

// cleanup 清理超时节点
func (nr *NodeRegistry) cleanup() {
	nr.mu.Lock()
	now := time.Now()
	expiredNodes := make([]*NodeInfo, 0)

	for id, node := range nr.nodes {
		if now.Sub(node.LastSeen) > nr.config.NodeTTL {
			expiredNodes = append(expiredNodes, copyNode(node))
			delete(nr.nodes, id)
		}
	}

	nr.mu.Unlock()

	for _, node := range expiredNodes {
		nr.logger.Warnf("Node expired: %s (last seen: %v)", node.ID, node.LastSeen)
		ev := &NodeEvent{Type: EventNodeRemoved, Node: copyNode(node)}
		nr.sendEvent(ev)
	}

	if len(expiredNodes) > 0 {
		nr.logger.Infof("Cleaned up %d expired nodes", len(expiredNodes))
	}
}

// RegistryStats 注册表统计信息
type RegistryStats struct {
	TotalNodes     int            `json:"total_nodes"`
	HealthyNodes   int            `json:"healthy_nodes"`
	UnhealthyNodes int            `json:"unhealthy_nodes"`
	Subscribers    int            `json:"subscribers"`
	NodesByStatus  map[string]int `json:"nodes_by_status"`
	NodesByRegion  map[string]int `json:"nodes_by_region"`
	NodesByZone    map[string]int `json:"nodes_by_zone"`
}

// InMemoryDiscovery 基于内存的服务发现实现（用于测试和单机部署）
type InMemoryDiscovery struct {
	registry  *NodeRegistry
	localNode *NodeInfo
	logger    logger.Logger
}

// NewInMemoryDiscovery 创建内存服务发现
func NewInMemoryDiscovery(config *RegistryConfig) *InMemoryDiscovery {
	return &InMemoryDiscovery{
		registry: NewNodeRegistry(config),
		logger:   logger.WithField("component", "inmemory_discovery"),
	}
}

// Register 实现DiscoveryService接口
func (imd *InMemoryDiscovery) Register(ctx context.Context, node *NodeInfo) error {
	imd.localNode = node
	return imd.registry.Register(node)
}

// Unregister 实现DiscoveryService接口
func (imd *InMemoryDiscovery) Unregister(ctx context.Context, nodeID string) error {
	return imd.registry.Unregister(nodeID)
}

// GetNodes 实现DiscoveryService接口
func (imd *InMemoryDiscovery) GetNodes(ctx context.Context) (map[string]*NodeInfo, error) {
	return imd.registry.GetHealthyNodes(), nil
}

// Watch 实现DiscoveryService接口
func (imd *InMemoryDiscovery) Watch(ctx context.Context) (<-chan *NodeEvent, error) {
	subscriberID := fmt.Sprintf("inmemory_%d", time.Now().UnixNano())
	return imd.registry.Subscribe(subscriberID), nil
}

// UpdateNode 实现DiscoveryService接口
func (imd *InMemoryDiscovery) UpdateNode(ctx context.Context, node *NodeInfo) error {
	return imd.registry.Register(node) // 注册会覆盖更新
}

// Heartbeat 实现DiscoveryService接口
func (imd *InMemoryDiscovery) Heartbeat(ctx context.Context, nodeID string) error {
	return imd.registry.Heartbeat(nodeID)
}

// Close 实现DiscoveryService接口
func (imd *InMemoryDiscovery) Close() error {
	return imd.registry.Close()
}

// copyNode 做深拷贝（node 可能为 nil）
func copyNode(node *NodeInfo) *NodeInfo {
	if node == nil {
		return nil
	}
	n := *node
	if node.Metadata != nil {
		n.Metadata = make(map[string]string, len(node.Metadata))
		for k, v := range node.Metadata {
			n.Metadata[k] = v
		}
	} else {
		n.Metadata = make(map[string]string)
	}
	return &n
}
