package discovery

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"

	"turbocache/internal/config"
	"turbocache/internal/utils/logger"
)

// NodeInfo 节点信息
type NodeInfo struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Weight   int               `json:"weight"`
	Region   string            `json:"region"`
	Zone     string            `json:"zone"`
	Metadata map[string]string `json:"metadata"`

	// 运行时信息
	RegisteredAt time.Time `json:"registered_at"`
	LastSeen     time.Time `json:"last_seen"`
	Status       string    `json:"status"` // healthy, unhealthy, draining
}

// EventType 事件类型
type EventType int

const (
	EventNodeAdded EventType = iota
	EventNodeUpdated
	EventNodeRemoved
)

// NodeEvent 节点事件
type NodeEvent struct {
	Type     EventType `json:"type"`
	Node     *NodeInfo `json:"node"`
	PrevNode *NodeInfo `json:"prev_node,omitempty"`
}

// DiscoveryService 服务发现接口
type DiscoveryService interface {
	// 注册节点
	Register(ctx context.Context, node *NodeInfo) error

	// 注销节点
	Unregister(ctx context.Context, nodeID string) error

	// 获取所有节点
	GetNodes(ctx context.Context) (map[string]*NodeInfo, error)

	// 监听节点变更
	Watch(ctx context.Context) (<-chan *NodeEvent, error)

	// 更新节点信息
	UpdateNode(ctx context.Context, node *NodeInfo) error

	// 心跳保活
	Heartbeat(ctx context.Context, nodeID string) error

	// 关闭服务
	Close() error
}

// EtcdDiscovery 基于etcd的服务发现实现
type EtcdDiscovery struct {
	client *clientv3.Client
	config *config.DiscoveryConfig
	logger logger.Logger

	// 租约管理
	leaseID     clientv3.LeaseID
	leaseCancel context.CancelFunc

	// 本地缓存
	mu    sync.RWMutex
	nodes map[string]*NodeInfo

	// 事件通道
	eventCh chan *NodeEvent
	watchCh clientv3.WatchChan

	// 生命周期
	closed    bool
	closeOnce sync.Once
}

// NewEtcdDiscovery 创建etcd服务发现实例
func NewEtcdDiscovery(config *config.DiscoveryConfig) (*EtcdDiscovery, error) {
	if config == nil {
		return nil, fmt.Errorf("discovery config is required")
	}

	// 创建etcd客户端
	etcdConfig := clientv3.Config{
		Endpoints:   config.Endpoints,
		DialTimeout: 5 * time.Second,
		Username:    config.Username,
		Password:    config.Password,
	}

	client, err := clientv3.New(etcdConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create etcd client: %v", err)
	}

	discovery := &EtcdDiscovery{
		client:  client,
		config:  config,
		logger:  logger.WithField("component", "etcd_discovery"),
		nodes:   make(map[string]*NodeInfo),
		eventCh: make(chan *NodeEvent, 100),
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := client.Status(ctx, config.Endpoints[0]); err != nil {
		client.Close()
		return nil, fmt.Errorf("failed to connect to etcd: %v", err)
	}

	// 设置默认值
	if discovery.config.LeaseTTL == 0 {
		discovery.config.LeaseTTL = 10 * time.Second
	}
	if discovery.config.HeartbeatInterval == 0 {
		discovery.config.HeartbeatInterval = discovery.config.LeaseTTL / 3
	}
	if discovery.config.RegisterPath == "" {
		discovery.config.RegisterPath = "/turbocache/nodes"
	}

	discovery.logger.Info("Connected to etcd cluster")

	// 加载现有节点
	if err := discovery.loadExistingNodes(context.Background()); err != nil {
		discovery.logger.Warnf("Failed to load existing nodes: %v", err)
	}

	return discovery, nil
}

// Register 注册节点
func (ed *EtcdDiscovery) Register(ctx context.Context, node *NodeInfo) error {
	if ed.closed {
		return fmt.Errorf("discovery service is closed")
	}

	// 设置注册时间和状态
	node.RegisteredAt = time.Now()
	node.LastSeen = time.Now()
	if node.Status == "" {
		node.Status = "healthy"
	}

	// 序列化节点信息
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %v", err)
	}

	// 创建租约
	leaseResp, err := ed.client.Grant(ctx, int64(ed.config.LeaseTTL.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to create lease: %v", err)
	}

	ed.leaseID = leaseResp.ID

	// 注册节点到etcd
	key := ed.getNodeKey(node.ID)
	_, err = ed.client.Put(ctx, key, string(data), clientv3.WithLease(ed.leaseID))
	if err != nil {
		return fmt.Errorf("failed to register node: %v", err)
	}

	// 启动租约续约
	ed.startLeaseKeepAlive(node.ID)

	// 更新本地缓存
	ed.mu.Lock()
	ed.nodes[node.ID] = node
	ed.mu.Unlock()

	ed.logger.Infof("Node registered: %s (%s)", node.ID, node.Address)

	// 发送事件
	select {
	case ed.eventCh <- &NodeEvent{Type: EventNodeAdded, Node: node}:
	default:
		ed.logger.Warn("Event channel is full, dropping node added event")
	}

	return nil
}

// Unregister 注销节点
func (ed *EtcdDiscovery) Unregister(ctx context.Context, nodeID string) error {
	if ed.closed {
		return fmt.Errorf("discovery service is closed")
	}

	// 从etcd删除节点
	key := ed.getNodeKey(nodeID)
	resp, err := ed.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to unregister node: %v", err)
	}

	if resp.Deleted == 0 {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// 撤销租约
	if ed.leaseID != 0 {
		if ed.leaseCancel != nil {
			ed.leaseCancel()
		}
		_, err := ed.client.Revoke(ctx, ed.leaseID)
		if err != nil {
			ed.logger.Warnf("Failed to revoke lease: %v", err)
		}
		ed.leaseID = 0
	}

	// 更新本地缓存
	ed.mu.Lock()
	node := ed.nodes[nodeID]
	delete(ed.nodes, nodeID)
	ed.mu.Unlock()

	ed.logger.Infof("Node unregistered: %s", nodeID)

	// 发送事件
	if node != nil {
		select {
		case ed.eventCh <- &NodeEvent{Type: EventNodeRemoved, Node: node}:
		default:
			ed.logger.Warn("Event channel is full, dropping node removed event")
		}
	}

	return nil
}

// GetNodes 获取所有节点
func (ed *EtcdDiscovery) GetNodes(ctx context.Context) (map[string]*NodeInfo, error) {
	if ed.closed {
		return nil, fmt.Errorf("discovery service is closed")
	}

	ed.mu.RLock()
	nodes := make(map[string]*NodeInfo)
	for id, node := range ed.nodes {
		// 深拷贝节点信息
		nodeCopy := *node
		nodeCopy.Metadata = make(map[string]string)
		for k, v := range node.Metadata {
			nodeCopy.Metadata[k] = v
		}
		nodes[id] = &nodeCopy
	}
	ed.mu.RUnlock()

	return nodes, nil
}

// Watch 监听节点变更
func (ed *EtcdDiscovery) Watch(ctx context.Context) (<-chan *NodeEvent, error) {
	if ed.closed {
		return nil, fmt.Errorf("discovery service is closed")
	}

	// 启动etcd监听
	go ed.startEtcdWatch(ctx)

	return ed.eventCh, nil
}

// UpdateNode 更新节点信息
func (ed *EtcdDiscovery) UpdateNode(ctx context.Context, node *NodeInfo) error {
	if ed.closed {
		return fmt.Errorf("discovery service is closed")
	}

	// 检查节点是否存在
	ed.mu.RLock()
	oldNode, exists := ed.nodes[node.ID]
	ed.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node not found: %s", node.ID)
	}

	// 保留注册时间，更新最后见面时间
	node.RegisteredAt = oldNode.RegisteredAt
	node.LastSeen = time.Now()

	// 序列化节点信息
	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("failed to marshal node info: %v", err)
	}

	// 更新etcd中的节点信息
	key := ed.getNodeKey(node.ID)
	_, err = ed.client.Put(ctx, key, string(data), clientv3.WithLease(ed.leaseID))
	if err != nil {
		return fmt.Errorf("failed to update node: %v", err)
	}

	// 更新本地缓存
	ed.mu.Lock()
	prevNode := ed.nodes[node.ID]
	ed.nodes[node.ID] = node
	ed.mu.Unlock()

	ed.logger.Debugf("Node updated: %s", node.ID)

	// 发送事件
	select {
	case ed.eventCh <- &NodeEvent{Type: EventNodeUpdated, Node: node, PrevNode: prevNode}:
	default:
		ed.logger.Warn("Event channel is full, dropping node updated event")
	}

	return nil
}

// Heartbeat 心跳保活
func (ed *EtcdDiscovery) Heartbeat(ctx context.Context, nodeID string) error {
	if ed.closed {
		return fmt.Errorf("discovery service is closed")
	}

	ed.mu.RLock()
	node, exists := ed.nodes[nodeID]
	ed.mu.RUnlock()

	if !exists {
		return fmt.Errorf("node not found: %s", nodeID)
	}

	// 更新最后见面时间
	node.LastSeen = time.Now()
	return ed.UpdateNode(ctx, node)
}

// Close 关闭服务
func (ed *EtcdDiscovery) Close() error {
	var err error
	ed.closeOnce.Do(func() {
		ed.closed = true

		// 取消租约续约
		if ed.leaseCancel != nil {
			ed.leaseCancel()
		}

		// 关闭事件通道
		close(ed.eventCh)

		// 关闭etcd客户端
		if ed.client != nil {
			err = ed.client.Close()
		}

		ed.logger.Info("Etcd discovery service closed")
	})

	return err
}

// loadExistingNodes 加载现有节点
func (ed *EtcdDiscovery) loadExistingNodes(ctx context.Context) error {
	prefix := ed.config.RegisterPath
	resp, err := ed.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return fmt.Errorf("failed to load existing nodes: %v", err)
	}

	ed.mu.Lock()
	defer ed.mu.Unlock()

	for _, kv := range resp.Kvs {
		var node NodeInfo
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			ed.logger.Warnf("Failed to unmarshal node data: %v", err)
			continue
		}

		ed.nodes[node.ID] = &node
	}

	ed.logger.Infof("Loaded %d existing nodes", len(ed.nodes))
	return nil
}

// startLeaseKeepAlive 启动租约续约
func (ed *EtcdDiscovery) startLeaseKeepAlive(nodeID string) {
	ctx, cancel := context.WithCancel(context.Background())
	ed.leaseCancel = cancel

	// 启动租约续约
	keepAliveCh, err := ed.client.KeepAlive(ctx, ed.leaseID)
	if err != nil {
		ed.logger.Errorf("Failed to start lease keep alive: %v", err)
		return
	}

	go func() {
		defer cancel()

		ticker := time.NewTicker(ed.config.HeartbeatInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// 执行心跳
				if err := ed.Heartbeat(context.Background(), nodeID); err != nil {
					ed.logger.Warnf("Heartbeat failed for node %s: %v", nodeID, err)
				}
			case resp := <-keepAliveCh:
				if resp == nil {
					ed.logger.Warn("Lease keep alive channel closed")
					return
				}
				ed.logger.Debugf("Lease renewed: %d", resp.ID)
			}
		}
	}()
}

// startEtcdWatch 启动etcd监听
func (ed *EtcdDiscovery) startEtcdWatch(ctx context.Context) {
	prefix := ed.config.RegisterPath
	watchCh := ed.client.Watch(ctx, prefix, clientv3.WithPrefix())

	for watchResp := range watchCh {
		if watchResp.Err() != nil {
			ed.logger.Errorf("Watch error: %v", watchResp.Err())
			continue
		}

		for _, event := range watchResp.Events {
			ed.handleWatchEvent(event)
		}
	}
}

// handleWatchEvent 处理监听事件
func (ed *EtcdDiscovery) handleWatchEvent(event *clientv3.Event) {
	nodeID := ed.extractNodeIDFromKey(string(event.Kv.Key))
	if nodeID == "" {
		return
	}

	switch event.Type {
	case mvccpb.PUT:
		var node NodeInfo
		if err := json.Unmarshal(event.Kv.Value, &node); err != nil {
			ed.logger.Warnf("Failed to unmarshal node data: %v", err)
			return
		}

		ed.mu.Lock()
		oldNode := ed.nodes[nodeID]
		ed.nodes[nodeID] = &node
		ed.mu.Unlock()

		var eventType EventType
		if oldNode == nil {
			eventType = EventNodeAdded
			ed.logger.Infof("Node added via watch: %s", nodeID)
		} else {
			eventType = EventNodeUpdated
			ed.logger.Debugf("Node updated via watch: %s", nodeID)
		}

		select {
		case ed.eventCh <- &NodeEvent{Type: eventType, Node: &node, PrevNode: oldNode}:
		default:
			ed.logger.Warn("Event channel is full, dropping watch event")
		}

	case mvccpb.DELETE:
		ed.mu.Lock()
		node := ed.nodes[nodeID]
		delete(ed.nodes, nodeID)
		ed.mu.Unlock()

		if node != nil {
			ed.logger.Infof("Node removed via watch: %s", nodeID)
			select {
			case ed.eventCh <- &NodeEvent{Type: EventNodeRemoved, Node: node}:
			default:
				ed.logger.Warn("Event channel is full, dropping watch event")
			}
		}
	}
}

// getNodeKey 获取节点在etcd中的键
func (ed *EtcdDiscovery) getNodeKey(nodeID string) string {
	return path.Join(ed.config.RegisterPath, nodeID)
}

// extractNodeIDFromKey 从etcd键中提取节点ID
func (ed *EtcdDiscovery) extractNodeIDFromKey(key string) string {
	return path.Base(key)
}
