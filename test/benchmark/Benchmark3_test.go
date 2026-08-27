package benchmark

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turbocache/internal/cache/engine"
	"turbocache/internal/cache/value"
	"turbocache/internal/cluster/discovery"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/config"
)

// MockNode 模拟缓存节点
type MockNode struct {
	ID      string
	Address string
	Cache   *engine.L1Cache
	Running bool
	mu      sync.RWMutex

	// 统计信息
	RequestCount int64
	SuccessCount int64
	FailureCount int64
	AvgLatency   int64
	TotalLatency int64
	LatencyCount int64
}

// NewMockNode 创建模拟节点
func NewMockNode(id, address string) (*MockNode, error) {
	cfg := &config.L1CacheConfig{
		Enabled:    true,
		MaxSize:    1024 * 1024 * 50, // 50MB per node
		MaxEntries: 10000,
		ShardCount: 8,
		TTL:        5 * time.Minute,
	}

	cache, err := engine.NewL1Cache(cfg)
	if err != nil {
		return nil, err
	}

	return &MockNode{
		ID:      id,
		Address: address,
		Cache:   cache,
		Running: true,
	}, nil
}

// Get 处理Get请求
func (n *MockNode) Get(ctx context.Context, key string) (*value.Entry, error) {
	n.mu.RLock()
	running := n.Running
	n.mu.RUnlock()

	if !running {
		atomic.AddInt64(&n.FailureCount, 1)
		return nil, fmt.Errorf("node %s is not running", n.ID)
	}

	atomic.AddInt64(&n.RequestCount, 1)
	start := time.Now()

	entry, err := n.Cache.Get(ctx, key)

	latency := time.Since(start)
	atomic.AddInt64(&n.TotalLatency, latency.Nanoseconds())
	atomic.AddInt64(&n.LatencyCount, 1)

	if err != nil {
		atomic.AddInt64(&n.FailureCount, 1)
		return nil, err
	}

	atomic.AddInt64(&n.SuccessCount, 1)
	return entry, nil
}

// Set 处理Set请求
func (n *MockNode) Set(ctx context.Context, key string, entry *value.Entry) error {
	n.mu.RLock()
	running := n.Running
	n.mu.RUnlock()

	if !running {
		return fmt.Errorf("node %s is not running", n.ID)
	}

	return n.Cache.Set(ctx, key, entry)
}

// Stop 停止节点
func (n *MockNode) Stop() {
	n.mu.Lock()
	n.Running = false
	n.mu.Unlock()
}

// Start 启动节点
func (n *MockNode) Start() {
	n.mu.Lock()
	n.Running = true
	n.mu.Unlock()
}

// GetAvgLatency 获取平均延迟
func (n *MockNode) GetAvgLatency() time.Duration {
	count := atomic.LoadInt64(&n.LatencyCount)
	if count == 0 {
		return 0
	}
	total := atomic.LoadInt64(&n.TotalLatency)
	return time.Duration(total / count)
}

// Close 关闭节点
func (n *MockNode) Close() error {
	return n.Cache.Close()
}

// ClusterSimulator 集群模拟器
type ClusterSimulator struct {
	Nodes     map[string]*MockNode
	Discovery discovery.DiscoveryService
	HashRing  *hash.ConsistentHash
	mu        sync.RWMutex
}

// NewClusterSimulator 创建集群模拟器
func NewClusterSimulator(nodeCount int) (*ClusterSimulator, error) {
	// 创建服务发现
	registryConfig := &discovery.RegistryConfig{
		NodeTTL:             30 * time.Second,
		CleanupInterval:     10 * time.Second,
		HealthCheckInterval: 5 * time.Second,
	}
	disc := discovery.NewInMemoryDiscovery(registryConfig)

	// 创建一致性哈希环
	hashFunc := hash.GetHashFunc("xxhash")
	hashRing := hash.NewConsistentHash(150, hashFunc)

	sim := &ClusterSimulator{
		Nodes:     make(map[string]*MockNode),
		Discovery: disc,
		HashRing:  hashRing,
	}

	// 创建节点
	ctx := context.Background()
	for i := 1; i <= nodeCount; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		address := fmt.Sprintf("localhost:900%d", i)

		node, err := NewMockNode(nodeID, address)
		if err != nil {
			return nil, fmt.Errorf("failed to create node %s: %v", nodeID, err)
		}

		sim.Nodes[nodeID] = node

		// 注册到服务发现
		nodeInfo := &discovery.NodeInfo{
			ID:      nodeID,
			Address: address,
			Weight:  100,
			Status:  "healthy",
			Metadata: map[string]string{
				"region": fmt.Sprintf("region%d", (i-1)%3+1),
			},
		}
		if err := disc.Register(ctx, nodeInfo); err != nil {
			return nil, fmt.Errorf("failed to register node %s: %v", nodeID, err)
		}

		// 添加到哈希环
		hashRing.AddNode(&hash.Node{
			ID:       nodeID,
			Address:  address,
			Weight:   100,
			Metadata: nodeInfo.Metadata,
		})
	}

	return sim, nil
}

// Get 从集群获取数据
func (sim *ClusterSimulator) Get(ctx context.Context, key string) (*value.Entry, error) {
	// 通过一致性哈希找到目标节点
	hashNode := sim.HashRing.GetNode(key)
	if hashNode == nil {
		return nil, fmt.Errorf("no node available for key: %s", key)
	}

	sim.mu.RLock()
	node := sim.Nodes[hashNode.ID]
	sim.mu.RUnlock()

	if node == nil {
		return nil, fmt.Errorf("node %s not found", hashNode.ID)
	}

	return node.Get(ctx, key)
}

// Set 向集群写入数据
func (sim *ClusterSimulator) Set(ctx context.Context, key string, entry *value.Entry) error {
	// 通过一致性哈希找到目标节点
	hashNode := sim.HashRing.GetNode(key)
	if hashNode == nil {
		return fmt.Errorf("no node available for key: %s", key)
	}

	sim.mu.RLock()
	node := sim.Nodes[hashNode.ID]
	sim.mu.RUnlock()

	if node == nil {
		return fmt.Errorf("node %s not found", hashNode.ID)
	}

	return node.Set(ctx, key, entry)
}

// StopNode 停止指定节点 (模拟故障)
func (sim *ClusterSimulator) StopNode(nodeID string) error {
	sim.mu.RLock()
	node := sim.Nodes[nodeID]
	sim.mu.RUnlock()

	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.Stop()

	// 从哈希环移除
	sim.HashRing.RemoveNode(nodeID)

	return nil
}

// StartNode 启动指定节点 (模拟恢复)
func (sim *ClusterSimulator) StartNode(nodeID string) error {
	sim.mu.RLock()
	node := sim.Nodes[nodeID]
	sim.mu.RUnlock()

	if node == nil {
		return fmt.Errorf("node %s not found", nodeID)
	}

	node.Start()

	// 重新加入哈希环
	sim.HashRing.AddNode(&hash.Node{
		ID:      nodeID,
		Address: node.Address,
		Weight:  100,
	})

	return nil
}

// GetClusterStats 获取集群统计信息
func (sim *ClusterSimulator) GetClusterStats() map[string]interface{} {
	sim.mu.RLock()
	defer sim.mu.RUnlock()

	stats := make(map[string]interface{})
	stats["total_nodes"] = len(sim.Nodes)

	var totalRequests, totalSuccess, totalFailure int64
	runningNodes := 0
	nodeStats := make(map[string]interface{})

	for nodeID, node := range sim.Nodes {
		requests := atomic.LoadInt64(&node.RequestCount)
		success := atomic.LoadInt64(&node.SuccessCount)
		failure := atomic.LoadInt64(&node.FailureCount)

		totalRequests += requests
		totalSuccess += success
		totalFailure += failure

		if node.Running {
			runningNodes++
		}

		nodeStats[nodeID] = map[string]interface{}{
			"running":     node.Running,
			"requests":    requests,
			"success":     success,
			"failure":     failure,
			"avg_latency": node.GetAvgLatency().String(),
		}
	}

	stats["running_nodes"] = runningNodes
	stats["total_requests"] = totalRequests
	stats["total_success"] = totalSuccess
	stats["total_failure"] = totalFailure
	if totalRequests > 0 {
		stats["success_rate"] = float64(totalSuccess) / float64(totalRequests) * 100
	}
	stats["node_stats"] = nodeStats

	return stats
}

// Close 关闭模拟器
func (sim *ClusterSimulator) Close() error {
	sim.mu.Lock()
	defer sim.mu.Unlock()

	for _, node := range sim.Nodes {
		node.Close()
	}

	return sim.Discovery.Close()
}

// TestClusterSimulation 完整的集群模拟测试
func TestClusterSimulation(t *testing.T) {
	t.Log("Starting cluster simulation test...")

	// 创建5节点集群
	sim, err := NewClusterSimulator(5)
	if err != nil {
		t.Fatalf("Failed to create cluster simulator: %v", err)
	}
	defer sim.Close()

	ctx := context.Background()

	// Phase 1: 正常操作
	t.Log("\n=== Phase 1: Normal Operations ===")

	// 写入测试数据
	keyCount := 10000
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, value.NewByteView([]byte("value")), 5*time.Minute)
		if err := sim.Set(ctx, key, entry); err != nil {
			t.Logf("Warning: Set failed for key %s: %v", key, err)
		}
	}

	// 读取测试
	successCount := 0
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		if _, err := sim.Get(ctx, key); err == nil {
			successCount++
		}
	}

	initialHitRate := float64(successCount) / float64(keyCount) * 100
	t.Logf("Initial hit rate: %.2f%% (%d/%d)", initialHitRate, successCount, keyCount)

	// Phase 2: 节点故障
	t.Log("\n=== Phase 2: Node Failure ===")

	// 停止2个节点
	if err := sim.StopNode("node2"); err != nil {
		t.Fatalf("Failed to stop node2: %v", err)
	}
	if err := sim.StopNode("node4"); err != nil {
		t.Fatalf("Failed to stop node4: %v", err)
	}
	t.Log("Stopped node2 and node4")

	// 测试故障后的可用性
	time.Sleep(100 * time.Millisecond) // 等待状态同步

	failureSuccessCount := 0
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		if _, err := sim.Get(ctx, key); err == nil {
			failureSuccessCount++
		}
	}

	failureHitRate := float64(failureSuccessCount) / float64(keyCount) * 100
	t.Logf("Hit rate after node failure: %.2f%% (%d/%d)",
		failureHitRate, failureSuccessCount, keyCount)
	t.Logf("Data loss: %.2f%%", (initialHitRate - failureHitRate))

	// Phase 3: 节点恢复
	t.Log("\n=== Phase 3: Node Recovery ===")

	if err := sim.StartNode("node2"); err != nil {
		t.Fatalf("Failed to start node2: %v", err)
	}
	if err := sim.StartNode("node4"); err != nil {
		t.Fatalf("Failed to start node4: %v", err)
	}
	t.Log("Restarted node2 and node4")

	time.Sleep(100 * time.Millisecond)

	recoverySuccessCount := 0
	for i := 0; i < keyCount; i++ {
		key := fmt.Sprintf("key_%d", i)
		if _, err := sim.Get(ctx, key); err == nil {
			recoverySuccessCount++
		}
	}

	recoveryHitRate := float64(recoverySuccessCount) / float64(keyCount) * 100
	t.Logf("Hit rate after recovery: %.2f%% (%d/%d)",
		recoveryHitRate, recoverySuccessCount, keyCount)

	// Phase 4: 高并发压力测试
	t.Log("\n=== Phase 4: Concurrent Load Test ===")

	concurrency := 50
	requestsPerWorker := 1000

	var wg sync.WaitGroup
	wg.Add(concurrency)

	startTime := time.Now()
	var concurrentSuccess, concurrentFailure int64

	for i := 0; i < concurrency; i++ {
		go func(workerID int) {
			defer wg.Done()

			for j := 0; j < requestsPerWorker; j++ {
				key := fmt.Sprintf("key_%d", rand.Intn(keyCount))
				if _, err := sim.Get(ctx, key); err == nil {
					atomic.AddInt64(&concurrentSuccess, 1)
				} else {
					atomic.AddInt64(&concurrentFailure, 1)
				}
			}
		}(i)
	}

	wg.Wait()
	duration := time.Since(startTime)

	totalConcurrentRequests := concurrency * requestsPerWorker
	qps := float64(totalConcurrentRequests) / duration.Seconds()
	successRate := float64(concurrentSuccess) / float64(totalConcurrentRequests) * 100

	t.Logf("Concurrent test results:")
	t.Logf("  Total requests: %d", totalConcurrentRequests)
	t.Logf("  Duration: %v", duration)
	t.Logf("  QPS: %.2f", qps)
	t.Logf("  Success: %d (%.2f%%)", concurrentSuccess, successRate)
	t.Logf("  Failure: %d", concurrentFailure)

	// 最终统计
	t.Log("\n=== Final Cluster Statistics ===")
	finalStats := sim.GetClusterStats()

	t.Logf("Total nodes: %v", finalStats["total_nodes"])
	t.Logf("Running nodes: %v", finalStats["running_nodes"])
	t.Logf("Total requests: %v", finalStats["total_requests"])
	t.Logf("Success rate: %.2f%%", finalStats["success_rate"])

	if nodeStats, ok := finalStats["node_stats"].(map[string]interface{}); ok {
		t.Log("\nPer-node statistics:")
		for nodeID, stats := range nodeStats {
			if nodeMap, ok := stats.(map[string]interface{}); ok {
				t.Logf("  %s: requests=%v, success=%v, failure=%v, avg_latency=%v, running=%v",
					nodeID,
					nodeMap["requests"],
					nodeMap["success"],
					nodeMap["failure"],
					nodeMap["avg_latency"],
					nodeMap["running"],
				)
			}
		}
	}

	// 验证结果
	if failureHitRate < 50.0 {
		t.Errorf("Hit rate after failure too low: %.2f%%, expected >= 50%%", failureHitRate)
	}

	if qps < 10000 {
		t.Errorf("QPS too low: %.2f, expected >= 10000", qps)
	}

	t.Log("\n✅ Cluster simulation test passed")
}

// BenchmarkClusterThroughput 集群吞吐量基准测试
func BenchmarkClusterThroughput(b *testing.B) {
	sim, err := NewClusterSimulator(5)
	if err != nil {
		b.Fatalf("Failed to create cluster simulator: %v", err)
	}
	defer sim.Close()

	ctx := context.Background()

	// 预热
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, value.NewByteView([]byte("value")), 5*time.Minute)
		sim.Set(ctx, key, entry)
	}

	b.ResetTimer()
	b.ReportAllocs()

	// 测试读取吞吐量
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i%10000)
			sim.Get(ctx, key)
			i++
		}
	})
}
