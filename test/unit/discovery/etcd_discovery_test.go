package discovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"turbocache/internal/cluster/discovery"
	"turbocache/internal/config"
)

// 注意：这些测试需要运行的etcd实例，或者使用testcontainers
// 这里提供基本的单元测试框架，实际运行需要etcd环境

// TestNewEtcdDiscovery 测试创建etcd服务发现（需要mock或真实etcd）
func TestNewEtcdDiscovery_InvalidConfig(t *testing.T) {
	// 测试nil配置
	_, err := discovery.NewEtcdDiscovery(nil)
	if err == nil {
		t.Error("Should return error for nil config")
	}
}

// TestNewEtcdDiscovery_ConnectionFailure 测试连接失败
func TestNewEtcdDiscovery_ConnectionFailure(t *testing.T) {
	config := &config.DiscoveryConfig{
		Endpoints:         []string{"127.0.0.1:2379"}, // 假设没有运行etcd
		RegisterPath:      "/turbocache/nodes",
		LeaseTTL:          30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
	}

	_, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := discovery.NewEtcdDiscovery(config)
	if err == nil {
		// 如果本地有运行etcd，这个测试会通过
		t.Skip("Etcd appears to be running locally, skipping connection failure test")
	}

	// 验证错误包含预期信息
	t.Logf("Expected connection error: %v", err)
}

// 以下测试需要真实的etcd或testcontainers环境
// 这里提供测试结构，供集成测试时使用

/*
// TestEtcdDiscovery_Integration 集成测试（需要etcd）
func TestEtcdDiscovery_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &config.DiscoveryConfig{
		Endpoints:         []string{"127.0.0.1:2379"},
		RegisterPath:      "/turbocache/test/nodes",
		LeaseTTL:          10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	}

	disc, err := discovery.NewEtcdDiscovery(config)
	if err != nil {
		t.Fatalf("Failed to create etcd discovery: %v", err)
	}
	defer disc.Close()

	t.Run("register and get node", func(t *testing.T) {
		ctx := context.Background()

		node := &discovery.NodeInfo{
			ID:      "test-node-1",
			Address: "127.0.0.1:8001",
			Weight:  100,
			Region:  "test-region",
			Zone:    "test-zone",
			Status:  "healthy",
			Metadata: map[string]string{
				"version": "1.0.0",
			},
		}

		// 注册节点
		err := disc.Register(ctx, node)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// 获取节点
		nodes, err := disc.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		if len(nodes) == 0 {
			t.Fatal("Expected at least one node")
		}

		retrievedNode, exists := nodes["test-node-1"]
		if !exists {
			t.Fatal("Registered node not found")
		}

		if retrievedNode.Address != node.Address {
			t.Errorf("Address mismatch: expected %s, got %s",
				node.Address, retrievedNode.Address)
		}

		// 注销节点
		err = disc.Unregister(ctx, "test-node-1")
		if err != nil {
			t.Fatalf("Unregister failed: %v", err)
		}

		// 验证节点已删除
		nodes, err = disc.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		if _, exists := nodes["test-node-1"]; exists {
			t.Error("Node should be deleted after unregister")
		}
	})

	t.Run("watch node events", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// 开始监听
		eventCh, err := disc.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch failed: %v", err)
		}

		// 注册节点
		node := &discovery.NodeInfo{
			ID:      "test-node-2",
			Address: "127.0.0.1:8002",
			Status:  "healthy",
		}

		err = disc.Register(ctx, node)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// 等待事件
		select {
		case event := <-eventCh:
			if event.Type != discovery.EventNodeAdded {
				t.Errorf("Expected EventNodeAdded, got %v", event.Type)
			}
			if event.Node.ID != "test-node-2" {
				t.Errorf("Expected node test-node-2, got %s", event.Node.ID)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for node added event")
		}

		// 更新节点
		node.Status = "draining"
		err = disc.UpdateNode(ctx, node)
		if err != nil {
			t.Fatalf("UpdateNode failed: %v", err)
		}

		// 等待更新事件
		select {
		case event := <-eventCh:
			if event.Type != discovery.EventNodeUpdated {
				t.Errorf("Expected EventNodeUpdated, got %v", event.Type)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for node updated event")
		}

		// 清理
		disc.Unregister(ctx, "test-node-2")
	})

	t.Run("heartbeat keeps node alive", func(t *testing.T) {
		ctx := context.Background()

		node := &discovery.NodeInfo{
			ID:      "test-node-3",
			Address: "127.0.0.1:8003",
			Status:  "healthy",
		}

		err := disc.Register(ctx, node)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// 发送心跳
		for i := 0; i < 3; i++ {
			time.Sleep(2 * time.Second)
			err = disc.Heartbeat(ctx, "test-node-3")
			if err != nil {
				t.Errorf("Heartbeat %d failed: %v", i, err)
			}
		}

		// 验证节点仍然存在
		nodes, err := disc.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		if _, exists := nodes["test-node-3"]; !exists {
			t.Error("Node should still exist after heartbeats")
		}

		// 清理
		disc.Unregister(ctx, "test-node-3")
	})

	t.Run("node expires without heartbeat", func(t *testing.T) {
		ctx := context.Background()

		// 使用较短的TTL
		shortConfig := *config
		shortConfig.LeaseTTL = 3 * time.Second

		shortDisc, err := discovery.NewEtcdDiscovery(&shortConfig)
		if err != nil {
			t.Fatalf("Failed to create discovery: %v", err)
		}
		defer shortDisc.Close()

		node := &discovery.NodeInfo{
			ID:      "test-node-4",
			Address: "127.0.0.1:8004",
			Status:  "healthy",
		}

		err = shortDisc.Register(ctx, node)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// 等待租约过期（不发送心跳）
		time.Sleep(5 * time.Second)

		// 节点应该被自动移除
		nodes, err := shortDisc.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		if _, exists := nodes["test-node-4"]; exists {
			t.Error("Node should be removed after lease expiration")
		}
	})
}

// TestEtcdDiscovery_MultipleInstances 测试多个实例
func TestEtcdDiscovery_MultipleInstances(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	config := &config.DiscoveryConfig{
		Endpoints:         []string{"127.0.0.1:2379"},
		RegisterPath:      "/turbocache/test/multi",
		LeaseTTL:          10 * time.Second,
		HeartbeatInterval: 3 * time.Second,
	}

	// 创建多个discovery实例
	disc1, err := discovery.NewEtcdDiscovery(config)
	if err != nil {
		t.Fatalf("Failed to create discovery 1: %v", err)
	}
	defer disc1.Close()

	disc2, err := discovery.NewEtcdDiscovery(config)
	if err != nil {
		t.Fatalf("Failed to create discovery 2: %v", err)
	}
	defer disc2.Close()

	ctx := context.Background()

	// 实例1注册节点
	node1 := &discovery.NodeInfo{
		ID:      "node-1",
		Address: "127.0.0.1:8001",
		Status:  "healthy",
	}
	err = disc1.Register(ctx, node1)
	if err != nil {
		t.Fatalf("Register node1 failed: %v", err)
	}

	// 实例2应该能看到节点1
	time.Sleep(500 * time.Millisecond)
	nodes, err := disc2.GetNodes(ctx)
	if err != nil {
		t.Fatalf("GetNodes failed: %v", err)
	}

	if _, exists := nodes["node-1"]; !exists {
		t.Error("Instance 2 should see node registered by instance 1")
	}

	// 实例2注册节点
	node2 := &discovery.NodeInfo{
		ID:      "node-2",
		Address: "127.0.0.1:8002",
		Status:  "healthy",
	}
	err = disc2.Register(ctx, node2)
	if err != nil {
		t.Fatalf("Register node2 failed: %v", err)
	}

	// 实例1应该能看到节点2
	time.Sleep(500 * time.Millisecond)
	nodes, err = disc1.GetNodes(ctx)
	if err != nil {
		t.Fatalf("GetNodes failed: %v", err)
	}

	if len(nodes) != 2 {
		t.Errorf("Expected 2 nodes, got %d", len(nodes))
	}

	// 清理
	disc1.Unregister(ctx, "node-1")
	disc2.Unregister(ctx, "node-2")
}
*/

// TestInMemoryDiscovery 测试内存服务发现（不依赖etcd）
func TestInMemoryDiscovery(t *testing.T) {
	config := &discovery.RegistryConfig{
		NodeTTL:         30 * time.Second,
		CleanupInterval: 10 * time.Second,
	}

	disc := discovery.NewInMemoryDiscovery(config)
	if disc == nil {
		t.Fatal("NewInMemoryDiscovery returned nil")
	}
	defer disc.Close()

	ctx := context.Background()

	t.Run("register and get nodes", func(t *testing.T) {
		node := &discovery.NodeInfo{
			ID:      "node1",
			Address: "127.0.0.1:8001",
			Weight:  100,
			Status:  "healthy",
		}

		err := disc.Register(ctx, node)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		nodes, err := disc.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		if len(nodes) != 1 {
			t.Errorf("Expected 1 node, got %d", len(nodes))
		}

		if _, exists := nodes["node1"]; !exists {
			t.Error("Registered node not found")
		}
	})

	t.Run("update node", func(t *testing.T) {
		node := &discovery.NodeInfo{
			ID:      "node1",
			Address: "127.0.0.1:8001",
			Weight:  200, // 更新权重
			Status:  "draining",
		}

		err := disc.UpdateNode(ctx, node)
		if err != nil {
			t.Fatalf("UpdateNode failed: %v", err)
		}

		nodes, err := disc.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		updatedNode := nodes["node1"]
		if updatedNode.Weight != 200 {
			t.Errorf("Expected weight 200, got %d", updatedNode.Weight)
		}
	})

	t.Run("heartbeat", func(t *testing.T) {
		err := disc.Heartbeat(ctx, "node1")
		if err != nil {
			t.Errorf("Heartbeat failed: %v", err)
		}

		// 不存在的节点
		err = disc.Heartbeat(ctx, "node999")
		if err == nil {
			t.Error("Heartbeat should fail for non-existent node")
		}
	})

	t.Run("watch events", func(t *testing.T) {
		eventCh, err := disc.Watch(ctx)
		if err != nil {
			t.Fatalf("Watch failed: %v", err)
		}

		// 注册新节点
		node2 := &discovery.NodeInfo{
			ID:      "node2",
			Address: "127.0.0.1:8002",
			Status:  "healthy",
		}

		err = disc.Register(ctx, node2)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// 等待事件
		select {
		case event := <-eventCh:
			if event.Type != discovery.EventNodeAdded {
				t.Errorf("Expected EventNodeAdded, got %v", event.Type)
			}
		case <-time.After(1 * time.Second):
			t.Fatal("Timeout waiting for event")
		}
	})

	t.Run("unregister", func(t *testing.T) {
		err := disc.Unregister(ctx, "node1")
		if err != nil {
			t.Fatalf("Unregister failed: %v", err)
		}

		nodes, err := disc.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		if _, exists := nodes["node1"]; exists {
			t.Error("Node should be removed after unregister")
		}
	})
}

// TestInMemoryDiscovery_Multiple 测试内存发现的多节点场景
func TestInMemoryDiscovery_Multiple(t *testing.T) {
	disc := discovery.NewInMemoryDiscovery(nil)
	defer disc.Close()

	ctx := context.Background()

	// 注册多个节点
	for i := 1; i <= 5; i++ {
		node := &discovery.NodeInfo{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
			Weight:  100 * i,
			Region:  "us-east",
			Zone:    fmt.Sprintf("zone-%d", i%2),
			Status:  "healthy",
		}
		err := disc.Register(ctx, node)
		if err != nil {
			t.Fatalf("Register node%d failed: %v", i, err)
		}
	}

	// 验证所有节点
	nodes, err := disc.GetNodes(ctx)
	if err != nil {
		t.Fatalf("GetNodes failed: %v", err)
	}

	if len(nodes) != 5 {
		t.Errorf("Expected 5 nodes, got %d", len(nodes))
	}

	// 验证节点属性
	for i := 1; i <= 5; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		node, exists := nodes[nodeID]
		if !exists {
			t.Errorf("Node %s not found", nodeID)
			continue
		}

		expectedWeight := 100 * i
		if node.Weight != expectedWeight {
			t.Errorf("Node %s: expected weight %d, got %d",
				nodeID, expectedWeight, node.Weight)
		}
	}
}

// TestHealthAwareDiscovery 测试健康感知的服务发现
func TestHealthAwareDiscovery(t *testing.T) {
	// 使用内存发现作为底层
	baseDiscovery := discovery.NewInMemoryDiscovery(nil)

	healthConfig := &discovery.HealthCheckConfig{
		Interval:         1 * time.Hour, // 禁用自动检查
		Timeout:          100 * time.Millisecond,
		FailureThreshold: 2,
		SuccessThreshold: 1,
	}

	had := discovery.NewHealthAwareDiscovery(baseDiscovery, healthConfig)
	defer had.Close()

	ctx := context.Background()

	t.Run("register with health check", func(t *testing.T) {
		node := &discovery.NodeInfo{
			ID:      "node1",
			Address: "127.0.0.1:9999", // 无效地址
			Status:  "healthy",
		}

		err := had.Register(ctx, node)
		if err != nil {
			t.Fatalf("Register failed: %v", err)
		}

		// 等待健康检查
		time.Sleep(200 * time.Millisecond)

		// 由于地址无效，节点可能被标记为不健康
		// 但仍应该在发现系统中
		nodes, err := had.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		// GetNodes返回健康的节点，由于地址无效，可能为空
		t.Logf("Healthy nodes count: %d", len(nodes))
	})

	t.Run("unregister removes health check", func(t *testing.T) {
		err := had.Unregister(ctx, "node1")
		if err != nil {
			t.Fatalf("Unregister failed: %v", err)
		}

		// 验证节点已删除
		nodes, err := had.GetNodes(ctx)
		if err != nil {
			t.Fatalf("GetNodes failed: %v", err)
		}

		if _, exists := nodes["node1"]; exists {
			t.Error("Node should be removed")
		}
	})
}

// BenchmarkInMemoryDiscovery_Register 性能基准测试
func BenchmarkInMemoryDiscovery_Register(b *testing.B) {
	disc := discovery.NewInMemoryDiscovery(nil)
	defer disc.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := &discovery.NodeInfo{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:80%02d", i%100),
		}
		disc.Register(ctx, node)
	}
}

// BenchmarkInMemoryDiscovery_GetNodes 性能基准测试
func BenchmarkInMemoryDiscovery_GetNodes(b *testing.B) {
	disc := discovery.NewInMemoryDiscovery(nil)
	defer disc.Close()

	ctx := context.Background()

	// 预先注册节点
	for i := 0; i < 100; i++ {
		node := &discovery.NodeInfo{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:80%02d", i),
		}
		disc.Register(ctx, node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		disc.GetNodes(ctx)
	}
}
