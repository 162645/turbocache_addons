package discovery

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"turbocache/internal/cluster/discovery"
)

// TestNewNodeRegistry 测试创建节点注册表
func TestNewNodeRegistry(t *testing.T) {
	config := &discovery.RegistryConfig{
		NodeTTL:             30 * time.Second,
		CleanupInterval:     10 * time.Second,
		HealthCheckInterval: 5 * time.Second,
	}

	registry := discovery.NewNodeRegistry(config)
	if registry == nil {
		t.Fatal("NewNodeRegistry 返回 nil")
	}

	stats := registry.GetStats()
	if stats.TotalNodes != 0 {
		t.Error("新注册表应当没有节点")
	}
}

// TestNodeRegistry_Register 测试注册节点
func TestNodeRegistry_Register(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Weight:  100,
		Region:  "us-east",
		Zone:    "us-east-1a",
		Status:  "healthy",
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	// 注册节点
	err := registry.Register(node)
	if err != nil {
		t.Fatalf("Register() 失败: %v", err)
	}

	// 验证节点已注册
	stats := registry.GetStats()
	if stats.TotalNodes != 1 {
		t.Errorf("期望 1 个节点，实际 %d", stats.TotalNodes)
	}

	// 获取节点
	retrievedNode, exists := registry.GetNode("node1")
	if !exists {
		t.Fatal("注册后节点应存在")
	}

	if retrievedNode.ID != node.ID {
		t.Errorf("期望节点 ID %s，实际 %s", node.ID, retrievedNode.ID)
	}

	if retrievedNode.Address != node.Address {
		t.Errorf("期望地址 %s，实际 %s", node.Address, retrievedNode.Address)
	}
}

// TestNodeRegistry_RegisterInvalidNode 测试注册无效节点
func TestNodeRegistry_RegisterInvalidNode(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	// 空 ID 的节点应被拒绝
	node := &discovery.NodeInfo{
		ID:      "",
		Address: "127.0.0.1:8001",
	}

	err := registry.Register(node)
	if err == nil {
		t.Error("空节点 ID 应返回错误")
	}
}

// TestNodeRegistry_Unregister 测试注销节点
func TestNodeRegistry_Unregister(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
	}

	registry.Register(node)

	// 注销节点
	err := registry.Unregister("node1")
	if err != nil {
		t.Fatalf("Unregister() 失败: %v", err)
	}

	// 验证节点已删除
	_, exists := registry.GetNode("node1")
	if exists {
		t.Error("注销后节点不应存在")
	}

	// 注销不存在的节点应返回错误
	err = registry.Unregister("node2")
	if err == nil {
		t.Error("注销不存在节点应返回错误")
	}
}

// TestNodeRegistry_GetAllNodes 测试获取所有节点
func TestNodeRegistry_GetAllNodes(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	// 注册多个节点
	for i := 1; i <= 3; i++ {
		node := &discovery.NodeInfo{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
		}
		registry.Register(node)
	}

	nodes := registry.GetAllNodes()
	if len(nodes) != 3 {
		t.Errorf("期望 3 个节点，实际 %d", len(nodes))
	}

	// 验证所有节点都存在
	for i := 1; i <= 3; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		if _, exists := nodes[nodeID]; !exists {
			t.Errorf("未找到节点 %s", nodeID)
		}
	}
}

// TestNodeRegistry_GetHealthyNodes 测试获取健康节点
func TestNodeRegistry_GetHealthyNodes(t *testing.T) {
	config := &discovery.RegistryConfig{
		NodeTTL:         30 * time.Second,
		CleanupInterval: 10 * time.Second,
	}
	registry := discovery.NewNodeRegistry(config)

	// 注册健康节点
	healthyNode := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Status:  "healthy",
	}
	registry.Register(healthyNode)

	// 注册不健康节点
	unhealthyNode := &discovery.NodeInfo{
		ID:      "node2",
		Address: "127.0.0.1:8002",
		Status:  "unhealthy",
	}
	registry.Register(unhealthyNode)

	// 获取健康节点
	healthyNodes := registry.GetHealthyNodes()
	if len(healthyNodes) != 1 {
		t.Errorf("期望 1 个健康节点，实际 %d", len(healthyNodes))
	}

	if _, exists := healthyNodes["node1"]; !exists {
		t.Error("node1 应在健康节点中")
	}

	if _, exists := healthyNodes["node2"]; exists {
		t.Error("node2 不应在健康节点中")
	}
}

// TestNodeRegistry_UpdateNodeStatus 测试更新节点状态
func TestNodeRegistry_UpdateNodeStatus(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Status:  "healthy",
	}
	registry.Register(node)

	// 更新状态
	err := registry.UpdateNodeStatus("node1", "draining")
	if err != nil {
		t.Fatalf("UpdateNodeStatus() 失败: %v", err)
	}

	// 验证状态已更新
	updatedNode, _ := registry.GetNode("node1")
	if updatedNode.Status != "draining" {
		t.Errorf("期望状态 'draining'，实际 '%s'", updatedNode.Status)
	}

	// 更新不存在的节点应返回错误
	err = registry.UpdateNodeStatus("node2", "healthy")
	if err == nil {
		t.Error("更新不存在节点应返回错误")
	}
}

// TestNodeRegistry_Heartbeat 测试心跳
func TestNodeRegistry_Heartbeat(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
	}
	registry.Register(node)

	// 记录初始 LastSeen
	initialNode, _ := registry.GetNode("node1")
	initialLastSeen := initialNode.LastSeen

	// 等待一小段时间
	time.Sleep(10 * time.Millisecond)

	// 发送心跳
	err := registry.Heartbeat("node1")
	if err != nil {
		t.Fatalf("Heartbeat() 失败: %v", err)
	}

	// 验证 LastSeen 已更新
	updatedNode, _ := registry.GetNode("node1")
	if !updatedNode.LastSeen.After(initialLastSeen) {
		t.Error("心跳后 LastSeen 应被更新")
	}

	// 不存在的节点心跳应返回错误
	err = registry.Heartbeat("node2")
	if err == nil {
		t.Error("不存在节点的心跳应返回错误")
	}
}

// TestNodeRegistry_Subscribe 测试订阅节点事件
func TestNodeRegistry_Subscribe(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	// 订阅事件
	eventCh := registry.Subscribe("test-subscriber")

	// 注册节点（应该收到 ADD 事件）
	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
	}
	registry.Register(node)

	// 等待事件
	select {
	case event := <-eventCh:
		if event.Type != discovery.EventNodeAdded {
			t.Errorf("期望 EventNodeAdded，实际 %v", event.Type)
		}
		if event.Node.ID != "node1" {
			t.Errorf("期望 node1，实际 %s", event.Node.ID)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("等待事件超时")
	}

	// 注销节点（应该收到 REMOVED 事件）
	registry.Unregister("node1")

	select {
	case event := <-eventCh:
		if event.Type != discovery.EventNodeRemoved {
			t.Errorf("期望 EventNodeRemoved，实际 %v", event.Type)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("等待移除事件超时")
	}
}

// TestNodeRegistry_Unsubscribe 测试取消订阅
func TestNodeRegistry_Unsubscribe(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	eventCh := registry.Subscribe("test-subscriber")

	// 取消订阅
	registry.Unsubscribe("test-subscriber")

	// 通道应该被关闭
	select {
	case _, ok := <-eventCh:
		if ok {
			t.Error("取消订阅后通道应被关闭")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("通道未关闭")
	}
}

// TestNodeRegistry_GetStats 测试统计信息
func TestNodeRegistry_GetStats(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	// 注册不同状态的节点
	registry.Register(&discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Status:  "healthy",
		Region:  "us-east",
		Zone:    "us-east-1a",
	})

	registry.Register(&discovery.NodeInfo{
		ID:      "node2",
		Address: "127.0.0.1:8002",
		Status:  "healthy",
		Region:  "us-east",
		Zone:    "us-east-1b",
	})

	registry.Register(&discovery.NodeInfo{
		ID:      "node3",
		Address: "127.0.0.1:8003",
		Status:  "unhealthy",
		Region:  "us-west",
		Zone:    "us-west-1a",
	})

	stats := registry.GetStats()

	if stats.TotalNodes != 3 {
		t.Errorf("期望总节点数 3，实际 %d", stats.TotalNodes)
	}

	if stats.HealthyNodes != 2 {
		t.Errorf("期望健康节点数 2，实际 %d", stats.HealthyNodes)
	}

	if stats.UnhealthyNodes != 1 {
		t.Errorf("期望不健康节点数 1，实际 %d", stats.UnhealthyNodes)
	}

	if stats.NodesByStatus["healthy"] != 2 {
		t.Errorf("期望状态映射中 healthy 为 2，实际 %d", stats.NodesByStatus["healthy"])
	}

	if stats.NodesByRegion["us-east"] != 2 {
		t.Errorf("期望 us-east 区域有 2 个节点，实际 %d", stats.NodesByRegion["us-east"])
	}
}

// TestNodeRegistry_NodeExpiration 测试节点过期
func TestNodeRegistry_NodeExpiration(t *testing.T) {
	config := &discovery.RegistryConfig{
		NodeTTL:         100 * time.Millisecond, // 短 TTL 用于测试
		CleanupInterval: 50 * time.Millisecond,
	}
	registry := discovery.NewNodeRegistry(config)

	// 订阅事件
	eventCh := registry.Subscribe("test-subscriber")

	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
	}
	registry.Register(node)

	// 等待节点过期
	time.Sleep(200 * time.Millisecond)

	// 应该收到节点移除事件
	select {
	case event := <-eventCh:
		// 跳过初始的 ADD 事件
		if event.Type == discovery.EventNodeAdded {
			select {
			case event := <-eventCh:
				if event.Type != discovery.EventNodeRemoved {
					t.Errorf("期望 EventNodeRemoved，实际 %v", event.Type)
				}
			case <-time.After(300 * time.Millisecond):
				t.Fatal("等待过期事件超时")
			}
		}
	case <-time.After(300 * time.Millisecond):
		t.Fatal("等待事件超时")
	}

	// 验证节点已被清理
	_, exists := registry.GetNode("node1")
	if exists {
		t.Error("过期节点应已被移除")
	}
}

// TestNodeRegistry_ConcurrentOperations 测试并发操作
func TestNodeRegistry_ConcurrentOperations(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// 并发注册节点
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			node := &discovery.NodeInfo{
				ID:      fmt.Sprintf("node%d", id),
				Address: fmt.Sprintf("127.0.0.1:80%02d", id),
			}
			if err := registry.Register(node); err != nil {
				errChan <- err
			}
		}(i)
	}

	// 并发读取节点
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node%d", id%25)
			registry.GetNode(nodeID)
		}(i)
	}

	// 并发心跳
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node%d", id)
			time.Sleep(10 * time.Millisecond)
			registry.Heartbeat(nodeID)
		}(i)
	}

	wg.Wait()
	close(errChan)

	// 检查错误
	for err := range errChan {
		t.Error(err)
	}

	// 验证最终状态
	stats := registry.GetStats()
	if stats.TotalNodes < 30 {
		t.Logf("警告：期望至少 30 个节点，实际 %d", stats.TotalNodes)
	}
}

// TestNodeRegistry_Close 测试关闭注册表
func TestNodeRegistry_Close(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	// 注册节点
	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
	}
	registry.Register(node)

	// 订阅事件
	eventCh := registry.Subscribe("test-subscriber")

	// 关闭注册表
	err := registry.Close()
	if err != nil {
		t.Errorf("Close() 失败: %v", err)
	}

	// 验证事件通道已关闭
	select {
	case _, ok := <-eventCh:
		if ok {
			t.Error("事件通道应已关闭")
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("事件通道未关闭")
	}

	// 关闭后的操作应该失败
	err = registry.Register(&discovery.NodeInfo{ID: "node2", Address: "127.0.0.1:8002"})
	if err == nil {
		t.Error("关闭后 Register 应失败")
	}
}

// TestNodeRegistry_MultipleSubscribers 测试多个订阅者
func TestNodeRegistry_MultipleSubscribers(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	// 创建多个订阅者
	sub1 := registry.Subscribe("subscriber1")
	sub2 := registry.Subscribe("subscriber2")
	sub3 := registry.Subscribe("subscriber3")

	// 注册节点
	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
	}
	registry.Register(node)

	// 所有订阅者都应该收到事件
	timeout := time.After(1 * time.Second)
	receivedCount := 0

	for i := 0; i < 3; i++ {
		select {
		case event := <-sub1:
			if event.Type == discovery.EventNodeAdded {
				receivedCount++
			}
		case event := <-sub2:
			if event.Type == discovery.EventNodeAdded {
				receivedCount++
			}
		case event := <-sub3:
			if event.Type == discovery.EventNodeAdded {
				receivedCount++
			}
		case <-timeout:
			t.Fatal("等待事件超时")
		}
	}

	if receivedCount < 3 {
		t.Errorf("期望 3 个订阅者都收到事件，实际 %d", receivedCount)
	}
}

// TestNodeRegistry_UpdateTriggersEvent 测试更新触发事件
func TestNodeRegistry_UpdateTriggersEvent(t *testing.T) {
	registry := discovery.NewNodeRegistry(nil)

	eventCh := registry.Subscribe("test-subscriber")

	// 注册节点
	node := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Status:  "healthy",
	}
	registry.Register(node)

	// 消费 ADD 事件
	<-eventCh

	// 更新节点（Register 会覆盖更新）
	updatedNode := &discovery.NodeInfo{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Status:  "healthy",
		Metadata: map[string]string{
			"version": "2.0.0",
		},
	}
	registry.Register(updatedNode)

	// 应该收到 UPDATE 事件
	select {
	case event := <-eventCh:
		if event.Type != discovery.EventNodeUpdated {
			t.Errorf("期望 EventNodeUpdated，实际 %v", event.Type)
		}
		if event.PrevNode == nil {
			t.Error("更新事件的 PrevNode 不应为 nil")
		}
	case <-time.After(1 * time.Second):
		t.Fatal("等待更新事件超时")
	}
}

// BenchmarkNodeRegistry_Register 性能基准测试
func BenchmarkNodeRegistry_Register(b *testing.B) {
	registry := discovery.NewNodeRegistry(nil)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		node := &discovery.NodeInfo{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:80%02d", i%100),
		}
		registry.Register(node)
	}
}

// BenchmarkNodeRegistry_GetNode 性能基准测试
func BenchmarkNodeRegistry_GetNode(b *testing.B) {
	registry := discovery.NewNodeRegistry(nil)

	// 预先注册节点
	for i := 0; i < 100; i++ {
		node := &discovery.NodeInfo{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:80%02d", i),
		}
		registry.Register(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := fmt.Sprintf("node%d", i%100)
		registry.GetNode(nodeID)
	}
}

// BenchmarkNodeRegistry_Heartbeat 性能基准测试
func BenchmarkNodeRegistry_Heartbeat(b *testing.B) {
	registry := discovery.NewNodeRegistry(nil)

	// 预先注册节点
	for i := 0; i < 100; i++ {
		node := &discovery.NodeInfo{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:80%02d", i),
		}
		registry.Register(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := fmt.Sprintf("node%d", i%100)
		registry.Heartbeat(nodeID)
	}
}
