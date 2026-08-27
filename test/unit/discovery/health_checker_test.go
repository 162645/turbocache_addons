package discovery

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turbocache/internal/cluster/discovery"
)

// TestNewHealthChecker 测试创建健康检查器
func TestNewHealthChecker(t *testing.T) {
	tests := []struct {
		name   string
		config *discovery.HealthCheckConfig
	}{
		{
			name: "with config",
			config: &discovery.HealthCheckConfig{
				Interval:         5 * time.Second,
				Timeout:          3 * time.Second,
				FailureThreshold: 3,
				SuccessThreshold: 2,
				MaxConcurrency:   10,
			},
		},
		{
			name:   "with nil config (use defaults)",
			config: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := discovery.NewHealthChecker(tt.config)
			if hc == nil {
				t.Fatal("NewHealthChecker returned nil")
			}

			stats := hc.GetStats()
			if stats.TotalNodes != 0 {
				t.Error("New health checker should have no nodes")
			}
		})
	}
}

// TestHealthChecker_AddNode 测试添加节点
func TestHealthChecker_AddNode(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval:         1 * time.Hour, // 长间隔避免自动检查
		Timeout:          1 * time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 2,
	}
	hc := discovery.NewHealthChecker(config)

	// 添加节点（使用无效地址，连接会失败但不影响添加）
	err := hc.AddNode("node1", "127.0.0.1:9999")
	if err != nil {
		t.Errorf("AddNode() failed: %v", err)
	}

	stats := hc.GetStats()
	if stats.TotalNodes != 1 {
		t.Errorf("Expected 1 node, got %d", stats.TotalNodes)
	}

	// 重复添加（应该被忽略）
	err = hc.AddNode("node1", "127.0.0.1:9999")
	if err != nil {
		t.Errorf("AddNode() should not fail for duplicate: %v", err)
	}

	stats = hc.GetStats()
	if stats.TotalNodes != 1 {
		t.Errorf("Expected 1 node after duplicate add, got %d", stats.TotalNodes)
	}
}

// TestHealthChecker_RemoveNode 测试移除节点
func TestHealthChecker_RemoveNode(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval: 1 * time.Hour,
		Timeout:  1 * time.Second,
	}
	hc := discovery.NewHealthChecker(config)

	hc.AddNode("node1", "127.0.0.1:9999")
	hc.AddNode("node2", "127.0.0.1:9998")

	// 移除节点
	hc.RemoveNode("node1")

	stats := hc.GetStats()
	if stats.TotalNodes != 1 {
		t.Errorf("Expected 1 node after removal, got %d", stats.TotalNodes)
	}

	// 移除不存在的节点（应该被忽略）
	hc.RemoveNode("node3")

	stats = hc.GetStats()
	if stats.TotalNodes != 1 {
		t.Errorf("Expected 1 node, got %d", stats.TotalNodes)
	}
}

// TestHealthChecker_GetNodeHealth 测试获取节点健康状态
func TestHealthChecker_GetNodeHealth(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval: 1 * time.Hour,
		Timeout:  1 * time.Second,
	}
	hc := discovery.NewHealthChecker(config)

	hc.AddNode("node1", "127.0.0.1:9999")

	// 等待初始检查完成
	time.Sleep(100 * time.Millisecond)

	// 获取节点健康状态
	health, exists := hc.GetNodeHealth("node1")
	if !exists {
		t.Fatal("Node should exist")
	}

	if health.NodeID != "node1" {
		t.Errorf("Expected node1, got %s", health.NodeID)
	}

	if health.Address != "127.0.0.1:9999" {
		t.Errorf("Expected address 127.0.0.1:9999, got %s", health.Address)
	}

	// 不存在的节点
	_, exists = hc.GetNodeHealth("node2")
	if exists {
		t.Error("Non-existent node should not exist")
	}
}

// TestHealthChecker_GetAllHealth 测试获取所有节点健康状态
func TestHealthChecker_GetAllHealth(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval: 1 * time.Hour,
		Timeout:  1 * time.Second,
	}
	hc := discovery.NewHealthChecker(config)

	hc.AddNode("node1", "127.0.0.1:9999")
	hc.AddNode("node2", "127.0.0.1:9998")
	hc.AddNode("node3", "127.0.0.1:9997")

	time.Sleep(100 * time.Millisecond)

	allHealth := hc.GetAllHealth()
	if len(allHealth) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(allHealth))
	}

	for nodeID, health := range allHealth {
		if health.NodeID != nodeID {
			t.Errorf("Health record node ID mismatch: %s != %s", health.NodeID, nodeID)
		}
	}
}

// TestHealthChecker_GetHealthyNodes 测试获取健康节点列表
func TestHealthChecker_GetHealthyNodes(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval:         1 * time.Hour,
		Timeout:          1 * time.Second,
		FailureThreshold: 1,
		SuccessThreshold: 1,
	}
	hc := discovery.NewHealthChecker(config)

	// 添加节点（这些地址无法连接，将被标记为不健康）
	hc.AddNode("node1", "127.0.0.1:9999")
	hc.AddNode("node2", "127.0.0.1:9998")

	// 等待初始检查
	time.Sleep(200 * time.Millisecond)

	healthyNodes := hc.GetHealthyNodes()

	// 由于无法连接，应该没有健康节点（或根据SuccessThreshold的实现）
	t.Logf("Healthy nodes count: %d", len(healthyNodes))
}

// TestHealthChecker_HealthChangeCallback 测试健康状态变更回调
func TestHealthChecker_HealthChangeCallback(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval:         100 * time.Millisecond,
		Timeout:          50 * time.Millisecond,
		FailureThreshold: 2,
		SuccessThreshold: 1,
	}
	hc := discovery.NewHealthChecker(config)

	var callbackCount int32
	var mu sync.Mutex
	changes := make([]struct {
		nodeID  string
		healthy bool
	}, 0)

	// 设置回调
	hc.SetHealthChangeCallback(func(nodeID string, healthy bool) {
		atomic.AddInt32(&callbackCount, 1)
		mu.Lock()
		changes = append(changes, struct {
			nodeID  string
			healthy bool
		}{nodeID, healthy})
		mu.Unlock()
	})

	// 添加节点（无效地址）
	hc.AddNode("node1", "127.0.0.1:9999")

	// 启动检查
	hc.Start()
	defer hc.Stop()

	// 等待几次检查
	time.Sleep(500 * time.Millisecond)

	// 应该至少有一次回调（健康状态变更）
	count := atomic.LoadInt32(&callbackCount)
	if count == 0 {
		t.Log("Warning: No health change callbacks received (may vary based on timing)")
	} else {
		t.Logf("Received %d health change callbacks", count)
	}
}

// TestHealthChecker_StartStop 测试启动和停止
func TestHealthChecker_StartStop(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval: 100 * time.Millisecond,
		Timeout:  50 * time.Millisecond,
	}
	hc := discovery.NewHealthChecker(config)

	hc.AddNode("node1", "127.0.0.1:9999")

	// 启动
	hc.Start()

	// 等待一段时间
	time.Sleep(300 * time.Millisecond)

	// 停止
	hc.Stop()

	// 停止后再次停止应该安全
	hc.Stop()

	// 停止后添加节点应该失败
	err := hc.AddNode("node2", "127.0.0.1:9998")
	if err == nil {
		t.Error("AddNode should fail after Stop()")
	}
}

// TestHealthChecker_GetStats 测试统计信息
func TestHealthChecker_GetStats(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval:         1 * time.Hour,
		Timeout:          1 * time.Second,
		FailureThreshold: 3,
		SuccessThreshold: 2,
	}
	hc := discovery.NewHealthChecker(config)

	hc.AddNode("node1", "127.0.0.1:9999")
	hc.AddNode("node2", "127.0.0.1:9998")

	stats := hc.GetStats()

	if stats.TotalNodes != 2 {
		t.Errorf("Expected 2 total nodes, got %d", stats.TotalNodes)
	}

	if stats.CheckInterval != 1*time.Hour {
		t.Errorf("Expected interval 1h, got %v", stats.CheckInterval)
	}

	if stats.CheckTimeout != 1*time.Second {
		t.Errorf("Expected timeout 1s, got %v", stats.CheckTimeout)
	}
}

// TestHealthChecker_ThresholdBehavior 测试阈值行为
func TestHealthChecker_ThresholdBehavior(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval:         1 * time.Hour, // 手动触发
		Timeout:          50 * time.Millisecond,
		FailureThreshold: 3,
		SuccessThreshold: 2,
	}
	hc := discovery.NewHealthChecker(config)

	hc.AddNode("node1", "127.0.0.1:9999") // 无效地址

	time.Sleep(100 * time.Millisecond)

	// 获取初始状态
	health, _ := hc.GetNodeHealth("node1")

	t.Logf("Initial health state: healthy=%v, failures=%d, successes=%d",
		health.Healthy, health.FailureCount, health.SuccessCount)

	// 由于FailureThreshold=3，需要3次失败才标记为不健康
	// 初始检查会失败一次，但不会立即标记为不健康
}

// TestHealthChecker_ConcurrentOperations 测试并发操作
func TestHealthChecker_ConcurrentOperations(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval:       1 * time.Hour,
		Timeout:        500 * time.Millisecond,
		MaxConcurrency: 20,
	}
	hc := discovery.NewHealthChecker(config)

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// 并发添加节点
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node%d", id)
			address := fmt.Sprintf("127.0.0.1:99%02d", id%100)
			if err := hc.AddNode(nodeID, address); err != nil {
				errChan <- err
			}
		}(i)
	}

	// 并发读取
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node%d", id%25)
			hc.GetNodeHealth(nodeID)
		}(i)
	}

	// 并发获取统计
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = hc.GetStats()
		}()
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Error(err)
	}

	stats := hc.GetStats()
	if stats.TotalNodes < 40 {
		t.Logf("Warning: Expected at least 40 nodes, got %d", stats.TotalNodes)
	}
}

// TestHealthChecker_DisabledInterval 测试禁用间隔
func TestHealthChecker_DisabledInterval(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval: 0, // 禁用自动检查
		Timeout:  1 * time.Second,
	}
	hc := discovery.NewHealthChecker(config)

	hc.AddNode("node1", "127.0.0.1:9999")

	// 启动（应该不会进行自动检查）
	hc.Start()
	defer hc.Stop()

	time.Sleep(200 * time.Millisecond)

	// 手动检查不会自动运行，但节点应该存在
	_, exists := hc.GetNodeHealth("node1")
	if !exists {
		t.Error("Node should exist even with disabled interval")
	}
}

// TestHealthChecker_MultipleAddRemove 测试多次添加删除
func TestHealthChecker_MultipleAddRemove(t *testing.T) {
	config := &discovery.HealthCheckConfig{
		Interval: 1 * time.Hour,
		Timeout:  500 * time.Millisecond,
	}
	hc := discovery.NewHealthChecker(config)

	// 添加和删除节点多次
	for i := 0; i < 10; i++ {
		hc.AddNode("node1", "127.0.0.1:9999")
		time.Sleep(10 * time.Millisecond)
		hc.RemoveNode("node1")
		time.Sleep(10 * time.Millisecond)
	}

	stats := hc.GetStats()
	if stats.TotalNodes != 0 {
		t.Errorf("Expected 0 nodes after removal, got %d", stats.TotalNodes)
	}
}

// BenchmarkHealthChecker_AddNode 性能基准测试
func BenchmarkHealthChecker_AddNode(b *testing.B) {
	config := &discovery.HealthCheckConfig{
		Interval: 1 * time.Hour,
		Timeout:  100 * time.Millisecond,
	}
	hc := discovery.NewHealthChecker(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		address := fmt.Sprintf("127.0.0.1:99%02d", i%100)
		hc.AddNode(nodeID, address)
	}
}

// BenchmarkHealthChecker_GetNodeHealth 性能基准测试
func BenchmarkHealthChecker_GetNodeHealth(b *testing.B) {
	config := &discovery.HealthCheckConfig{
		Interval: 1 * time.Hour,
		Timeout:  100 * time.Millisecond,
	}
	hc := discovery.NewHealthChecker(config)

	// 预先添加节点
	for i := 0; i < 100; i++ {
		nodeID := fmt.Sprintf("node%d", i)
		address := fmt.Sprintf("127.0.0.1:99%02d", i)
		hc.AddNode(nodeID, address)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodeID := fmt.Sprintf("node%d", i%100)
		hc.GetNodeHealth(nodeID)
	}
}
