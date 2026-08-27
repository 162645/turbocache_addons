package cluster

import (
	"fmt"
	"sync"
	"testing"

	"turbocache/internal/cluster/hash"
)

// TestNewVirtualNodeManager 测试创建虚拟节点管理器
func TestNewVirtualNodeManager(t *testing.T) {
	tests := []struct {
		name     string
		replicas int
		hashFunc hash.HashFunc
	}{
		{
			name:     "default hash function",
			replicas: 100,
			hashFunc: nil,
		},
		{
			name:     "custom hash function",
			replicas: 150,
			hashFunc: hash.GetHashFunc("fnv"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := hash.NewVirtualNodeManager(tt.replicas, tt.hashFunc)
			if m == nil {
				t.Fatal("NewVirtualNodeManager returned nil")
			}
			if m.GetNodeCount() != 0 {
				t.Error("New manager should have no nodes")
			}
		})
	}
}

// TestVirtualNodeManager_AddNode 测试添加节点
func TestVirtualNodeManager_AddNode(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	// 添加第一个节点
	vnodes := m.AddNode("node1")
	if len(vnodes) != 100 {
		t.Errorf("Expected 100 virtual nodes, got %d", len(vnodes))
	}

	if m.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node, got %d", m.GetNodeCount())
	}

	// 验证虚拟节点信息
	for i, vnode := range vnodes {
		if vnode.RealNode != "node1" {
			t.Errorf("Virtual node %d has wrong real node: %s", i, vnode.RealNode)
		}
		if vnode.Index != i {
			t.Errorf("Virtual node has wrong index: expected %d, got %d", i, vnode.Index)
		}
		expectedID := fmt.Sprintf("node1#%d", i)
		if vnode.ID != expectedID {
			t.Errorf("Virtual node has wrong ID: expected %s, got %s", expectedID, vnode.ID)
		}
	}

	// 重复添加（应该返回已存在的虚拟节点）
	vnodes2 := m.AddNode("node1")
	if len(vnodes2) != 100 {
		t.Errorf("Duplicate add should return existing vnodes, got %d", len(vnodes2))
	}
}

// TestVirtualNodeManager_RemoveNode 测试移除节点
func TestVirtualNodeManager_RemoveNode(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	m.AddNode("node1")
	m.AddNode("node2")

	// 移除节点
	removedVNodes := m.RemoveNode("node1")
	if len(removedVNodes) != 100 {
		t.Errorf("Expected 100 removed virtual nodes, got %d", len(removedVNodes))
	}

	if m.GetNodeCount() != 1 {
		t.Errorf("Expected 1 node after removal, got %d", m.GetNodeCount())
	}

	if m.GetVirtualNodeCount() != 100 {
		t.Errorf("Expected 100 virtual nodes after removal, got %d", m.GetVirtualNodeCount())
	}

	// 移除不存在的节点
	removedVNodes = m.RemoveNode("node3")
	if removedVNodes != nil {
		t.Error("Removing non-existent node should return nil")
	}
}

// TestVirtualNodeManager_GetNode 测试获取节点
func TestVirtualNodeManager_GetNode(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	// 空管理器
	if node := m.GetNode("test_key"); node != "" {
		t.Error("Empty manager should return empty string")
	}

	// 添加节点
	m.AddNode("node1")
	m.AddNode("node2")
	m.AddNode("node3")

	// 测试一致性
	key := "test_key_123"
	firstNode := m.GetNode(key)
	for i := 0; i < 10; i++ {
		node := m.GetNode(key)
		if node != firstNode {
			t.Errorf("Same key should always map to same node, got %s and %s", node, firstNode)
		}
	}

	// 测试键分布
	distribution := make(map[string]int)
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("key_%d", i)
		node := m.GetNode(key)
		distribution[node]++
	}

	// 应该分布到所有节点
	if len(distribution) != 3 {
		t.Errorf("Keys should distribute to all 3 nodes, got %d nodes", len(distribution))
	}

	// 验证分布相对均匀
	for nodeID, count := range distribution {
		expectedCount := 10000 / 3
		tolerance := float64(expectedCount) * 0.3
		diff := float64(count - expectedCount)
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("Node %s has unbalanced distribution: %d keys", nodeID, count)
		}
		t.Logf("Node %s: %d keys (%.1f%%)", nodeID, count, float64(count)/100)
	}
}

// TestVirtualNodeManager_GetNodes 测试获取多个候选节点
func TestVirtualNodeManager_GetNodes(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	// 空管理器
	if nodes := m.GetNodes("key", 3); nodes != nil {
		t.Error("Empty manager should return nil")
	}

	// 添加节点
	m.AddNode("node1")
	m.AddNode("node2")
	m.AddNode("node3")

	tests := []struct {
		name      string
		key       string
		n         int
		wantCount int
	}{
		{
			name:      "get 2 nodes",
			key:       "test_key",
			n:         2,
			wantCount: 2,
		},
		{
			name:      "get all nodes",
			key:       "test_key",
			n:         3,
			wantCount: 3,
		},
		{
			name:      "get more than available",
			key:       "test_key",
			n:         5,
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := m.GetNodes(tt.key, tt.n)
			if len(nodes) != tt.wantCount {
				t.Errorf("Expected %d nodes, got %d", tt.wantCount, len(nodes))
			}

			// 验证节点不重复
			seen := make(map[string]bool)
			for _, nodeID := range nodes {
				if seen[nodeID] {
					t.Errorf("Duplicate node: %s", nodeID)
				}
				seen[nodeID] = true
			}
		})
	}
}

// TestVirtualNodeManager_GetVirtualNodes 测试获取虚拟节点
func TestVirtualNodeManager_GetVirtualNodes(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	// 不存在的节点
	if vnodes := m.GetVirtualNodes("node1"); vnodes != nil {
		t.Error("Non-existent node should return nil")
	}

	// 添加节点
	m.AddNode("node1")
	vnodes := m.GetVirtualNodes("node1")

	if len(vnodes) != 100 {
		t.Errorf("Expected 100 virtual nodes, got %d", len(vnodes))
	}

	// 验证返回的是副本（修改不影响原数据）
	vnodes[0].RealNode = "modified"
	vnodes2 := m.GetVirtualNodes("node1")
	if vnodes2[0].RealNode == "modified" {
		t.Error("GetVirtualNodes should return a copy")
	}
}

// TestVirtualNodeManager_GetAllNodes 测试获取所有节点
func TestVirtualNodeManager_GetAllNodes(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	// 空管理器
	if nodes := m.GetAllNodes(); len(nodes) != 0 {
		t.Error("Empty manager should return empty slice")
	}

	// 添加节点
	m.AddNode("node1")
	m.AddNode("node2")
	m.AddNode("node3")

	nodes := m.GetAllNodes()
	if len(nodes) != 3 {
		t.Errorf("Expected 3 nodes, got %d", len(nodes))
	}

	// 验证包含所有节点
	nodeMap := make(map[string]bool)
	for _, nodeID := range nodes {
		nodeMap[nodeID] = true
	}

	for _, expected := range []string{"node1", "node2", "node3"} {
		if !nodeMap[expected] {
			t.Errorf("Missing node: %s", expected)
		}
	}
}

// TestVirtualNodeManager_GetLoadDistribution 测试负载分布
func TestVirtualNodeManager_GetLoadDistribution(t *testing.T) {
	m := hash.NewVirtualNodeManager(150, nil)

	m.AddNode("node1")
	m.AddNode("node2")

	distribution := m.GetLoadDistribution()
	if len(distribution) != 2 {
		t.Errorf("Expected 2 entries in distribution, got %d", len(distribution))
	}

	if distribution["node1"] != 150 {
		t.Errorf("Expected 150 virtual nodes for node1, got %d", distribution["node1"])
	}

	if distribution["node2"] != 150 {
		t.Errorf("Expected 150 virtual nodes for node2, got %d", distribution["node2"])
	}
}

// TestVirtualNodeManager_Rebalance 测试重新平衡
func TestVirtualNodeManager_Rebalance(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	m.AddNode("node1")
	m.AddNode("node2")

	// 记录重平衡前的节点分配
	key := "test_key"
	nodeBefore := m.GetNode(key)

	// 重平衡
	m.Rebalance()

	// 节点分配应该保持不变
	nodeAfter := m.GetNode(key)
	if nodeBefore != nodeAfter {
		t.Errorf("Node assignment changed after rebalance: %s -> %s", nodeBefore, nodeAfter)
	}

	// 验证虚拟节点数量不变
	if m.GetVirtualNodeCount() != 200 {
		t.Errorf("Expected 200 virtual nodes after rebalance, got %d", m.GetVirtualNodeCount())
	}
}

// TestVirtualNodeManager_Clear 测试清空
func TestVirtualNodeManager_Clear(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	m.AddNode("node1")
	m.AddNode("node2")

	m.Clear()

	if m.GetNodeCount() != 0 {
		t.Error("Node count should be 0 after Clear()")
	}

	if m.GetVirtualNodeCount() != 0 {
		t.Error("Virtual node count should be 0 after Clear()")
	}

	if node := m.GetNode("test_key"); node != "" {
		t.Error("GetNode should return empty after Clear()")
	}
}

// TestVirtualNodeManager_GetRingInfo 测试获取环信息
func TestVirtualNodeManager_GetRingInfo(t *testing.T) {
	m := hash.NewVirtualNodeManager(10, nil) // 使用少量虚拟节点便于测试

	// 空环
	if segments := m.GetRingInfo(); segments != nil {
		t.Error("Empty ring should return nil")
	}

	// 添加节点
	m.AddNode("node1")
	m.AddNode("node2")

	segments := m.GetRingInfo()
	if len(segments) != 20 { // 2个节点 * 10个虚拟节点
		t.Errorf("Expected 20 segments, got %d", len(segments))
	}

	// 验证环段信息
	totalRatio := 0.0
	for _, seg := range segments {
		if seg.SegmentRatio < 0 || seg.SegmentRatio > 1 {
			t.Errorf("Invalid segment ratio: %f", seg.SegmentRatio)
		}
		totalRatio += seg.SegmentRatio

		if seg.RealNode != "node1" && seg.RealNode != "node2" {
			t.Errorf("Invalid real node: %s", seg.RealNode)
		}
	}

	// 总比例应该接近1.0
	if totalRatio < 0.99 || totalRatio > 1.01 {
		t.Errorf("Total segment ratio should be ~1.0, got %f", totalRatio)
	}
}

// TestVirtualNodeManager_GetStats 测试统计信息
func TestVirtualNodeManager_GetStats(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	m.AddNode("node1")
	m.AddNode("node2")
	m.AddNode("node3")

	tests := []struct {
		name        string
		includeRing bool
	}{
		{
			name:        "stats without ring",
			includeRing: false,
		},
		{
			name:        "stats with ring",
			includeRing: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stats := m.GetStats(tt.includeRing)

			if stats.TotalNodes != 3 {
				t.Errorf("Expected 3 total nodes, got %d", stats.TotalNodes)
			}

			if stats.TotalVirtualNodes != 300 {
				t.Errorf("Expected 300 virtual nodes, got %d", stats.TotalVirtualNodes)
			}

			if stats.ReplicasPerNode != 100 {
				t.Errorf("Expected 100 replicas per node, got %d", stats.ReplicasPerNode)
			}

			if stats.Balance == nil {
				t.Error("Balance metrics should not be nil")
			} else {
				if stats.Balance.MaxLoad != 100 || stats.Balance.MinLoad != 100 {
					t.Errorf("Expected all nodes to have 100 vnodes, got max=%d, min=%d",
						stats.Balance.MaxLoad, stats.Balance.MinLoad)
				}
			}

			if tt.includeRing {
				if stats.RingSegments == nil || len(stats.RingSegments) != 300 {
					t.Error("Ring segments should be included when requested")
				}
			} else {
				if stats.RingSegments != nil {
					t.Error("Ring segments should not be included when not requested")
				}
			}
		})
	}
}

// TestVirtualNodeManager_NodeRemovalImpact 测试节点移除的影响
func TestVirtualNodeManager_NodeRemovalImpact(t *testing.T) {
	m := hash.NewVirtualNodeManager(150, nil)

	m.AddNode("node1")
	m.AddNode("node2")
	m.AddNode("node3")

	// 记录初始映射
	const numKeys = 10000
	initialMapping := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		initialMapping[key] = m.GetNode(key)
	}

	// 移除一个节点
	m.RemoveNode("node2")

	// 统计受影响的键
	affectedKeys := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		newNode := m.GetNode(key)
		if initialMapping[key] != newNode {
			affectedKeys++
		}
	}

	// 期望约1/3的键受影响
	expectedAffected := numKeys / 3
	tolerance := float64(expectedAffected) * 0.4

	diff := float64(affectedKeys - expectedAffected)
	if diff < 0 {
		diff = -diff
	}

	if diff > tolerance {
		t.Errorf("Too many keys affected: expected ~%d, got %d", expectedAffected, affectedKeys)
	}

	t.Logf("Affected keys: %d/%d (%.2f%%)", affectedKeys, numKeys,
		float64(affectedKeys)/float64(numKeys)*100)
}

// TestVirtualNodeManager_ConcurrentOperations 测试并发操作
func TestVirtualNodeManager_ConcurrentOperations(t *testing.T) {
	m := hash.NewVirtualNodeManager(100, nil)

	// 预先添加节点
	for i := 1; i <= 5; i++ {
		m.AddNode(fmt.Sprintf("node%d", i))
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// 并发读取
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key_%d", id)
			node := m.GetNode(key)
			if node == "" {
				errChan <- fmt.Errorf("GetNode returned empty string for %s", key)
			}
		}(i)
	}

	// 并发添加节点
	for i := 6; i <= 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			m.AddNode(fmt.Sprintf("node%d", id))
		}(i)
	}

	// 并发获取统计
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = m.GetStats(false)
		}()
	}

	wg.Wait()
	close(errChan)

	// 检查错误
	for err := range errChan {
		t.Error(err)
	}

	// 验证最终状态
	if m.GetNodeCount() != 10 {
		t.Errorf("Expected 10 nodes, got %d", m.GetNodeCount())
	}
}

// BenchmarkVirtualNodeManager_GetNode 性能基准测试
func BenchmarkVirtualNodeManager_GetNode(b *testing.B) {
	m := hash.NewVirtualNodeManager(150, nil)

	for i := 1; i <= 10; i++ {
		m.AddNode(fmt.Sprintf("node%d", i))
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m.GetNode(keys[i%len(keys)])
	}
}

// BenchmarkVirtualNodeManager_AddNode 性能基准测试
func BenchmarkVirtualNodeManager_AddNode(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := hash.NewVirtualNodeManager(150, nil)
		b.StartTimer()

		m.AddNode(fmt.Sprintf("node%d", i))
	}
}

// BenchmarkVirtualNodeManager_RemoveNode 性能基准测试
func BenchmarkVirtualNodeManager_RemoveNode(b *testing.B) {
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		b.StopTimer()
		m := hash.NewVirtualNodeManager(150, nil)
		for j := 0; j < 10; j++ {
			m.AddNode(fmt.Sprintf("node%d", j))
		}
		b.StartTimer()

		m.RemoveNode("node5")
	}
}
