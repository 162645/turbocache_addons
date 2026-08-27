package cluster

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"turbocache/internal/cluster/hash"
)

// TestNewConsistentHash 测试创建一致性哈希环
func TestNewConsistentHash(t *testing.T) {
	tests := []struct {
		name     string
		replicas int
		hashFunc hash.HashFunc
		wantNil  bool
	}{
		{
			name:     "default hash function",
			replicas: 100,
			hashFunc: nil,
			wantNil:  false,
		},
		{
			name:     "custom hash function",
			replicas: 150,
			hashFunc: hash.GetHashFunc("sha1"),
			wantNil:  false,
		},
		{
			name:     "zero replicas",
			replicas: 0,
			hashFunc: nil,
			wantNil:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := hash.NewConsistentHash(tt.replicas, tt.hashFunc)
			if (ch == nil) != tt.wantNil {
				t.Errorf("NewConsistentHash() = %v, wantNil %v", ch, tt.wantNil)
			}
			if ch != nil && ch.IsEmpty() != true {
				t.Error("New hash ring should be empty")
			}
		})
	}
}

// TestConsistentHash_AddNode 测试添加节点
func TestConsistentHash_AddNode(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	node1 := &hash.Node{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Weight:  1,
		Metadata: map[string]string{
			"region": "us-east",
		},
	}

	node2 := &hash.Node{
		ID:      "node2",
		Address: "127.0.0.1:8002",
		Weight:  2,
	}

	// 添加第一个节点
	ch.AddNode(node1)
	if ch.Size() != 1 {
		t.Errorf("Expected size 1, got %d", ch.Size())
	}

	// 添加第二个节点
	ch.AddNode(node2)
	if ch.Size() != 2 {
		t.Errorf("Expected size 2, got %d", ch.Size())
	}

	// 重复添加节点（应该被忽略）
	ch.AddNode(node1)
	if ch.Size() != 2 {
		t.Errorf("Expected size 2 after duplicate add, got %d", ch.Size())
	}

	// 验证虚拟节点数量
	stats := ch.Stats()
	totalVNodes := stats["total_virtual_nodes"].(int)
	expectedVNodes := 100*1 + 100*2 // node1权重1 + node2权重2
	if totalVNodes != expectedVNodes {
		t.Errorf("Expected %d virtual nodes, got %d", expectedVNodes, totalVNodes)
	}
}

// TestConsistentHash_RemoveNode 测试移除节点
func TestConsistentHash_RemoveNode(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	node1 := &hash.Node{ID: "node1", Address: "127.0.0.1:8001", Weight: 1}
	node2 := &hash.Node{ID: "node2", Address: "127.0.0.1:8002", Weight: 1}

	ch.AddNode(node1)
	ch.AddNode(node2)

	// 移除节点
	ch.RemoveNode("node1")
	if ch.Size() != 1 {
		t.Errorf("Expected size 1 after remove, got %d", ch.Size())
	}

	// 移除不存在的节点（应该被忽略）
	ch.RemoveNode("node3")
	if ch.Size() != 1 {
		t.Errorf("Expected size 1 after removing non-existent node, got %d", ch.Size())
	}

	// 验证节点确实被移除
	node := ch.GetNode("test_key")
	if node == nil || node.ID != "node2" {
		t.Error("Should route to node2 after node1 removed")
	}
}

// TestConsistentHash_GetNode 测试获取键对应的节点
func TestConsistentHash_GetNode(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	// 空环应该返回nil
	if node := ch.GetNode("key1"); node != nil {
		t.Error("Empty ring should return nil")
	}

	// 添加节点
	node1 := &hash.Node{ID: "node1", Address: "127.0.0.1:8001", Weight: 1}
	node2 := &hash.Node{ID: "node2", Address: "127.0.0.1:8002", Weight: 1}
	node3 := &hash.Node{ID: "node3", Address: "127.0.0.1:8003", Weight: 1}

	ch.AddNode(node1)
	ch.AddNode(node2)
	ch.AddNode(node3)

	// 测试相同键总是路由到相同节点
	key := "test_key_123"
	firstNode := ch.GetNode(key)
	for i := 0; i < 10; i++ {
		node := ch.GetNode(key)
		if node.ID != firstNode.ID {
			t.Errorf("Same key should route to same node, got %s and %s", node.ID, firstNode.ID)
		}
	}

	// 测试不同键的分布
	keyToNode := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		node := ch.GetNode(key)
		keyToNode[key] = node.ID
	}

	// 验证键分布到多个节点
	nodeDistrib := make(map[string]int)
	for _, nodeID := range keyToNode {
		nodeDistrib[nodeID]++
	}

	if len(nodeDistrib) < 3 {
		t.Errorf("Keys should be distributed across all nodes, got %d nodes", len(nodeDistrib))
	}
}

// TestConsistentHash_GetNodes 测试获取多个候选节点
func TestConsistentHash_GetNodes(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	node1 := &hash.Node{ID: "node1", Address: "127.0.0.1:8001", Weight: 1}
	node2 := &hash.Node{ID: "node2", Address: "127.0.0.1:8002", Weight: 1}
	node3 := &hash.Node{ID: "node3", Address: "127.0.0.1:8003", Weight: 1}

	ch.AddNode(node1)
	ch.AddNode(node2)
	ch.AddNode(node3)

	tests := []struct {
		name      string
		key       string
		count     int
		wantCount int
	}{
		{
			name:      "get 2 nodes",
			key:       "test_key",
			count:     2,
			wantCount: 2,
		},
		{
			name:      "get more nodes than available",
			key:       "test_key",
			count:     5,
			wantCount: 3, // 只有3个节点
		},
		{
			name:      "get zero nodes",
			key:       "test_key",
			count:     0,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nodes := ch.GetNodes(tt.key, tt.count)
			if len(nodes) != tt.wantCount {
				t.Errorf("GetNodes() returned %d nodes, want %d", len(nodes), tt.wantCount)
			}

			// 验证返回的节点不重复
			seen := make(map[string]bool)
			for _, node := range nodes {
				if seen[node.ID] {
					t.Errorf("Duplicate node ID: %s", node.ID)
				}
				seen[node.ID] = true
			}
		})
	}
}

// TestConsistentHash_HashDistribution 测试哈希分布均匀性
func TestConsistentHash_HashDistribution(t *testing.T) {
	ch := hash.NewConsistentHash(150, nil)

	// 添加5个节点
	for i := 1; i <= 5; i++ {
		node := &hash.Node{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
			Weight:  1,
		}
		ch.AddNode(node)
	}

	// 生成10000个键并统计分布
	const totalKeys = 10000
	distribution := make(map[string]int)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		node := ch.GetNode(key)
		distribution[node.ID]++
	}

	// 期望每个节点大约分配 totalKeys/5 个键
	expectedPerNode := totalKeys / 5
	tolerance := float64(expectedPerNode) * 0.3 // 允许30%偏差

	for nodeID, count := range distribution {
		diff := float64(count - expectedPerNode)
		if diff < 0 {
			diff = -diff
		}
		if diff > tolerance {
			t.Errorf("Node %s has unbalanced load: got %d keys, expected ~%d (±%.0f)",
				nodeID, count, expectedPerNode, tolerance)
		}
	}
}

// TestDiagnoseConsistentHash -- 诊断测试：比较 crc32 与 sha1 的分布差异并打印统计量（可直接运行）
// 说明：该测试仅使用导出 API，不依赖未导出字段，帮助排查哈希 / vnode 分布的问题。
func TestDiagnoseConsistentHash(t *testing.T) {
	const totalKeys = 10000
	nodeCount := 5
	replicas := 150

	// helper to build a ring with given hash func and return distribution + stats
	runAndCollect := func(hf hash.HashFunc) (map[string]int, map[string]interface{}) {
		ch := hash.NewConsistentHash(replicas, hf)
		for i := 1; i <= nodeCount; i++ {
			node := &hash.Node{
				ID:      fmt.Sprintf("node%d", i),
				Address: fmt.Sprintf("127.0.0.1:800%d", i),
				Weight:  1,
			}
			ch.AddNode(node)
		}

		dist := make(map[string]int)
		for i := 0; i < totalKeys; i++ {
			key := fmt.Sprintf("key_%d", i)
			node := ch.GetNode(key)
			if node != nil {
				dist[node.ID]++
			}
		}

		stats := ch.Stats()
		return dist, stats
	}

	distCRC, statsCRC := runAndCollect(nil) // 默认 crc32
	distSHA1, statsSHA1 := runAndCollect(hash.GetHashFunc("sha1"))

	// 计算并打印统计量
	calcStats := func(dist map[string]int) (avg, stddev float64, min, max int) {
		min = math.MaxInt32
		max = 0
		sum := 0
		for _, v := range dist {
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
			sum += v
		}
		avg = float64(sum) / float64(len(dist))
		variance := 0.0
		for _, v := range dist {
			diff := float64(v) - avg
			variance += diff * diff
		}
		variance /= float64(len(dist))
		stddev = math.Sqrt(variance)
		return
	}

	avgCRC, stdCRC, minCRC, maxCRC := calcStats(distCRC)
	avgSHA, stdSHA, minSHA, maxSHA := calcStats(distSHA1)

	t.Logf("CRC32: total_virtual_nodes=%v, total_nodes=%v", statsCRC["total_virtual_nodes"], statsCRC["total_nodes"])
	t.Logf("CRC32 distribution: avg=%.2f std=%.2f min=%d max=%d -> %v", avgCRC, stdCRC, minCRC, maxCRC, distCRC)

	t.Logf("SHA1: total_virtual_nodes=%v, total_nodes=%v", statsSHA1["total_virtual_nodes"], statsSHA1["total_nodes"])
	t.Logf("SHA1 distribution: avg=%.2f std=%.2f min=%d max=%d -> %v", avgSHA, stdSHA, minSHA, maxSHA, distSHA1)

	// 简单建议输出：如果 sha1 stddev 小很多，说明是哈希选择导致的偏差
	if stdSHA < stdCRC {
		t.Log("SHA1 gives a lower stddev than CRC32 -> consider switching hash function for better distribution")
	} else {
		t.Log("CRC32 stddev <= SHA1 stddev -> distribution issue may be in vnode generation or replicas")
	}
}

// TestConsistentHash_NodeFailover 测试节点故障转移
func TestConsistentHash_NodeFailover(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	node1 := &hash.Node{ID: "node1", Address: "127.0.0.1:8001", Weight: 1}
	node2 := &hash.Node{ID: "node2", Address: "127.0.0.1:8002", Weight: 1}
	node3 := &hash.Node{ID: "node3", Address: "127.0.0.1:8003", Weight: 1}

	ch.AddNode(node1)
	ch.AddNode(node2)
	ch.AddNode(node3)

	// 记录所有键的初始分配
	const numKeys = 1000
	initialMapping := make(map[string]string)
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		node := ch.GetNode(key)
		initialMapping[key] = node.ID
	}

	// 移除一个节点
	ch.RemoveNode("node2")

	// 统计受影响的键
	affectedCount := 0
	for i := 0; i < numKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		newNode := ch.GetNode(key)
		if initialMapping[key] != newNode.ID {
			affectedCount++
		}
	}

	// 理论上应该只有约1/3的键受影响（原本分配到node2的键）
	expectedAffected := numKeys / 3
	tolerance := float64(expectedAffected) * 0.5 // 允许50%偏差

	diff := float64(affectedCount - expectedAffected)
	if diff < 0 {
		diff = -diff
	}
	if diff > tolerance {
		t.Errorf("Too many keys affected by node removal: got %d, expected ~%d",
			affectedCount, expectedAffected)
	}

	t.Logf("Node removal affected %d/%d keys (%.2f%%)",
		affectedCount, numKeys, float64(affectedCount)/float64(numKeys)*100)
}

// TestConsistentHash_UpdateNode 测试更新节点
func TestConsistentHash_UpdateNode(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	node := &hash.Node{
		ID:       "node1",
		Address:  "127.0.0.1:8001",
		Weight:   1,
		Metadata: map[string]string{"version": "1.0"},
	}

	ch.AddNode(node)

	// 更新元数据（不改变权重）
	updatedNode := &hash.Node{
		ID:       "node1",
		Address:  "127.0.0.1:8001",
		Weight:   1,
		Metadata: map[string]string{"version": "2.0"},
	}
	ch.UpdateNode(updatedNode)

	// 验证元数据已更新
	nodes := ch.GetAllNodes()
	if nodes["node1"].Metadata["version"] != "2.0" {
		t.Error("Node metadata should be updated")
	}

	// 更新权重（应该重建虚拟节点）
	updatedNode2 := &hash.Node{
		ID:      "node1",
		Address: "127.0.0.1:8001",
		Weight:  3,
	}
	ch.UpdateNode(updatedNode2)

	stats := ch.Stats()
	expectedVNodes := 100 * 3
	actualVNodes := stats["total_virtual_nodes"].(int)
	if actualVNodes != expectedVNodes {
		t.Errorf("Expected %d virtual nodes after weight update, got %d",
			expectedVNodes, actualVNodes)
	}
}

// TestConsistentHash_ConcurrentOperations 测试并发操作
func TestConsistentHash_ConcurrentOperations(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	// 预先添加一些节点
	for i := 1; i <= 5; i++ {
		node := &hash.Node{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
			Weight:  1,
		}
		ch.AddNode(node)
	}

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// 并发读取
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key_%d", id)
			node := ch.GetNode(key)
			if node == nil {
				errChan <- fmt.Errorf("GetNode returned nil for key %s", key)
			}
		}(i)
	}

	// 并发添加节点
	for i := 6; i <= 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			node := &hash.Node{
				ID:      fmt.Sprintf("node%d", id),
				Address: fmt.Sprintf("127.0.0.1:800%d", id),
				Weight:  1,
			}
			ch.AddNode(node)
		}(i)
	}

	// 并发获取统计信息
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = ch.Stats()
		}()
	}

	wg.Wait()
	close(errChan)

	// 检查错误
	for err := range errChan {
		t.Error(err)
	}

	// 验证最终状态
	if ch.Size() != 10 {
		t.Errorf("Expected 10 nodes after concurrent operations, got %d", ch.Size())
	}
}

// TestConsistentHash_Clear 测试清空哈希环
func TestConsistentHash_Clear(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	// 添加节点
	for i := 1; i <= 3; i++ {
		node := &hash.Node{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
			Weight:  1,
		}
		ch.AddNode(node)
	}

	// 清空
	ch.Clear()

	if !ch.IsEmpty() {
		t.Error("Hash ring should be empty after Clear()")
	}

	if ch.Size() != 0 {
		t.Errorf("Expected size 0 after Clear(), got %d", ch.Size())
	}

	stats := ch.Stats()
	if stats["total_virtual_nodes"].(int) != 0 {
		t.Error("Should have no virtual nodes after Clear()")
	}
}

// TestConsistentHash_GetBalance 测试负载均衡度
func TestConsistentHash_GetBalance(t *testing.T) {
	ch := hash.NewConsistentHash(150, nil)

	// 空环
	if balance := ch.GetBalance(); balance != 0 {
		t.Errorf("Empty ring should have balance 0, got %f", balance)
	}

	// 添加节点
	for i := 1; i <= 3; i++ {
		node := &hash.Node{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
			Weight:  1,
		}
		ch.AddNode(node)
	}

	// 生成一些键来产生分布
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		ch.GetNode(key)
	}

	balance := ch.GetBalance()
	if balance < 0 || balance > 1 {
		t.Errorf("Balance should be between 0 and 1, got %f", balance)
	}

	// 良好的哈希应该有较高的平衡度（>0.7）
	if balance < 0.5 {
		t.Logf("Warning: Low balance detected: %f", balance)
	}
}

// TestConsistentHash_WeightedNodes 测试加权节点
func TestConsistentHash_WeightedNodes(t *testing.T) {
	ch := hash.NewConsistentHash(100, nil)

	// 添加不同权重的节点
	node1 := &hash.Node{ID: "node1", Address: "127.0.0.1:8001", Weight: 1}
	node2 := &hash.Node{ID: "node2", Address: "127.0.0.1:8002", Weight: 3}
	node3 := &hash.Node{ID: "node3", Address: "127.0.0.1:8003", Weight: 5}

	ch.AddNode(node1)
	ch.AddNode(node2)
	ch.AddNode(node3)

	// 生成键并统计分布
	const totalKeys = 10000
	distribution := make(map[string]int)

	for i := 0; i < totalKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		node := ch.GetNode(key)
		distribution[node.ID]++
	}

	// 验证分布大致符合权重比例 1:3:5
	totalWeight := 1 + 3 + 5
	for nodeID, count := range distribution {
		var expectedWeight int
		switch nodeID {
		case "node1":
			expectedWeight = 1
		case "node2":
			expectedWeight = 3
		case "node3":
			expectedWeight = 5
		}

		expectedCount := totalKeys * expectedWeight / totalWeight
		tolerance := float64(expectedCount) * 0.4 // 允许40%偏差

		diff := float64(count - expectedCount)
		if diff < 0 {
			diff = -diff
		}

		if diff > tolerance {
			t.Errorf("Node %s: expected ~%d keys, got %d (weight=%d)",
				nodeID, expectedCount, count, expectedWeight)
		}

		t.Logf("Node %s (weight=%d): %d keys (%.1f%%)",
			nodeID, expectedWeight, count, float64(count)/float64(totalKeys)*100)
	}
}

// BenchmarkConsistentHash_GetNode 性能基准测试
func BenchmarkConsistentHash_GetNode(b *testing.B) {
	ch := hash.NewConsistentHash(150, nil)

	for i := 1; i <= 10; i++ {
		node := &hash.Node{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
			Weight:  1,
		}
		ch.AddNode(node)
	}

	keys := make([]string, 1000)
	for i := range keys {
		keys[i] = fmt.Sprintf("key_%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ch.GetNode(keys[i%len(keys)])
	}
}

// BenchmarkConsistentHash_GetNodes 性能基准测试
func BenchmarkConsistentHash_GetNodes(b *testing.B) {
	ch := hash.NewConsistentHash(150, nil)

	for i := 1; i <= 10; i++ {
		node := &hash.Node{
			ID:      fmt.Sprintf("node%d", i),
			Address: fmt.Sprintf("127.0.0.1:800%d", i),
			Weight:  1,
		}
		ch.AddNode(node)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		ch.GetNodes(key, 3)
	}
}
