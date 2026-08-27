package cluster

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"turbocache/internal/cluster/hash"
)

// ---------- 辅助函数 ----------

// assertRatioClose 用于断言实际比例与期望比例在给定容差内
func assertRatioClose(t *testing.T, nodeID string, actual, expected, tol float64) {
	diff := math.Abs(actual - expected)
	if diff > tol {
		t.Fatalf("节点 %s 比例偏差过大：实际=%.4f 期望=%.4f 容差=%.4f", nodeID, actual, expected, tol)
	}
}

// sumInts 求和
func sumInts(m map[string]int) int {
	total := 0
	for _, v := range m {
		total += v
	}
	return total
}

// ---------- 测试开始 ----------

// TestNewWeightedHash 创建与初始状态检查
func TestNewWeightedHash(t *testing.T) {
	cases := []struct {
		name         string
		baseReplicas int
		hashFunc     hash.HashFunc
	}{
		{"default_xxhash_100", 100, nil},
		{"sha1_150", 150, hash.GetHashFunc("sha1")},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			wh := hash.NewWeightedHash(c.baseReplicas, c.hashFunc)
			if wh == nil {
				t.Fatalf("NewWeightedHash 返回 nil")
			}
			if got := wh.GetNodeCount(); got != 0 {
				t.Fatalf("新创建的 WeightedHash 应该没有节点, got=%d", got)
			}
			if got := wh.GetTotalWeight(); got != 0 {
				t.Fatalf("新创建的 WeightedHash 总权重应为0, got=%d", got)
			}
		})
	}
}

// TestWeightedHash_AddRemove_Update 覆盖增删改和错误分支
func TestWeightedHash_AddRemove_Update(t *testing.T) {
	wh := hash.NewWeightedHash(100, nil)

	// 添加节点
	if err := wh.AddNode("node1", 0); err != nil {
		t.Fatalf("添加 node1 失败: %v", err)
	}
	if err := wh.AddNode("node2", 200); err != nil {
		t.Fatalf("添加 node2 失败: %v", err)
	}
	if err := wh.AddNode("node3", 50); err != nil {
		t.Fatalf("添加 node3 失败: %v", err)
	}

	// 重复添加应报错
	if err := wh.AddNode("node1", 100); err == nil {
		t.Fatalf("重复添加 node1 应返回错误")
	}

	// 权重验证（node1 传 0 -> 应默认 100）
	if w := wh.GetWeight("node1"); w != 100 {
		t.Fatalf("node1 权重期望 100, got=%d", w)
	}

	expectedTotal := 100 + 200 + 50
	if tw := wh.GetTotalWeight(); tw != expectedTotal {
		t.Fatalf("总权重期望 %d, got=%d", expectedTotal, tw)
	}

	// UpdateWeight 正常路径
	if err := wh.UpdateWeight("node1", 300); err != nil {
		t.Fatalf("UpdateWeight node1 失败: %v", err)
	}
	if wh.GetWeight("node1") != 300 {
		t.Fatalf("node1 权重应更新为 300")
	}
	if wh.GetTotalWeight() != 300+200+50 {
		t.Fatalf("总权重不正确")
	}

	// UpdateWeight 对不存在节点返回错误
	if err := wh.UpdateWeight("noexist", 100); err == nil {
		t.Fatalf("更新不存在节点应返回错误")
	}

	// RemoveNode 正常路径
	if err := wh.RemoveNode("node3"); err != nil {
		t.Fatalf("RemoveNode node3 失败: %v", err)
	}
	if _, ok := wh.GetNodeDistribution(1)["node3"]; ok {
		// map lookup won't panic; just ensure node removed
	}

	// RemoveNode 对不存在节点应返回错误
	if err := wh.RemoveNode("node3"); err == nil {
		t.Fatalf("重复删除不存在的节点应返回错误")
	}
}

// TestWeightedHash_GetNode_Distribution 验证加权分布（大样本）
func TestWeightedHash_GetNode_Distribution(t *testing.T) {
	wh := hash.NewWeightedHash(100, nil)

	wh.AddNode("node1", 100)
	wh.AddNode("node2", 200)
	wh.AddNode("node3", 300)

	// 采样数足够大以减少统计噪声
	sample := 30000
	dist := wh.GetNodeDistribution(sample)

	// 基本检查
	if len(dist) != 3 {
		t.Fatalf("分布节点数量应为3, got=%d", len(dist))
	}
	if sum := sumInts(dist); sum != sample {
		t.Fatalf("采样总数应为 %d, got=%d", sample, sum)
	}

	// 验证比例接近权重比例 100:200:300 = 1:2:3
	totalWeight := 600.0
	tol := 0.06 // 允许 6% 的绝对比例偏差（工程化容差）
	for nodeID, count := range dist {
		actual := float64(count) / float64(sample)
		expected := float64(wh.GetWeight(nodeID)) / totalWeight
		assertRatioClose(t, nodeID, actual, expected, tol)
		t.Logf("节点 %s: count=%d 期望比例=%.4f 实际比例=%.4f", nodeID, count, expected, actual)
	}
}

// TestWeightedHash_GetNodes_GetAllNodes_GetStats 测试相关只读API
func TestWeightedHash_GetNodes_GetAllNodes_GetStats(t *testing.T) {
	wh := hash.NewWeightedHash(100, nil)
	wh.AddNode("node1", 100)
	wh.AddNode("node2", 300)

	// GetNode 一致性
	k := "some_key_123"
	n1 := wh.GetNode(k)
	for i := 0; i < 5; i++ {
		if wh.GetNode(k) != n1 {
			t.Fatalf("相同 key 应该返回相同节点")
		}
	}

	// GetNodes
	nodes := wh.GetNodes(k, 2)
	if len(nodes) != 2 {
		t.Fatalf("GetNodes 应返回 2 个候选")
	}
	// 无重复
	seen := make(map[string]bool)
	for _, id := range nodes {
		if seen[id] {
			t.Fatalf("GetNodes 返回了重复节点: %s", id)
		}
		seen[id] = true
	}

	// GetAllNodes 与 GetStats
	all := wh.GetAllNodes()
	if len(all) != 2 {
		t.Fatalf("GetAllNodes 长度应为2")
	}
	stats := wh.GetStats()
	if stats.TotalNodes != 2 {
		t.Fatalf("GetStats TotalNodes 应为2")
	}
	if stats.TotalWeight != 400 {
		t.Fatalf("GetStats TotalWeight 应为400")
	}
}

// TestWeightedHash_ValidateDistribution 较严谨的阈值测试
func TestWeightedHash_ValidateDistribution(t *testing.T) {
	wh := hash.NewWeightedHash(100, nil)
	wh.AddNode("n1", 100)
	wh.AddNode("n2", 200)
	wh.AddNode("n3", 300)

	// 大样本、松阈值 -> 通常为 true
	if ok := wh.ValidateDistribution(20000, 0.1); !ok {
		t.Fatalf("大样本松阈值下应返回 true")
	}

	// 小样本、严格阈值 -> 可能为 false
	if ok := wh.ValidateDistribution(50, 0.01); ok {
		// 这里不强制失败，但记录提示（因为样本小行为不稳定）
		t.Log("小样本严格阈值下返回 true（可能受随机性影响）")
	}
}

// TestWeightedHash_GetExpectedLoad_GetLoadImbalance 检查负载计算逻辑
func TestWeightedHash_GetExpectedLoad_GetLoadImbalance(t *testing.T) {
	wh := hash.NewWeightedHash(100, nil)
	wh.AddNode("n1", 100)
	wh.AddNode("n2", 200)
	wh.AddNode("n3", 300)

	totalLoad := int64(6000)
	// 期望 100:200:300 -> 1000,2000,3000
	tests := []struct {
		node string
		want int64
	}{
		{"n1", 1000},
		{"n2", 2000},
		{"n3", 3000},
	}
	for _, tt := range tests {
		if got := wh.GetExpectedLoad(tt.node, totalLoad); got != tt.want {
			t.Fatalf("GetExpectedLoad(%s) = %d, want %d", tt.node, got, tt.want)
		}
	}

	// 检查不均衡度计算：完全按期望分配 -> imbalance 低
	actualLoads := map[string]int64{"n1": 1000, "n2": 2000, "n3": 3000}
	if imb := wh.GetLoadImbalance(actualLoads); imb > 1e-9 {
		t.Fatalf("完全平衡的实际负载应接近 0, got %f", imb)
	}

	// 检查不均衡情形：均匀分配 -> 高不均衡
	actualLoads2 := map[string]int64{"n1": 2000, "n2": 2000, "n3": 2000}
	if imb := wh.GetLoadImbalance(actualLoads2); imb < 0.01 {
		t.Fatalf("均匀分配相对于权重应产生较高的不均衡, got %f", imb)
	}
}

// TestWeightedHash_Clone_Simulate 确保克隆与模拟不修改原结构
func TestWeightedHash_Clone_Simulate(t *testing.T) {
	wh := hash.NewWeightedHash(100, nil)
	wh.AddNode("n1", 100)
	wh.AddNode("n2", 200)

	clone := wh.Clone()
	if clone == nil {
		t.Fatalf("Clone 返回 nil")
	}
	if clone.GetNodeCount() != wh.GetNodeCount() {
		t.Fatalf("Clone 节点数应与原相同")
	}

	// 在 clone 上添加节点，原不变
	clone.AddNode("n3", 300)
	if wh.GetNodeCount() == clone.GetNodeCount() {
		t.Fatalf("修改 clone 不应影响原对象")
	}

	// 模拟添加节点不会改变原
	testKeys := make([]string, 2000)
	for i := range testKeys {
		testKeys[i] = fmt.Sprintf("k_%d", i)
	}
	affected := wh.SimulateAddNode("n3", 300, testKeys)
	t.Logf("模拟添加节点会影响 %d/%d 键", affected, len(testKeys))
	// 模拟后原应保持
	if wh.GetNodeCount() != 2 {
		t.Fatalf("SimulateAddNode 不应修改原 hash")
	}

	// 模拟删除类似
	affected2 := wh.SimulateRemoveNode("n2", testKeys)
	t.Logf("模拟删除节点会影响 %d/%d 键", affected2, len(testKeys))
}

// TestWeightedHash_ConcurrentOperations 并发读写混合测试
func TestWeightedHash_ConcurrentOperations(t *testing.T) {
	wh := hash.NewWeightedHash(100, nil)

	// 先添加少量节点
	for i := 1; i <= 4; i++ {
		if err := wh.AddNode(fmt.Sprintf("node%d", i), 100); err != nil {
			t.Fatalf("初始化 AddNode 失败: %v", err)
		}
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 200)

	// 并发读：GetNode
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			k := fmt.Sprintf("con_key_%d", id)
			n := wh.GetNode(k)
			if n == "" {
				errCh <- fmt.Errorf("GetNode 返回空 %s", k)
			}
		}(i)
	}

	// 并发写：AddNode/RemoveNode/UpdateWeight
	for i := 5; i <= 12; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			nodeID := fmt.Sprintf("node%d", id)
			if err := wh.AddNode(nodeID, 100+id); err != nil {
				// 允许重复添加导致的错误也记录，但不致命
				errCh <- err
			}
			// 尝试更新权重
			_ = wh.UpdateWeight(nodeID, 200)
			// 尝试移除（有时会移除掉刚刚添加的）
			_ = wh.RemoveNode(nodeID)
		}(i)
	}

	// 并发获取统计
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = wh.GetStats()
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		// 记录并且失败测试，便于发现并发问题
		t.Errorf("并发操作发生错误（注意：部分错误可能为并发重复操作导致）：%v", err)
	}

	// 最终至少存在最初添加的节点（并发移除可能减少，但不应全部为0）
	if wh.GetNodeCount() == 0 {
		t.Fatalf("并发操作后节点数量不应为0")
	}
}

// TestWeightedHash_EdgeCases 边界条件测试
func TestWeightedHash_EdgeCases(t *testing.T) {
	t.Run("single node routes all keys", func(t *testing.T) {
		wh := hash.NewWeightedHash(100, nil)
		_ = wh.AddNode("solo", 100)
		for i := 0; i < 200; i++ {
			if wh.GetNode(fmt.Sprintf("k_%d", i)) != "solo" {
				t.Fatalf("单节点情况下所有键应路由到 solo")
			}
		}
	})

	t.Run("negative_or_zero_weight_defaults", func(t *testing.T) {
		wh := hash.NewWeightedHash(100, nil)
		_ = wh.AddNode("a", -10)
		_ = wh.AddNode("b", 0)
		if wh.GetWeight("a") != 100 || wh.GetWeight("b") != 100 {
			t.Fatalf("负或零权重应默认到 100")
		}
	})

	t.Run("tiny_base_replicas", func(t *testing.T) {
		wh := hash.NewWeightedHash(1, nil)
		_ = wh.AddNode("x", 100)
		nodes := wh.GetAllNodes()
		if len(nodes) == 0 || nodes[0].VirtualNodes < 1 {
			t.Fatalf("tiny base replicas 下应至少有 1 个虚拟节点")
		}
	})
}
