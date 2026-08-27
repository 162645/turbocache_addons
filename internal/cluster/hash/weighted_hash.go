package hash

import (
	"fmt"
	"sort"
	"sync"
)

// WeightedHash 加权一致性哈希（完整实现）
// 说明：
// - 默认哈希函数若未传入，将在 NewWeightedHash 中使用 xxhashHash32（包内推荐的高性能哈希）
// - vnode 管理使用 VirtualNodeManager，接口保持不变
type WeightedHash struct {
	mu sync.RWMutex

	// 用于序列化对 vnodeManager 可变字段 / 增删操作，避免与 h.mu 同时持有导致死锁
	vnodeMu sync.Mutex

	// 哈希函数（如果需要外部自定义，可在创建时传入）
	hashFunc HashFunc

	// 节点权重
	weights map[string]int

	// 虚拟节点管理器（每个真实节点根据权重创建不同数量的虚拟节点）
	vnodeManager *VirtualNodeManager

	// 基础虚拟节点数（权重为100的节点的虚拟节点数）
	baseReplicas int

	// 节点总权重（用于统计与期望比例计算）
	totalWeight int
}

// WeightedNode 用于返回每个节点的统计信息
type WeightedNode struct {
	ID            string  `json:"id"`
	Weight        int     `json:"weight"`
	VirtualNodes  int     `json:"virtual_nodes"`
	ExpectedRatio float64 `json:"expected_ratio"`
	ActualRatio   float64 `json:"actual_ratio"`
}

// NewWeightedHash 创建加权一致性哈希
// baseReplicas: 基础副本数（权重为100时对应的虚拟节点数）
// hashFunc: 可选，自定义哈希函数；若为 nil 则使用包内推荐的 xxhashHash32
func NewWeightedHash(baseReplicas int, hashFunc HashFunc) *WeightedHash {
	if hashFunc == nil {
		hashFunc = xxhashHash32
	}

	return &WeightedHash{
		hashFunc:     hashFunc,
		weights:      make(map[string]int),
		vnodeManager: NewVirtualNodeManager(0, hashFunc), // 初始 replicas = 0，添加时临时设置
		baseReplicas: baseReplicas,
	}
}

// AddNode 添加节点（带权重）
// weight 的语义：相对权重，100 表示基准权重。
// 返回 error 表示节点已存在。
func (h *WeightedHash) AddNode(nodeID string, weight int) error {
	// 规范权重
	if weight <= 0 {
		weight = 100 // 默认权重
	}

	// 先在 h.mu 内检查并更新 weights/totalWeight（只保护 h 自身字段）
	h.mu.Lock()
	if _, exists := h.weights[nodeID]; exists {
		h.mu.Unlock()
		return fmt.Errorf("node %s already exists", nodeID)
	}
	h.weights[nodeID] = weight
	h.totalWeight += weight
	// 计算需要的 replicas（基于快照）
	replicas := (h.baseReplicas * weight) / 100
	if replicas < 1 {
		replicas = 1
	}
	h.mu.Unlock()

	// 在锁外对 vnodeManager 做变更，但序列化 vnodeManager 的修改以避免并发修改其内部可变字段
	h.vnodeMu.Lock()
	// 暂时修改 vnodeManager.replicas，然后添加节点（与原实现兼容）
	oldReplicas := h.vnodeManager.replicas
	h.vnodeManager.replicas = replicas
	h.vnodeManager.AddNode(nodeID)
	h.vnodeManager.replicas = oldReplicas
	h.vnodeMu.Unlock()

	return nil
}

// RemoveNode 移除节点
func (h *WeightedHash) RemoveNode(nodeID string) error {
	// 先在 h.mu 内检查并更新 weights/totalWeight（只保护 h 自身字段）
	h.mu.Lock()
	weight, exists := h.weights[nodeID]
	if !exists {
		h.mu.Unlock()
		return fmt.Errorf("node %s not found", nodeID)
	}
	// 更新权重记录
	delete(h.weights, nodeID)
	h.totalWeight -= weight
	h.mu.Unlock()

	// 在锁外移除 vnodeManager 中节点，同时序列化 vnodeManager 操作
	h.vnodeMu.Lock()
	h.vnodeManager.RemoveNode(nodeID)
	h.vnodeMu.Unlock()

	return nil
}

// UpdateWeight 更新节点权重（先移除旧的 vnode，再用新权重重建）
// 如果节点不存在返回 error
func (h *WeightedHash) UpdateWeight(nodeID string, newWeight int) error {
	if newWeight <= 0 {
		newWeight = 100
	}

	// 在 h.mu 内读取并更新 weights/totalWeight（但不要在持有 h.mu 时直接修改 vnodeManager）
	h.mu.Lock()
	oldWeight, exists := h.weights[nodeID]
	if !exists {
		h.mu.Unlock()
		return fmt.Errorf("node %s not found", nodeID)
	}
	// 更新 h 的权重记录（先更新内存结构，具体 vnode 重建放到锁外）
	h.weights[nodeID] = newWeight
	h.totalWeight = h.totalWeight - oldWeight + newWeight
	replicas := (h.baseReplicas * newWeight) / 100
	if replicas < 1 {
		replicas = 1
	}
	h.mu.Unlock()

	// 在锁外对 vnodeManager 做实际重建（先移除再设置 replicas 并添加）
	h.vnodeMu.Lock()
	h.vnodeManager.RemoveNode(nodeID)
	oldReplicas := h.vnodeManager.replicas
	h.vnodeManager.replicas = replicas
	h.vnodeManager.AddNode(nodeID)
	h.vnodeManager.replicas = oldReplicas
	h.vnodeMu.Unlock()

	return nil
}

// GetNode 根据键获取对应的真实节点 ID
func (h *WeightedHash) GetNode(key string) string {
	h.mu.RLock()
	// 快速读快照（为了保证一致性，GetNode 直接委托 vnodeManager 查询；此处只保护 h 的元数据）
	// 这里不需要做复杂的 snapshot，因为 vnodeManager.GetNode 是并发安全（假设）
	h.mu.RUnlock()
	return h.vnodeManager.GetNode(key)
}

// GetNodes 获取键的 n 个候选真实节点（用于副本/备用）
func (h *WeightedHash) GetNodes(key string, n int) []string {
	h.mu.RLock()
	h.mu.RUnlock()
	return h.vnodeManager.GetNodes(key, n)
}

// GetWeight 获取指定节点的权重（若不存在，返回 0）
func (h *WeightedHash) GetWeight(nodeID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.weights[nodeID]
}

// GetAllNodes 返回所有节点的统计信息（按 ID 排序）
// 改写为：先对 h 的字段做快照，释放锁；在锁外调用 vnodeManager 查询，避免锁嵌套导致死锁
func (h *WeightedHash) GetAllNodes() []*WeightedNode {
	// 快速快照 weights/totalWeight（不调用 vnodeManager）
	h.mu.RLock()
	weightsCopy := make(map[string]int, len(h.weights))
	for id, w := range h.weights {
		weightsCopy[id] = w
	}
	totalWeight := h.totalWeight
	h.mu.RUnlock()

	// 在锁外安全调用 vnodeManager（vnodeManager 内部自行同步）
	totalVNodes := h.vnodeManager.GetVirtualNodeCount()

	nodes := make([]*WeightedNode, 0, len(weightsCopy))
	for nodeID, weight := range weightsCopy {
		vnodes := h.vnodeManager.GetVirtualNodes(nodeID)
		expectedRatio := 0.0
		if totalWeight > 0 {
			expectedRatio = float64(weight) / float64(totalWeight)
		}
		actualRatio := 0.0
		if totalVNodes > 0 {
			actualRatio = float64(len(vnodes)) / float64(totalVNodes)
		}

		nodes = append(nodes, &WeightedNode{
			ID:            nodeID,
			Weight:        weight,
			VirtualNodes:  len(vnodes),
			ExpectedRatio: expectedRatio,
			ActualRatio:   actualRatio,
		})
	}

	// 按 ID 排序，保证稳定输出
	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	return nodes
}

// GetNodeCount 返回当前节点数量
func (h *WeightedHash) GetNodeCount() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.weights)
}

// GetTotalWeight 返回当前总权重
func (h *WeightedHash) GetTotalWeight() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.totalWeight
}

// Rebalance 重新平衡（重建 ring 数据结构）
// 该方法会调用 vnodeManager.Rebalance()
func (h *WeightedHash) Rebalance() {
	// 为避免死锁：不要在持有 h.mu 时调用 vnodeManager.Rebalance（vnodeManager 内部同步）
	h.mu.Lock()
	// 这里只可以做与 h 相关的准备（目前无）
	h.mu.Unlock()

	h.vnodeMu.Lock()
	h.vnodeManager.Rebalance()
	h.vnodeMu.Unlock()
}

// Clear 清空所有节点和权重信息
func (h *WeightedHash) Clear() {
	// 清理 h 自身数据
	h.mu.Lock()
	h.weights = make(map[string]int)
	h.totalWeight = 0
	h.mu.Unlock()

	// 清理 vnode 管理器
	h.vnodeMu.Lock()
	h.vnodeManager.Clear()
	h.vnodeMu.Unlock()
}

// GetStats 返回综合统计信息（用于监控/调试）
// 改写：先做快照，释放锁，再在锁外调用计算涉及 vnodeManager 的部分
func (h *WeightedHash) GetStats() *WeightedStats {
	// 快速快照需要的结构
	h.mu.RLock()
	weightsCopy := make(map[string]int, len(h.weights))
	for k, v := range h.weights {
		weightsCopy[k] = v
	}
	totalWeight := h.totalWeight
	baseReplicas := h.baseReplicas
	totalNodes := len(h.weights)
	h.mu.RUnlock()

	// 在锁外使用 vnodeManager 查询（避免持有 h.mu 调用 vnodeManager 导致的锁顺序冲突）
	stats := &WeightedStats{
		TotalNodes:   totalNodes,
		TotalWeight:  totalWeight,
		TotalVNodes:  h.vnodeManager.GetVirtualNodeCount(),
		BaseReplicas: baseReplicas,
		Nodes:        make([]*WeightedNode, 0, len(weightsCopy)),
	}

	// 生成 Nodes（复用 GetAllNodes）
	stats.Nodes = h.GetAllNodes()

	// 计算权重方差/系数需要用到 weights，但不需要持锁（我们用快照）
	if len(weightsCopy) > 0 {
		avgWeight := float64(totalWeight) / float64(len(weightsCopy))
		var variance float64
		for _, w := range weightsCopy {
			diff := float64(w) - avgWeight
			variance += diff * diff
		}
		stats.WeightStdDev = variance / float64(len(weightsCopy))
		if avgWeight > 0 {
			stats.WeightCoefficient = stats.WeightStdDev / avgWeight
		}
	}

	// 使用基于 snapshot 的 distribution deviation 计算（在锁外调用 vnodeManager）
	stats.DistributionDeviation = h.calculateDistributionDeviationFromSnapshot(weightsCopy, totalWeight)

	return stats
}

// WeightedStats 加权哈希统计信息结构体
type WeightedStats struct {
	TotalNodes            int             `json:"total_nodes"`
	TotalWeight           int             `json:"total_weight"`
	TotalVNodes           int             `json:"total_vnodes"`
	BaseReplicas          int             `json:"base_replicas"`
	WeightStdDev          float64         `json:"weight_std_dev"`
	WeightCoefficient     float64         `json:"weight_coefficient"`
	DistributionDeviation float64         `json:"distribution_deviation"`
	Nodes                 []*WeightedNode `json:"nodes"`
}

// calculateDistributionDeviationFromSnapshot 计算实际分布与期望分布的 MSE（均方差）
// 注：该函数不获取 h.mu，而是以传入的快照（weights, totalWeight）为准，在锁外安全调用 vnodeManager
func (h *WeightedHash) calculateDistributionDeviationFromSnapshot(weights map[string]int, totalWeight int) float64 {
	if len(weights) == 0 || totalWeight == 0 {
		return 0.0
	}

	totalVNodes := h.vnodeManager.GetVirtualNodeCount()
	if totalVNodes == 0 {
		return 0.0
	}

	var deviation float64
	for nodeID, weight := range weights {
		expectedRatio := float64(weight) / float64(totalWeight)
		vnodes := h.vnodeManager.GetVirtualNodes(nodeID)
		actualRatio := float64(len(vnodes)) / float64(totalVNodes)
		diff := actualRatio - expectedRatio
		deviation += diff * diff
	}

	return deviation / float64(len(weights))
}

// GetNodeDistribution 采样得到键落在各真实节点的分布（用于测试/诊断）
// sampleSize: 生成 sampleSize 个 test_key_i 进行统计
// 改写：只对 h 的字段做快照，然后在锁外调用 vnodeManager.GetNode
func (h *WeightedHash) GetNodeDistribution(sampleSize int) map[string]int {
	h.mu.RLock()
	weightsExist := len(h.weights) > 0
	h.mu.RUnlock()

	distribution := make(map[string]int)
	if !weightsExist {
		return distribution
	}

	for i := 0; i < sampleSize; i++ {
		key := fmt.Sprintf("test_key_%d", i)
		node := h.vnodeManager.GetNode(key)
		if node != "" {
			distribution[node]++
		}
	}
	return distribution
}

// GetNodeDistributionRatio 返回采样分布的比例（0..1）
func (h *WeightedHash) GetNodeDistributionRatio(sampleSize int) map[string]float64 {
	distribution := h.GetNodeDistribution(sampleSize)
	ratio := make(map[string]float64)
	for nodeID, count := range distribution {
		ratio[nodeID] = float64(count) / float64(sampleSize)
	}
	return ratio
}

// ValidateDistribution 验证分布是否在阈值内（用于自动化检测）
// threshold: 比例偏差阈值，例如 0.1 表示允许 10% 的绝对偏差
// 改写为：先 snapshot weights/totalWeight，再在锁外查询分布
func (h *WeightedHash) ValidateDistribution(sampleSize int, threshold float64) bool {
	// 拷贝快照
	h.mu.RLock()
	totalWeight := h.totalWeight
	weightsCopy := make(map[string]int, len(h.weights))
	for k, v := range h.weights {
		weightsCopy[k] = v
	}
	h.mu.RUnlock()

	if totalWeight == 0 {
		return true
	}
	if len(weightsCopy) == 0 {
		return true
	}

	distribution := h.GetNodeDistribution(sampleSize)

	for nodeID, weight := range weightsCopy {
		expectedRatio := float64(weight) / float64(totalWeight)
		actualCount := distribution[nodeID]
		actualRatio := float64(actualCount) / float64(sampleSize)

		deviation := actualRatio - expectedRatio
		if deviation < 0 {
			deviation = -deviation
		}

		if deviation > threshold {
			return false
		}
	}

	return true
}

// GetExpectedLoad 根据总负载计算某节点的期望负载
func (h *WeightedHash) GetExpectedLoad(nodeID string, totalLoad int64) int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	weight, exists := h.weights[nodeID]
	if !exists || h.totalWeight == 0 {
		return 0
	}
	return (totalLoad * int64(weight)) / int64(h.totalWeight)
}

// GetLoadImbalance 计算实际负载与期望负载的失衡指标（均方相对偏差）
func (h *WeightedHash) GetLoadImbalance(actualLoads map[string]int64) float64 {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if len(actualLoads) == 0 {
		return 0.0
	}

	var totalLoad int64
	for _, l := range actualLoads {
		totalLoad += l
	}
	if totalLoad == 0 {
		return 0.0
	}

	var imbalance float64
	for nodeID, actual := range actualLoads {
		expected := h.GetExpectedLoad(nodeID, totalLoad)
		if expected > 0 {
			diff := float64(actual-expected) / float64(expected)
			imbalance += diff * diff
		}
	}
	return imbalance / float64(len(actualLoads))
}

// SuggestRebalance 根据实际负载判断是否需要重新平衡（阈值由调用方决定）
func (h *WeightedHash) SuggestRebalance(actualLoads map[string]int64, threshold float64) bool {
	imbalance := h.GetLoadImbalance(actualLoads)
	return imbalance > threshold
}

// Clone 克隆当前 WeightedHash（用于模拟/测试，不共享可变状态）
// 改写：先做 snapshot（h 的字段），然后在锁外构建 clone 的 vnode（避免持有 h.mu 时调用 vnodeManager）
func (h *WeightedHash) Clone() *WeightedHash {
	h.mu.RLock()
	baseReplicas := h.baseReplicas
	hashFunc := h.hashFunc
	totalWeight := h.totalWeight
	weightsCopy := make(map[string]int, len(h.weights))
	for nodeID, w := range h.weights {
		weightsCopy[nodeID] = w
	}
	h.mu.RUnlock()

	clone := NewWeightedHash(baseReplicas, hashFunc)
	clone.totalWeight = totalWeight

	// 在 clone 上添加虚拟节点（clone 的内部同步管理自身 vnodeManager）
	for nodeID, w := range weightsCopy {
		_ = clone.AddNode(nodeID, w)
	}

	return clone
}

// SimulateAddNode 在克隆上模拟添加节点，返回受影响的键数（用于评估影响）
func (h *WeightedHash) SimulateAddNode(nodeID string, weight int, testKeys []string) int {
	clone := h.Clone()

	// 记录原映射
	oldMapping := make(map[string]string)
	for _, k := range testKeys {
		oldMapping[k] = h.GetNode(k)
	}

	// 在克隆上添加节点
	clone.AddNode(nodeID, weight)

	// 统计受影响键
	affected := 0
	for _, k := range testKeys {
		if clone.GetNode(k) != oldMapping[k] {
			affected++
		}
	}
	return affected
}

// SimulateRemoveNode 模拟删除节点的影响
func (h *WeightedHash) SimulateRemoveNode(nodeID string, testKeys []string) int {
	clone := h.Clone()

	oldMapping := make(map[string]string)
	for _, k := range testKeys {
		oldMapping[k] = h.GetNode(k)
	}

	clone.RemoveNode(nodeID)

	affected := 0
	for _, k := range testKeys {
		if clone.GetNode(k) != oldMapping[k] {
			affected++
		}
	}
	return affected
}
