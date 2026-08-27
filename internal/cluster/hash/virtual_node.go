package hash

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

// VNodeInfo 虚拟节点详细信息（用于 VirtualNodeManager）
type VNodeInfo struct {
	ID       string // 虚拟节点ID (realNode#index)
	Hash     uint32 // 哈希值
	RealNode string // 真实节点ID
	Index    int    // 虚拟节点索引
}

// VirtualNodeManager 虚拟节点管理器
type VirtualNodeManager struct {
	mu sync.RWMutex

	// 虚拟节点数量（每个真实节点的副本数）
	replicas int

	// 哈希函数
	hashFunc HashFunc

	// 虚拟节点环（按 hash 排序）
	ring []uint32

	// 哈希值到虚拟节点的映射
	hashToNode map[uint32]*VNodeInfo

	// 真实节点到虚拟节点的映射
	nodeToVirtual map[string][]*VNodeInfo
}

// NewVirtualNodeManager 创建虚拟节点管理器
func NewVirtualNodeManager(replicas int, hashFunc HashFunc) *VirtualNodeManager {
	if hashFunc == nil {
		// 统一默认使用包内推荐的 xxhash 实现
		hashFunc = xxhashHash32
	}

	return &VirtualNodeManager{
		replicas:      replicas,
		hashFunc:      hashFunc,
		hashToNode:    make(map[uint32]*VNodeInfo),
		nodeToVirtual: make(map[string][]*VNodeInfo),
	}
}

// AddNode 添加真实节点
//
// 关键改动：为了避免长时间持写锁（导致大量并发读/写进入阻塞 -> 死锁），
// 我们把可以并发执行的部分（虚拟节点构造与哈希计算）放到锁外执行，
// 只在短时间内获取写锁来检查/合并到共享结构：
//  1. 先读一下 replicas/hashFunc（用 RLock），然后释放锁；
//  2. 在外面构造虚拟节点切片（不持锁）；
//  3. 获取写锁，检查是否已有节点（避免并发重复添加），若无则合并并排序 ring。
//
// 这样显著缩短写锁临界区，减少与读操作的争用，避免死锁风险。
func (m *VirtualNodeManager) AddNode(nodeID string) []*VNodeInfo {
	// 先用读锁安全读取当前的 replicas 和 hashFunc 指针，避免直接读共享变量导致竞态
	m.mu.RLock()
	replicas := m.replicas
	hf := m.hashFunc
	m.mu.RUnlock()

	// 如果 replicas 为 0，直接返回空（或你可以根据需求设为默认 1）
	if replicas <= 0 {
		// 不持锁的快速返回
		virtualNodes := make([]*VNodeInfo, 0)
		return virtualNodes
	}

	// 在锁外创建虚拟节点数据（哈希计算在这里进行）——这部分耗时但并发安全
	virtualNodes := make([]*VNodeInfo, 0, replicas)
	vnodeHashes := make([]uint32, 0, replicas)
	for i := 0; i < replicas; i++ {
		id := fmt.Sprintf("%s#%d", nodeID, i)
		h := hf([]byte(id))

		v := &VNodeInfo{
			ID:       id,
			Hash:     h,
			RealNode: nodeID,
			Index:    i,
		}
		virtualNodes = append(virtualNodes, v)
		vnodeHashes = append(vnodeHashes, h)
	}

	// 短写锁临界区：检查是否已有 node、合并 vnode 到共享结构并排序 ring
	m.mu.Lock()
	defer m.mu.Unlock()

	// 如果在我们计算期间有其它 goroutine 已添加了相同节点，直接返回已有的（避免重复）
	if existing, exists := m.nodeToVirtual[nodeID]; exists {
		// 返回现有的内部切片（AddNode 的语义是返回内部切片）
		return existing
	}

	// 合并到 hashToNode 与 nodeToVirtual
	for i, v := range virtualNodes {
		m.hashToNode[v.Hash] = v
		m.ring = append(m.ring, vnodeHashes[i])
	}
	// 保存 node->vnodes 映射（指向新创建的虚拟节点切片）
	m.nodeToVirtual[nodeID] = virtualNodes

	// 排序环（在写锁内进行，保证一致性）
	sort.Slice(m.ring, func(i, j int) bool {
		return m.ring[i] < m.ring[j]
	})

	return virtualNodes
}

// RemoveNode 移除真实节点
func (m *VirtualNodeManager) RemoveNode(nodeID string) []*VNodeInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 获取虚拟节点
	virtualNodes, exists := m.nodeToVirtual[nodeID]
	if !exists {
		return nil
	}

	// 从映射中删除
	for _, vnode := range virtualNodes {
		delete(m.hashToNode, vnode.Hash)

		// 从环中删除（ring 已经排序）
		idx := sort.Search(len(m.ring), func(i int) bool {
			return m.ring[i] >= vnode.Hash
		})
		if idx < len(m.ring) && m.ring[idx] == vnode.Hash {
			m.ring = append(m.ring[:idx], m.ring[idx+1:]...)
		}
	}

	delete(m.nodeToVirtual, nodeID)

	return virtualNodes
}

// GetNode 根据键获取对应的真实节点
func (m *VirtualNodeManager) GetNode(key string) string {
	m.mu.RLock()
	// 快速路径：如果 ring 为空直接返回
	if len(m.ring) == 0 {
		m.mu.RUnlock()
		return ""
	}

	// 计算哈希（hf 不会修改内部结构）
	h := m.hashFunc([]byte(key))

	// 在环上查找第一个大于等于 h 的虚拟节点
	idx := sort.Search(len(m.ring), func(i int) bool {
		return m.ring[i] >= h
	})

	// 如果没找到，使用第一个节点（环形）
	if idx >= len(m.ring) {
		idx = 0
	}

	// 读取映射并返回真实节点ID
	vnode := m.hashToNode[m.ring[idx]]
	real := vnode.RealNode
	m.mu.RUnlock()
	return real
}

// GetNodes 获取键的N个候选节点（用于复制）
func (m *VirtualNodeManager) GetNodes(key string, n int) []string {
	m.mu.RLock()
	if len(m.ring) == 0 {
		m.mu.RUnlock()
		return nil
	}

	h := m.hashFunc([]byte(key))

	idx := sort.Search(len(m.ring), func(i int) bool {
		return m.ring[i] >= h
	})
	if idx >= len(m.ring) {
		idx = 0
	}

	// 收集N个不同的真实节点
	nodes := make([]string, 0, n)
	seen := make(map[string]bool)

	for len(nodes) < n && len(seen) < len(m.nodeToVirtual) {
		vnode := m.hashToNode[m.ring[idx]]
		if !seen[vnode.RealNode] {
			nodes = append(nodes, vnode.RealNode)
			seen[vnode.RealNode] = true
		}
		idx = (idx + 1) % len(m.ring)
	}

	m.mu.RUnlock()
	return nodes
}

// GetVirtualNodes 获取真实节点的所有虚拟节点（返回副本）
func (m *VirtualNodeManager) GetVirtualNodes(nodeID string) []*VNodeInfo {
	m.mu.RLock()
	vnodes, exists := m.nodeToVirtual[nodeID]
	if !exists {
		m.mu.RUnlock()
		return nil
	}

	// 返回深拷贝：对每个元素复制结构体的值并创建新的指针，
	// 这样外部修改不会影响内部存储（测试期待这一行为）。
	result := make([]*VNodeInfo, len(vnodes))
	for i, v := range vnodes {
		copyV := *v
		result[i] = &copyV
	}
	m.mu.RUnlock()
	return result
}

// GetAllNodes 获取所有真实节点
func (m *VirtualNodeManager) GetAllNodes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	nodes := make([]string, 0, len(m.nodeToVirtual))
	for nodeID := range m.nodeToVirtual {
		nodes = append(nodes, nodeID)
	}
	return nodes
}

// GetNodeCount 获取真实节点数量
func (m *VirtualNodeManager) GetNodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.nodeToVirtual)
}

// GetVirtualNodeCount 获取虚拟节点总数
func (m *VirtualNodeManager) GetVirtualNodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.ring)
}

// GetLoadDistribution 获取负载分布情况（按虚拟节点数）
func (m *VirtualNodeManager) GetLoadDistribution() map[string]int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	distribution := make(map[string]int, len(m.nodeToVirtual))
	for nodeID, vnodes := range m.nodeToVirtual {
		distribution[nodeID] = len(vnodes)
	}
	return distribution
}

// Rebalance 重新平衡虚拟节点（重建 ring）
func (m *VirtualNodeManager) Rebalance() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ring = make([]uint32, 0, len(m.hashToNode))
	for h := range m.hashToNode {
		m.ring = append(m.ring, h)
	}

	sort.Slice(m.ring, func(i, j int) bool {
		return m.ring[i] < m.ring[j]
	})
}

// Clear 清空所有节点
func (m *VirtualNodeManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ring = nil
	m.hashToNode = make(map[uint32]*VNodeInfo)
	m.nodeToVirtual = make(map[string][]*VNodeInfo)
}

// RingSegment 环段信息
type RingSegment struct {
	StartHash    uint32  `json:"start_hash"`
	EndHash      uint32  `json:"end_hash"`
	VirtualNode  string  `json:"virtual_node"`
	RealNode     string  `json:"real_node"`
	SegmentSize  uint64  `json:"segment_size"` // 使用 uint64 避免溢出
	SegmentRatio float64 `json:"segment_ratio"`
}

// GetRingInfo 获取环的信息（用于调试）
func (m *VirtualNodeManager) GetRingInfo() []RingSegment {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if len(m.ring) == 0 {
		return nil
	}

	segments := make([]RingSegment, len(m.ring))
	const maxUint32 = uint64(1) << 32 // 使用 uint64 避免溢出

	for i, hash := range m.ring {
		vnode := m.hashToNode[hash]
		var nextHash uint64
		if i < len(m.ring)-1 {
			nextHash = uint64(m.ring[i+1])
		} else {
			// 环形结构，最后一个段到第一个段
			nextHash = uint64(m.ring[0]) + maxUint32
		}

		startHash := uint64(hash)
		segmentSize := nextHash - startHash

		segments[i] = RingSegment{
			StartHash:    hash,
			EndHash:      uint32(nextHash % maxUint32),
			VirtualNode:  vnode.ID,
			RealNode:     vnode.RealNode,
			SegmentSize:  segmentSize,
			SegmentRatio: float64(segmentSize) / float64(maxUint32),
		}
	}

	return segments
}

// Stats / Balance 相关类型与函数（保留）
// Stats 统计信息
type Stats struct {
	TotalNodes        int             `json:"total_nodes"`
	TotalVirtualNodes int             `json:"total_virtual_nodes"`
	ReplicasPerNode   int             `json:"replicas_per_node"`
	LoadDistribution  map[string]int  `json:"load_distribution"`
	RingSegments      []RingSegment   `json:"ring_segments,omitempty"`
	Balance           *BalanceMetrics `json:"balance,omitempty"`
}

// BalanceMetrics 平衡度指标
type BalanceMetrics struct {
	MaxLoad     int     `json:"max_load"`
	MinLoad     int     `json:"min_load"`
	AvgLoad     float64 `json:"avg_load"`
	StdDev      float64 `json:"std_dev"`
	Coefficient float64 `json:"coefficient"` // 变异系数
}

// GetStats 获取统计信息（includeRing 控制是否包含 ring 段信息）
// 我们保持对外行为不变，但实现上尽量减少重复加锁：
// - 在一把读锁下读取必要数据并构造结果（包括 LoadDistribution 的副本）
// - 这样避免在持锁时再去调用会再次获取锁的 helper，减小锁争用窗口
func (m *VirtualNodeManager) GetStats(includeRing bool) *Stats {
	m.mu.RLock()
	// 做副本：节点数量、replicas、虚拟节点总数、以及 load distribution 副本
	totalNodes := len(m.nodeToVirtual)
	totalVNodes := len(m.ring)
	replicas := m.replicas

	loadDist := make(map[string]int, totalNodes)
	for nodeID, vnodes := range m.nodeToVirtual {
		loadDist[nodeID] = len(vnodes)
	}

	var ringSegments []RingSegment
	if includeRing && len(m.ring) > 0 {
		// 为避免在锁释放后 ring 有变化，直接在读锁下构造 ring 信息副本
		ringSegments = make([]RingSegment, len(m.ring))
		const maxUint32 = uint64(1) << 32
		for i, hash := range m.ring {
			vnode := m.hashToNode[hash]
			var nextHash uint64
			if i < len(m.ring)-1 {
				nextHash = uint64(m.ring[i+1])
			} else {
				nextHash = uint64(m.ring[0]) + maxUint32
			}
			startHash := uint64(hash)
			segmentSize := nextHash - startHash
			ringSegments[i] = RingSegment{
				StartHash:    hash,
				EndHash:      uint32(nextHash % maxUint32),
				VirtualNode:  vnode.ID,
				RealNode:     vnode.RealNode,
				SegmentSize:  segmentSize,
				SegmentRatio: float64(segmentSize) / float64(maxUint32),
			}
		}
	}

	// 计算 Balance（复用 calculateBalance 的实现，但为了降低锁争用我们在这里直接调用）
	balance := m.calculateBalanceLocked() // 使用已持锁的 helper

	m.mu.RUnlock()

	return &Stats{
		TotalNodes:        totalNodes,
		TotalVirtualNodes: totalVNodes,
		ReplicasPerNode:   replicas,
		LoadDistribution:  loadDist,
		RingSegments:      ringSegments,
		Balance:           balance,
	}
}

// calculateBalanceLocked 假定外部已经持有读锁（用于 GetStats 内部调用）
// 这样避免在 calculateBalance 内部再次获得锁，降低嵌套 RLock 的复杂度。
func (m *VirtualNodeManager) calculateBalanceLocked() *BalanceMetrics {
	if len(m.nodeToVirtual) == 0 {
		return nil
	}

	loads := make([]int, 0, len(m.nodeToVirtual))
	sum := 0
	maxLoad := 0
	minLoad := m.replicas

	for _, vnodes := range m.nodeToVirtual {
		load := len(vnodes)
		loads = append(loads, load)
		sum += load
		if load > maxLoad {
			maxLoad = load
		}
		if load < minLoad {
			minLoad = load
		}
	}

	avgLoad := float64(sum) / float64(len(loads))

	// 计算标准差
	variance := 0.0
	for _, load := range loads {
		diff := float64(load) - avgLoad
		variance += diff * diff
	}
	stdDev := 0.0
	if len(loads) > 0 {
		stdDev = math.Sqrt(variance / float64(len(loads)))
	}

	coefficient := 0.0
	if avgLoad > 0 {
		coefficient = stdDev / avgLoad
	}

	return &BalanceMetrics{
		MaxLoad:     maxLoad,
		MinLoad:     minLoad,
		AvgLoad:     avgLoad,
		StdDev:      stdDev,
		Coefficient: coefficient,
	}
}
