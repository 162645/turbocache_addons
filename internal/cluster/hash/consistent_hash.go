package hash

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"math"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/cespare/xxhash/v2"
)

// HashFunc 哈希函数类型：接收字节切片返回 uint32 值
type HashFunc func(data []byte) uint32

// Node 表示物理节点信息
type Node struct {
	ID       string            `json:"id"`
	Address  string            `json:"address"`
	Weight   int               `json:"weight"`
	Metadata map[string]string `json:"metadata"`
}

// VirtualNode 虚拟节点（用于 ConsistentHash）
type VirtualNode struct {
	Hash    uint32 `json:"hash"`
	NodeID  string `json:"node_id"`
	VNodeID int    `json:"vnode_id"`
}

// ConsistentHash 一致性哈希环
type ConsistentHash struct {
	mu           sync.RWMutex
	nodes        map[string]*Node // 物理节点
	virtualNodes []VirtualNode    // 虚拟节点（按哈希值排序）
	hashFunc     HashFunc         // 哈希函数
	replicas     int              // 每个节点的虚拟节点数量

	// 统计信息（totalKeys 用 atomic，keyDistrib 用 statsMu 保护）
	totalKeys  int64
	keyDistrib map[string]int64 // 每个节点的键分布
	statsMu    sync.Mutex       // 保护 keyDistrib（锁顺序：先 mu，再 statsMu）
}

// NewConsistentHash 创建一致性哈希环
// 注意：如果 hashFunc == nil，则默认使用 xxhash（xxhash64 -> 折叠到 32 位）
func NewConsistentHash(replicas int, hashFunc HashFunc) *ConsistentHash {
	if hashFunc == nil {
		hashFunc = xxhashHash32
	}

	return &ConsistentHash{
		nodes:      make(map[string]*Node),
		hashFunc:   hashFunc,
		replicas:   replicas,
		keyDistrib: make(map[string]int64),
	}
}

// AddNode 添加节点
func (ch *ConsistentHash) AddNode(node *Node) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// 检查节点是否已存在
	if _, exists := ch.nodes[node.ID]; exists {
		return
	}

	ch.nodes[node.ID] = node

	// 使用 statsMu 来修改 keyDistrib（保持锁顺序：先 mu，再 statsMu）
	ch.statsMu.Lock()
	ch.keyDistrib[node.ID] = 0
	ch.statsMu.Unlock()

	// 根据权重计算虚拟节点数量
	weight := node.Weight
	if weight <= 0 {
		weight = 1
	}

	virtualNodeCount := ch.replicas * weight
	if virtualNodeCount <= 0 {
		virtualNodeCount = 1
	}

	// 预分配，减少重分配开销
	newVNodes := make([]VirtualNode, 0, len(ch.virtualNodes)+virtualNodeCount)
	newVNodes = append(newVNodes, ch.virtualNodes...)

	// 添加虚拟节点
	for i := 0; i < virtualNodeCount; i++ {
		// 使用更明确的 vnode key 格式，降低意外冲突风险
		vNodeKey := fmt.Sprintf("vnode::%s::%d", node.ID, i)
		h := ch.hashFunc([]byte(vNodeKey))

		vNode := VirtualNode{
			Hash:    h,
			NodeID:  node.ID,
			VNodeID: i,
		}

		newVNodes = append(newVNodes, vNode)
	}

	ch.virtualNodes = newVNodes

	// 重新排序虚拟节点
	sort.Slice(ch.virtualNodes, func(i, j int) bool {
		return ch.virtualNodes[i].Hash < ch.virtualNodes[j].Hash
	})
}

// RemoveNode 移除节点
func (ch *ConsistentHash) RemoveNode(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// 检查节点是否存在
	if _, exists := ch.nodes[nodeID]; !exists {
		return
	}

	delete(ch.nodes, nodeID)

	// 使用 statsMu 来修改 keyDistrib（保持锁顺序：先 mu，再 statsMu）
	ch.statsMu.Lock()
	delete(ch.keyDistrib, nodeID)
	ch.statsMu.Unlock()

	// 移除虚拟节点
	newVirtualNodes := make([]VirtualNode, 0, len(ch.virtualNodes))
	for _, vNode := range ch.virtualNodes {
		if vNode.NodeID != nodeID {
			newVirtualNodes = append(newVirtualNodes, vNode)
		}
	}

	ch.virtualNodes = newVirtualNodes
}

// GetNode 获取键对应的节点
func (ch *ConsistentHash) GetNode(key string) *Node {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.virtualNodes) == 0 {
		return nil
	}

	hash := ch.hashFunc([]byte(key))

	// 二分查找第一个哈希值大于等于key哈希值的虚拟节点
	idx := sort.Search(len(ch.virtualNodes), func(i int) bool {
		return ch.virtualNodes[i].Hash >= hash
	})

	// 如果没找到，使用第一个虚拟节点（环形结构）
	if idx == len(ch.virtualNodes) {
		idx = 0
	}

	nodeID := ch.virtualNodes[idx].NodeID

	// 更新键分布统计（用 statsMu 保护，锁顺序：mu(R) -> statsMu）
	ch.statsMu.Lock()
	ch.keyDistrib[nodeID]++
	ch.statsMu.Unlock()
	atomic.AddInt64(&ch.totalKeys, 1)

	return ch.nodes[nodeID]
}

// GetNodes 获取键对应的多个节点（用于复制）
func (ch *ConsistentHash) GetNodes(key string, count int) []*Node {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	// 保护：virtualNodes 为空时直接返回
	if len(ch.virtualNodes) == 0 || len(ch.nodes) == 0 || count <= 0 {
		return nil
	}

	// 限制节点数量
	if count > len(ch.nodes) {
		count = len(ch.nodes)
	}

	hash := ch.hashFunc([]byte(key))

	// 找到第一个节点
	idx := sort.Search(len(ch.virtualNodes), func(i int) bool {
		return ch.virtualNodes[i].Hash >= hash
	})

	if idx == len(ch.virtualNodes) {
		idx = 0
	}

	// 收集不重复的节点
	nodeSet := make(map[string]*Node)
	nodes := make([]*Node, 0, count)

	// 局部计数，用于一次性更新 keyDistrib，避免频繁锁
	localCounts := make(map[string]int64)

	for len(nodes) < count && len(nodeSet) < len(ch.nodes) {
		vNode := ch.virtualNodes[idx]

		if _, exists := nodeSet[vNode.NodeID]; !exists {
			node := ch.nodes[vNode.NodeID]
			if node != nil {
				nodeSet[vNode.NodeID] = node
				nodes = append(nodes, node)
				localCounts[vNode.NodeID]++
			}
		}

		// 移动到下一个虚拟节点
		idx = (idx + 1) % len(ch.virtualNodes)
	}

	// 更新统计（保持锁顺序：mu(R) -> statsMu）
	ch.statsMu.Lock()
	for id, c := range localCounts {
		ch.keyDistrib[id] += c
	}
	ch.statsMu.Unlock()
	atomic.AddInt64(&ch.totalKeys, 1)

	return nodes
}

// GetAllNodes 获取所有节点（返回副本）
func (ch *ConsistentHash) GetAllNodes() map[string]*Node {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	nodes := make(map[string]*Node)
	for id, node := range ch.nodes {
		nodes[id] = &Node{
			ID:       node.ID,
			Address:  node.Address,
			Weight:   node.Weight,
			Metadata: make(map[string]string),
		}

		// 复制metadata
		for k, v := range node.Metadata {
			nodes[id].Metadata[k] = v
		}
	}

	return nodes
}

// UpdateNode 更新节点信息
func (ch *ConsistentHash) UpdateNode(node *Node) {
	// 先用 RLock 判断是否存在，避免长时间持有写锁或死锁（如果直接 Lock 后再调用 AddNode 可能会死锁）
	ch.mu.RLock()
	oldNode, exists := ch.nodes[node.ID]
	ch.mu.RUnlock()

	if !exists {
		ch.AddNode(node)
		return
	}

	// 如果权重发生变化，需要重新计算虚拟节点（Remove + Add）
	if oldNode.Weight != node.Weight {
		ch.RemoveNode(node.ID)
		ch.AddNode(node)
		return
	}

	// 仅更新节点信息（写操作需加写锁）
	ch.mu.Lock()
	ch.nodes[node.ID] = node
	ch.mu.Unlock()
}

// Stats 获取统计信息
func (ch *ConsistentHash) Stats() map[string]interface{} {
	ch.mu.RLock()
	// 在读取 keyDistrib 前使用 statsMu，锁顺序：mu(R) -> statsMu
	ch.statsMu.Lock()

	stats := map[string]interface{}{
		"total_nodes":         len(ch.nodes),
		"total_virtual_nodes": len(ch.virtualNodes),
		"replicas":            ch.replicas,
		"total_keys":          atomic.LoadInt64(&ch.totalKeys),
		"key_distribution":    make(map[string]int64),
	}

	// 复制键分布统计
	for nodeID, count := range ch.keyDistrib {
		stats["key_distribution"].(map[string]int64)[nodeID] = count
	}

	ch.statsMu.Unlock()
	ch.mu.RUnlock()

	return stats
}

// IsEmpty 检查环是否为空
func (ch *ConsistentHash) IsEmpty() bool {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return len(ch.nodes) == 0
}

// Size 获取节点数量
func (ch *ConsistentHash) Size() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	return len(ch.nodes)
}

// Clear 清空哈希环
func (ch *ConsistentHash) Clear() {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// 清理结构并重置统计（遵守锁顺序）
	ch.statsMu.Lock()
	ch.nodes = make(map[string]*Node)
	ch.virtualNodes = nil
	ch.keyDistrib = make(map[string]int64)
	atomic.StoreInt64(&ch.totalKeys, 0)
	ch.statsMu.Unlock()
}

// GetBalance 获取负载均衡度
func (ch *ConsistentHash) GetBalance() float64 {
	ch.mu.RLock()
	// 读取 keyDistrib 前先获取 statsMu（锁顺序：mu(R) -> statsMu）
	ch.statsMu.Lock()

	if len(ch.nodes) == 0 {
		ch.statsMu.Unlock()
		ch.mu.RUnlock()
		return 0
	}

	totalKeys := atomic.LoadInt64(&ch.totalKeys)
	if totalKeys == 0 {
		ch.statsMu.Unlock()
		ch.mu.RUnlock()
		return 1.0 // 没有键时认为完全平衡
	}

	avgKeysPerNode := float64(totalKeys) / float64(len(ch.nodes))
	if avgKeysPerNode == 0 {
		ch.statsMu.Unlock()
		ch.mu.RUnlock()
		return 1.0
	}

	variance := 0.0
	for _, count := range ch.keyDistrib {
		diff := float64(count) - avgKeysPerNode
		variance += diff * diff
	}

	ch.statsMu.Unlock()
	ch.mu.RUnlock()

	variance /= float64(len(ch.nodes))
	stdDev := math.Sqrt(variance)

	// 平衡度 = 1 - (标准差 / 平均值)
	balance := 1.0 - (stdDev / avgKeysPerNode)
	if balance < 0 {
		balance = 0
	}

	return balance
}

// --- 预定义哈希函数（保留若干实现以便测试/回退） ---

// crc32Hash CRC32哈希函数（保留）
func crc32Hash(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// sha1Hash SHA1哈希函数（保留）
func sha1Hash(data []byte) uint32 {
	sum := sha1.Sum(data)
	return binary.BigEndian.Uint32(sum[:4])
}

// fnvHash FNV-1a 32-bit（保留）
func fnvHash(data []byte) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)

	h := uint32(offset32)
	for _, b := range data {
		h ^= uint32(b)
		h *= prime32
	}
	return h
}

// xxhashHash32 使用 xxhash64 并折叠为 32 位（推荐默认）
func xxhashHash32(data []byte) uint32 {
	sum := xxhash.Sum64(data)
	return uint32(sum>>32) ^ uint32(sum&0xffffffff)
}

// GetHashFunc 根据名称获取哈希函数
func GetHashFunc(name string) HashFunc {
	switch name {
	case "crc32":
		return crc32Hash
	case "sha1":
		return sha1Hash
	case "fnv":
		return fnvHash
	case "xxhash", "xxh", "xxh64":
		return xxhashHash32
	default:
		return xxhashHash32
	}
}

// --- 加权一致性哈希扩展（保留） ---

type WeightedConsistentHash struct {
	*ConsistentHash
	weightMultiplier int
}

func NewWeightedConsistentHash(baseReplicas, weightMultiplier int, hashFunc HashFunc) *WeightedConsistentHash {
	return &WeightedConsistentHash{
		ConsistentHash:   NewConsistentHash(baseReplicas, hashFunc),
		weightMultiplier: weightMultiplier,
	}
}

func (wch *WeightedConsistentHash) AddNode(node *Node) {
	originalReplicas := wch.replicas
	if node.Weight > 0 {
		wch.replicas = originalReplicas * node.Weight * wch.weightMultiplier / 100
		if wch.replicas < 1 {
			wch.replicas = 1
		}
	}

	wch.ConsistentHash.AddNode(node)
	wch.replicas = originalReplicas
}
