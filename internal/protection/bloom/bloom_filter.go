package bloom

import (
	"fmt"
	"hash"
	"hash/fnv"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bits-and-blooms/bitset"
)

// BloomFilter 布隆过滤器
type BloomFilter struct {
	mu          sync.RWMutex
	bitset      *bitset.BitSet
	hashFuncs   []hash.Hash64 // 保留类型以兼容历史结构（当前实现使用 double-hashing，不复用此切片）
	m           uint          // 位数组大小
	k           uint          // 哈希函数个数
	n           uint64        // 已添加元素个数
	maxElements uint64        // 最大元素个数

	// 统计信息
	addCount      int64
	checkCount    int64
	falsePositive int64

	// 自动刷新
	autoRefresh   bool
	refreshPeriod time.Duration
	stopRefresh   chan struct{}
	refreshMutex  sync.Mutex
	closeOnce     sync.Once
}

// Config 布隆过滤器配置
type Config struct {
	ExpectedElements  uint64        `yaml:"expected_elements" json:"expected_elements"`
	FalsePositiveRate float64       `yaml:"false_positive_rate" json:"false_positive_rate"`
	AutoRefresh       bool          `yaml:"auto_refresh" json:"auto_refresh"`
	RefreshPeriod     time.Duration `yaml:"refresh_period" json:"refresh_period"`
}

// NewBloomFilter 创建布隆过滤器
func NewBloomFilter(config *Config) *BloomFilter {
	if config == nil {
		config = &Config{}
	}
	if config.ExpectedElements == 0 {
		config.ExpectedElements = 100000
	}
	if config.FalsePositiveRate == 0 {
		config.FalsePositiveRate = 0.01
	}

	// 计算最优参数
	m, k := calculateOptimalParameters(config.ExpectedElements, config.FalsePositiveRate)

	bf := &BloomFilter{
		bitset:        bitset.New(m),
		m:             m,
		k:             k,
		maxElements:   config.ExpectedElements,
		autoRefresh:   config.AutoRefresh,
		refreshPeriod: config.RefreshPeriod,
		stopRefresh:   make(chan struct{}),
	}

	// 不再在此复用 hash 对象；使用局部 double-hashing实现线程安全的哈希值生成
	bf.initHashFunctions()

	// 启动自动刷新
	if bf.autoRefresh && bf.refreshPeriod > 0 {
		go bf.startAutoRefresh()
	}

	return bf
}

// calculateOptimalParameters 计算最优参数，经典最优参数，使在期望元素数 n 与目标误报率 p 下，达到较优的空间/误报折中
func calculateOptimalParameters(n uint64, p float64) (m uint, k uint) {
	// m = -n * ln(p) / (ln(2))^2
	m = uint(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))

	// k = (m/n) * ln(2)
	// 注意：若 n 为 0 会导致除零，但在调用处已保证 n>0
	if n == 0 {
		k = 1
	} else {
		k = uint(math.Ceil(float64(m) / float64(n) * math.Log(2)))
	}

	// 确保k至少为1
	if k == 0 {
		k = 1
	}

	// m 至少为 1
	if m == 0 {
		m = 1
	}

	return m, k
}

// initHashFunctions (保留为 no-op): 当前实现使用 double hashing，因此不复用实例以保证线程安全。
// 如果未来需要优化（例如使用 sync.Pool 或者外部锁复用），可以在这里实现。
func (bf *BloomFilter) initHashFunctions() {
	bf.hashFuncs = nil
}

// Add 添加元素到布隆过滤器
func (bf *BloomFilter) Add(data []byte) {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	hashes := bf.getHashes(data)
	for _, hash := range hashes {
		bf.bitset.Set(hash)
	}

	bf.n++
	atomic.AddInt64(&bf.addCount, 1)
}

// AddString 添加字符串到布隆过滤器
func (bf *BloomFilter) AddString(str string) {
	bf.Add([]byte(str))
}

// Test 测试元素是否可能存在
func (bf *BloomFilter) Test(data []byte) bool {
	atomic.AddInt64(&bf.checkCount, 1)

	bf.mu.RLock()
	defer bf.mu.RUnlock()

	hashes := bf.getHashes(data)
	for _, hash := range hashes {
		if !bf.bitset.Test(hash) {
			return false // 绝对不存在
		}
	}

	return true // 可能存在
}

// TestString 测试字符串是否可能存在
func (bf *BloomFilter) TestString(str string) bool {
	return bf.Test([]byte(str))
}

// TestAndAdd 测试并添加元素（原子操作）
func (bf *BloomFilter) TestAndAdd(data []byte) bool {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	hashes := bf.getHashes(data)
	exists := true

	for _, hash := range hashes {
		if !bf.bitset.Test(hash) {
			exists = false
		}
		bf.bitset.Set(hash)
	}

	if !exists {
		bf.n++
		atomic.AddInt64(&bf.addCount, 1)
	}

	atomic.AddInt64(&bf.checkCount, 1)

	return exists
}

// getHashes 计算数据的多个哈希值 (使用 double hashing，线程安全且高效)
// 使用两个不同的局部哈希（fnv64a, fnv64）计算 h1 和 h2，然后按 a + i*b 生成 k 个哈希值
func (bf *BloomFilter) getHashes(data []byte) []uint {
	hashes := make([]uint, bf.k)

	// h1
	h1 := fnv.New64a()
	h1.Write(data)
	a := h1.Sum64()

	// h2
	h2 := fnv.New64()
	h2.Write(data)
	b := h2.Sum64()

	// 生成 k 个哈希值
	mod := uint64(bf.m)
	for i := uint(0); i < bf.k; i++ {
		combined := a + uint64(i)*b
		hashes[i] = uint(combined % mod)
	}

	return hashes
}

// Clear 清空布隆过滤器
func (bf *BloomFilter) Clear() {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	bf.bitset.ClearAll()
	bf.n = 0

	atomic.StoreInt64(&bf.addCount, 0)
	atomic.StoreInt64(&bf.checkCount, 0)
	atomic.StoreInt64(&bf.falsePositive, 0)
}

// MarshalBinary returns a consistent snapshot of the filter bitset.
func (bf *BloomFilter) MarshalBinary() ([]byte, error) {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.bitset.MarshalBinary()
}

// ReplaceBinary builds the replacement off-lock and swaps it atomically. This
// keeps readers on the old complete filter while a cluster snapshot is decoded.
func (bf *BloomFilter) ReplaceBinary(data []byte, elements uint64) error {
	next := bitset.New(bf.m)
	if err := next.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("decode bloom snapshot: %w", err)
	}
	if next.Len() != bf.m {
		return fmt.Errorf("incompatible bloom snapshot: got %d bits, want %d", next.Len(), bf.m)
	}

	bf.mu.Lock()
	bf.bitset = next
	bf.n = elements
	bf.mu.Unlock()
	return nil
}

// Stats 获取统计信息
func (bf *BloomFilter) Stats() *Stats {
	bf.mu.RLock()
	defer bf.mu.RUnlock()

	return &Stats{
		BitArraySize:      bf.m,
		HashFunctions:     bf.k,
		ElementsAdded:     bf.n,
		MaxElements:       bf.maxElements,
		EstimatedFPR:      bf.estimateFalsePositiveRate(),
		Capacity:          bf.capacity(),
		AddCount:          atomic.LoadInt64(&bf.addCount),
		CheckCount:        atomic.LoadInt64(&bf.checkCount),
		FalsePositiveHint: atomic.LoadInt64(&bf.falsePositive),
	}
}

// Stats 统计信息
type Stats struct {
	BitArraySize      uint    `json:"bit_array_size"`
	HashFunctions     uint    `json:"hash_functions"`
	ElementsAdded     uint64  `json:"elements_added"`
	MaxElements       uint64  `json:"max_elements"`
	EstimatedFPR      float64 `json:"estimated_fpr"`
	Capacity          float64 `json:"capacity"`
	AddCount          int64   `json:"add_count"`
	CheckCount        int64   `json:"check_count"`
	FalsePositiveHint int64   `json:"false_positive_hint"`
}

// estimateFalsePositiveRate 估算当前误报率
func (bf *BloomFilter) estimateFalsePositiveRate() float64 {
	if bf.n == 0 {
		return 0
	}

	// FPR = (1 - e^(-k*n/m))^k
	ratio := float64(bf.k) * float64(bf.n) / float64(bf.m)
	return math.Pow(1-math.Exp(-ratio), float64(bf.k))
}

// capacity 计算容量使用率
func (bf *BloomFilter) capacity() float64 {
	if bf.maxElements == 0 {
		return 0
	}
	return float64(bf.n) / float64(bf.maxElements)
}

// ShouldRefresh 判断是否应该刷新
// 修改为 >= 0.8 以匹配测试期望（当容量达到 80% 时应触发刷新）
func (bf *BloomFilter) ShouldRefresh() bool {
	bf.mu.RLock()
	defer bf.mu.RUnlock()
	return bf.capacity() >= 0.8 || bf.estimateFalsePositiveRate() > 0.05
}

// startAutoRefresh 启动自动刷新
func (bf *BloomFilter) startAutoRefresh() {
	ticker := time.NewTicker(bf.refreshPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if bf.ShouldRefresh() {
				bf.refreshMutex.Lock()
				bf.Clear()
				bf.refreshMutex.Unlock()
			}
		case <-bf.stopRefresh:
			return
		}
	}
}

// Close 关闭布隆过滤器
func (bf *BloomFilter) Close() error {
	if bf.autoRefresh {
		bf.closeOnce.Do(func() { close(bf.stopRefresh) })
	}
	return nil
}

// Union 合并两个布隆过滤器
func (bf *BloomFilter) Union(other *BloomFilter) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// 检查兼容性
	if bf.m != other.m || bf.k != other.k {
		return ErrIncompatibleFilters
	}

	bf.bitset = bf.bitset.Union(other.bitset)
	bf.n += other.n

	return nil
}

// Intersect 计算两个布隆过滤器的交集
func (bf *BloomFilter) Intersect(other *BloomFilter) error {
	bf.mu.Lock()
	defer bf.mu.Unlock()

	other.mu.RLock()
	defer other.mu.RUnlock()

	// 检查兼容性
	if bf.m != other.m || bf.k != other.k {
		return ErrIncompatibleFilters
	}

	bf.bitset = bf.bitset.Intersection(other.bitset)
	// 交集后元素个数难以准确计算，使用较小值作为估算
	if other.n < bf.n {
		bf.n = other.n
	}

	return nil
}

// ScalableBloomFilter 可扩展布隆过滤器
type ScalableBloomFilter struct {
	mu            sync.RWMutex
	filters       []*BloomFilter
	config        *Config
	currentIdx    int
	totalElements uint64
}

// NewScalableBloomFilter 创建可扩展布隆过滤器
func NewScalableBloomFilter(config *Config) *ScalableBloomFilter {
	sbf := &ScalableBloomFilter{
		config: config,
	}

	// 创建第一个过滤器
	sbf.addNewFilter()

	return sbf
}

// addNewFilter 添加新的过滤器
func (sbf *ScalableBloomFilter) addNewFilter() {
	// 每个新过滤器的容量翻倍，误报率减半（典型策略）
	newConfig := *sbf.config
	// 当 filters 长度为 0 时，len+1 = 1，ExpectedElements 不变
	newConfig.ExpectedElements = sbf.config.ExpectedElements * uint64(len(sbf.filters)+1)
	newConfig.FalsePositiveRate = sbf.config.FalsePositiveRate * math.Pow(0.5, float64(len(sbf.filters)))

	filter := NewBloomFilter(&newConfig)
	sbf.filters = append(sbf.filters, filter)
	sbf.currentIdx = len(sbf.filters) - 1
}

// Add 添加元素
func (sbf *ScalableBloomFilter) Add(data []byte) {
	sbf.mu.Lock()
	defer sbf.mu.Unlock()

	currentFilter := sbf.filters[sbf.currentIdx]

	// 检查当前过滤器是否已满
	if currentFilter.capacity() > 0.8 {
		sbf.addNewFilter()
		currentFilter = sbf.filters[sbf.currentIdx]
	}

	currentFilter.Add(data)
	sbf.totalElements++
}

// Test 测试元素是否存在
func (sbf *ScalableBloomFilter) Test(data []byte) bool {
	sbf.mu.RLock()
	defer sbf.mu.RUnlock()

	// 在所有过滤器中检查
	for _, filter := range sbf.filters {
		if filter.Test(data) {
			return true
		}
	}

	return false
}

// Stats 获取统计信息
func (sbf *ScalableBloomFilter) Stats() *ScalableStats {
	sbf.mu.RLock()
	defer sbf.mu.RUnlock()

	stats := &ScalableStats{
		FilterCount:   len(sbf.filters),
		TotalElements: sbf.totalElements,
		FilterStats:   make([]*Stats, len(sbf.filters)),
	}

	for i, filter := range sbf.filters {
		stats.FilterStats[i] = filter.Stats()
	}

	return stats
}

// ScalableStats 可扩展布隆过滤器统计信息
type ScalableStats struct {
	FilterCount   int      `json:"filter_count"`
	TotalElements uint64   `json:"total_elements"`
	FilterStats   []*Stats `json:"filter_stats"`
}

// CountingBloomFilter 计数布隆过滤器（支持删除）
type CountingBloomFilter struct {
	mu       sync.RWMutex
	counters []uint8
	m        uint
	k        uint
	n        uint64
	hashFunc hash.Hash64
}

// NewCountingBloomFilter 创建计数布隆过滤器
func NewCountingBloomFilter(expectedElements uint64, falsePositiveRate float64) *CountingBloomFilter {
	m, k := calculateOptimalParameters(expectedElements, falsePositiveRate)

	return &CountingBloomFilter{
		counters: make([]uint8, m),
		m:        m,
		k:        k,
		hashFunc: fnv.New64a(),
	}
}

// Add 添加元素
func (cbf *CountingBloomFilter) Add(data []byte) {
	cbf.mu.Lock()
	defer cbf.mu.Unlock()

	hashes := cbf.getHashes(data)
	for _, hash := range hashes {
		if cbf.counters[hash] < 255 { // 防止溢出
			cbf.counters[hash]++
		}
	}

	cbf.n++
}

// Remove 删除元素
func (cbf *CountingBloomFilter) Remove(data []byte) bool {
	cbf.mu.Lock()
	defer cbf.mu.Unlock()

	hashes := cbf.getHashes(data)

	// 首先检查元素是否存在
	for _, hash := range hashes {
		if cbf.counters[hash] == 0 {
			return false // 元素不存在
		}
	}

	// 减少计数
	for _, hash := range hashes {
		cbf.counters[hash]--
	}

	cbf.n--
	return true
}

// Test 测试元素是否存在
func (cbf *CountingBloomFilter) Test(data []byte) bool {
	cbf.mu.RLock()
	defer cbf.mu.RUnlock()

	hashes := cbf.getHashes(data)
	for _, hash := range hashes {
		if cbf.counters[hash] == 0 {
			return false
		}
	}

	return true
}

// getHashes 计算哈希值（CountingBloomFilter 使用 cbf.hashFunc 与种子）
func (cbf *CountingBloomFilter) getHashes(data []byte) []uint {
	hashes := make([]uint, cbf.k)

	for i := uint(0); i < cbf.k; i++ {
		cbf.hashFunc.Reset()
		cbf.hashFunc.Write(data)
		cbf.hashFunc.Write([]byte{byte(i)}) // 添加种子
		hash := cbf.hashFunc.Sum64()
		hashes[i] = uint(hash % uint64(cbf.m))
	}

	return hashes
}

// 错误定义
var (
	ErrIncompatibleFilters = fmt.Errorf("incompatible bloom filters")
)
