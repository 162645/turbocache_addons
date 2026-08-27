package eviction

import (
	"container/heap"
	"sync"
	"sync/atomic"
	"time"

	"turbocache/internal/cache/value"
)

// LFU 改进说明：
// - 修复了 SelectEvictKeys 中的链表遍历逻辑
// - 修复了哨兵节点判断问题
// - 增强了边界检查和空指针保护

// ----------------------------------
// LFUPolicy（策略）
// ----------------------------------

// LFUPolicy LFU淘汰策略实现
type LFUPolicy struct {
	mu          sync.RWMutex
	items       map[string]*lfuItem //主哈希表，O(1) 查找 key 对应项。
	freqGroups  map[int64]*lfuGroup // 频率分组
	minFreq     int64               // 最小频率
	maxEntries  int64
	currentSize int64

	// 统计信息
	accessCount   int64
	evictionCount int64
	hitCount      int64
	missCount     int64

	config *Config
}

// lfuItem LFU项目
type lfuItem struct {
	key        string
	entry      *value.Entry
	frequency  int64
	accessTime int64
	prev       *lfuItem
	next       *lfuItem
	group      *lfuGroup
}

// lfuGroup 频率分组
type lfuGroup struct {
	frequency int64
	head      *lfuItem
	tail      *lfuItem
	size      int
}

// NewLFUPolicy 创建LFU策略
func NewLFUPolicy(config *Config) *LFUPolicy {
	if config == nil {
		config = &Config{MaxEntries: 10000}
	}

	lfu := &LFUPolicy{
		items:      make(map[string]*lfuItem),
		freqGroups: make(map[int64]*lfuGroup),
		maxEntries: config.MaxEntries,
		minFreq:    1,
		config:     config,
	}

	return lfu
}

// OnAccess 记录访问事件
func (lfu *LFUPolicy) OnAccess(key string) {
	atomic.AddInt64(&lfu.accessCount, 1)

	lfu.mu.Lock()
	defer lfu.mu.Unlock()

	if item, exists := lfu.items[key]; exists {
		lfu.incrementFrequency(item)
		item.accessTime = time.Now().UnixNano()
		atomic.AddInt64(&lfu.hitCount, 1)
	} else {
		atomic.AddInt64(&lfu.missCount, 1)
	}
}

// OnSet 记录设置事件
func (lfu *LFUPolicy) OnSet(key string, entry *value.Entry) {
	lfu.mu.Lock()
	defer lfu.mu.Unlock()

	if item, exists := lfu.items[key]; exists {
		// 更新现有项目
		item.entry = entry
		item.accessTime = time.Now().UnixNano()
		lfu.incrementFrequency(item)
	} else {
		// 创建新项目
		item := &lfuItem{
			key:        key,
			entry:      entry,
			frequency:  1,
			accessTime: time.Now().UnixNano(),
		}

		lfu.items[key] = item
		lfu.addToGroup(item, 1)
		lfu.currentSize++

		// 新增后最小频率为 1
		lfu.minFreq = 1

		// 检查是否需要淘汰
		if lfu.maxEntries > 0 && lfu.currentSize > lfu.maxEntries {
			lfu.evictLFU()
		}
	}
}

// OnDelete 记录删除事件
func (lfu *LFUPolicy) OnDelete(key string) {
	lfu.mu.Lock()
	defer lfu.mu.Unlock()

	if item, exists := lfu.items[key]; exists {
		lfu.removeFromGroup(item)
		delete(lfu.items, key)
		lfu.currentSize--
		lfu.updateMinFreq()
	}
}

// SelectEvictKeys 选择要淘汰的键（修复版本）
func (lfu *LFUPolicy) SelectEvictKeys(count int) []string {
	if count <= 0 {
		return nil
	}

	lfu.mu.RLock()
	defer lfu.mu.RUnlock()

	if len(lfu.items) == 0 {
		return nil
	}

	var keys []string
	collected := 0
	maxIter := len(lfu.items) * 2 // 增加安全上限

	// 从最小频率开始向上遍历
	freq := lfu.minFreq
	freqIter := 0

	for collected < count && freqIter < len(lfu.freqGroups)+10 {
		group, exists := lfu.freqGroups[freq]
		if exists && group != nil && group.size > 0 {
			// 从尾部开始（最久未使用）
			current := group.tail.prev
			itemIter := 0

			// 遍历当前频率组，直到收集够或遍历完
			for current != nil && current != group.head && collected < count && itemIter < maxIter {
				// 确保这是一个有效的项目（不是哨兵节点）
				if current.key != "" {
					keys = append(keys, current.key)
					collected++
				}
				current = current.prev
				itemIter++
			}
		}
		freq++
		freqIter++

		// 如果已经遍历了所有可能的频率组，退出
		if freqIter > len(lfu.freqGroups)*2 {
			break
		}
	}

	return keys
}

// Stats 获取统计信息
func (lfu *LFUPolicy) Stats() *Stats {
	lfu.mu.RLock()
	currentSize := lfu.currentSize
	minFreq := lfu.minFreq
	freqGroupCount := len(lfu.freqGroups)
	lfu.mu.RUnlock()

	accessCount := atomic.LoadInt64(&lfu.accessCount)
	evictionCount := atomic.LoadInt64(&lfu.evictionCount)
	hitCount := atomic.LoadInt64(&lfu.hitCount)
	missCount := atomic.LoadInt64(&lfu.missCount)

	var hitRate float64
	if accessCount > 0 {
		hitRate = float64(hitCount) / float64(accessCount)
	}

	return &Stats{
		Type:          "lfu",
		TotalKeys:     currentSize,
		AccessCount:   accessCount,
		EvictionCount: evictionCount,
		HitRate:       hitRate,
		Extra: map[string]interface{}{
			"hit_count":        hitCount,
			"miss_count":       missCount,
			"max_entries":      lfu.maxEntries,
			"min_frequency":    minFreq,
			"freq_group_count": freqGroupCount,
		},
	}
}

// Reset 重置策略
func (lfu *LFUPolicy) Reset() {
	lfu.mu.Lock()
	defer lfu.mu.Unlock()

	lfu.items = make(map[string]*lfuItem)
	lfu.freqGroups = make(map[int64]*lfuGroup)
	lfu.minFreq = 1
	lfu.currentSize = 0

	atomic.StoreInt64(&lfu.accessCount, 0)
	atomic.StoreInt64(&lfu.evictionCount, 0)
	atomic.StoreInt64(&lfu.hitCount, 0)
	atomic.StoreInt64(&lfu.missCount, 0)
}

// Close 关闭策略
func (lfu *LFUPolicy) Close() error {
	lfu.Reset()
	return nil
}

// incrementFrequency 增加项目频率（仅在持锁上下文）
func (lfu *LFUPolicy) incrementFrequency(item *lfuItem) {
	oldFreq := item.frequency
	newFreq := oldFreq + 1

	// 从旧频率组移除（安全）
	lfu.removeFromGroup(item)

	// 添加到新频率组
	item.frequency = newFreq
	lfu.addToGroup(item, newFreq)

	// 如果旧频率是最小频率并且旧组已空，则更新 minFreq
	if oldFreq == lfu.minFreq {
		if group, exists := lfu.freqGroups[oldFreq]; !exists || group.size == 0 {
			lfu.minFreq = newFreq
		}
	}
}

// addToGroup 添加项目到频率组（带哨兵与保护）
func (lfu *LFUPolicy) addToGroup(item *lfuItem, frequency int64) {
	if item == nil {
		return
	}
	group, exists := lfu.freqGroups[frequency]
	if !exists {
		group = &lfuGroup{frequency: frequency}
		// 哨兵节点 - key 为空字符串用于识别
		group.head = &lfuItem{key: ""}
		group.tail = &lfuItem{key: ""}
		group.head.next = group.tail
		group.tail.prev = group.head
		group.size = 0
		lfu.freqGroups[frequency] = group
	}

	// 如果 item 已经在某组，先安全移除
	if item.group != nil {
		lfu.removeFromGroup(item)
	}

	// 添加到头部（最近使用）- 在 head 和 head.next 之间插入
	item.next = group.head.next
	item.prev = group.head
	if group.head.next != nil {
		group.head.next.prev = item
	}
	group.head.next = item
	item.group = group
	group.size++
}

// removeFromGroup 从频率组移除项目（带保护）
func (lfu *LFUPolicy) removeFromGroup(item *lfuItem) {
	if item == nil || item.group == nil || item.prev == nil || item.next == nil {
		return
	}
	group := item.group

	// 从链表中移除
	item.prev.next = item.next
	item.next.prev = item.prev
	group.size--

	// 如果组为空，删除组映射
	if group.size == 0 {
		delete(lfu.freqGroups, group.frequency)
	}

	item.group = nil
	item.prev = nil
	item.next = nil
}

// evictLFU 淘汰最少使用的项目（仅在持锁上下文）
func (lfu *LFUPolicy) evictLFU() {
	// 找到最小频率组
	group := lfu.freqGroups[lfu.minFreq]
	if group == nil || group.size == 0 {
		lfu.updateMinFreq()
		group = lfu.freqGroups[lfu.minFreq]
		if group == nil {
			return
		}
	}

	// 淘汰尾部项目（最久未使用）
	victim := group.tail.prev
	if victim == nil || victim == group.head || victim.key == "" {
		return
	}

	lfu.removeFromGroup(victim)
	delete(lfu.items, victim.key)
	lfu.currentSize--
	atomic.AddInt64(&lfu.evictionCount, 1)

	// 更新最小频率
	lfu.updateMinFreq()
}

// updateMinFreq 更新最小频率（扫描已存在组以找最小非空频率）
func (lfu *LFUPolicy) updateMinFreq() {
	if len(lfu.freqGroups) == 0 {
		lfu.minFreq = 1
		return
	}

	min := int64(0)
	first := true
	for freq, group := range lfu.freqGroups {
		if group == nil || group.size == 0 {
			continue
		}
		if first || freq < min {
			min = freq
			first = false
		}
	}
	if first {
		lfu.minFreq = 1
	} else {
		lfu.minFreq = min
	}
}

// ----------------------------------
// LFUCache（使用最小堆）
// ----------------------------------

// LFUCache LFU缓存实现（使用最小堆优化）
type LFUCache struct {
	mu            sync.RWMutex
	items         map[string]*lfuCacheItem
	frequencies   *lfuHeap
	maxEntries    int
	currentSize   int
	evictCallback EvictCallback

	// 统计信息
	accessCount   int64
	evictionCount int64
	hitCount      int64
	missCount     int64
}

// lfuCacheItem LFU缓存项
type lfuCacheItem struct {
	key        string
	entry      *value.Entry
	frequency  int64
	accessTime int64
	index      int // 在堆中的索引
}

// lfuHeap LFU最小堆
type lfuHeap []*lfuCacheItem

func (h lfuHeap) Len() int { return len(h) }
func (h lfuHeap) Less(i, j int) bool {
	if h[i].frequency != h[j].frequency {
		return h[i].frequency < h[j].frequency
	}
	return h[i].accessTime < h[j].accessTime
}
func (h lfuHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *lfuHeap) Push(x interface{}) {
	item := x.(*lfuCacheItem)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *lfuHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	item.index = -1
	*h = old[0 : n-1]
	return item
}

// NewLFUCache 创建LFU缓存
func NewLFUCache(maxEntries int) *LFUCache {
	cache := &LFUCache{
		items:       make(map[string]*lfuCacheItem),
		frequencies: &lfuHeap{},
		maxEntries:  maxEntries,
	}
	heap.Init(cache.frequencies)
	return cache
}

// Add 添加条目（注意回调在锁外执行）
func (c *LFUCache) Add(key string, entry *value.Entry) bool {
	c.mu.Lock()

	if item, exists := c.items[key]; exists {
		// 更新现有项目
		oldEntry := item.entry
		item.entry = entry
		item.frequency++
		item.accessTime = time.Now().UnixNano()
		heap.Fix(c.frequencies, item.index)

		c.mu.Unlock()
		// 在锁外调用回调（更新旧值）
		if c.evictCallback != nil {
			c.evictCallback(key, oldEntry)
		}
		return false
	}

	// 新项目
	item := &lfuCacheItem{key: key, entry: entry, frequency: 1, accessTime: time.Now().UnixNano()}
	c.items[key] = item
	heap.Push(c.frequencies, item)
	c.currentSize++

	// 如果需要淘汰，弹出最小项（在锁内），但不要在锁内调用回调
	var evicted *lfuCacheItem
	if c.maxEntries > 0 && c.currentSize > c.maxEntries {
		evicted = c.evictLFULocked()
	}
	c.mu.Unlock()

	if evicted != nil && c.evictCallback != nil {
		c.evictCallback(evicted.key, evicted.entry)
	}

	return true
}

// Get 获取条目
func (c *LFUCache) Get(key string) (*value.Entry, bool) {
	atomic.AddInt64(&c.accessCount, 1)

	c.mu.Lock()
	defer c.mu.Unlock()

	if item, exists := c.items[key]; exists {
		item.frequency++
		item.accessTime = time.Now().UnixNano()
		heap.Fix(c.frequencies, item.index)
		atomic.AddInt64(&c.hitCount, 1)
		return item.entry, true
	}

	atomic.AddInt64(&c.missCount, 1)
	return nil, false
}

// Remove 删除条目（在锁内执行删除，锁外执行回调）
func (c *LFUCache) Remove(key string) bool {
	c.mu.Lock()
	if item, exists := c.items[key]; exists {
		// 在锁内移除堆和 map，但不要调用回调
		heap.Remove(c.frequencies, item.index)
		delete(c.items, key)
		c.currentSize--
		entry := item.entry
		c.mu.Unlock()

		if c.evictCallback != nil {
			c.evictCallback(key, entry)
		}
		return true
	}
	c.mu.Unlock()
	return false
}

// Contains 检查是否包含键
func (c *LFUCache) Contains(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.items[key]
	return exists
}

// Len 获取当前大小
func (c *LFUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentSize
}

// Clear 清空缓存（收集回调，锁外执行）
func (c *LFUCache) Clear() {
	c.mu.Lock()
	var toCallback []evictedCallbackItem
	if c.evictCallback != nil {
		for k, it := range c.items {
			if it.entry != nil {
				toCallback = append(toCallback, evictedCallbackItem{key: k, entry: it.entry})
			}
		}
	}

	c.items = make(map[string]*lfuCacheItem)
	c.frequencies = &lfuHeap{}
	heap.Init(c.frequencies)
	c.currentSize = 0
	c.mu.Unlock()

	if c.evictCallback != nil {
		for _, e := range toCallback {
			c.evictCallback(e.key, e.entry)
		}
	}
}

// evictedCallbackItem 用于在锁外执行回调
type evictedCallbackItem struct {
	key   string
	entry *value.Entry
}

// SetEvictCallback 设置淘汰回调
func (c *LFUCache) SetEvictCallback(callback EvictCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictCallback = callback
}

// Stats 获取统计信息
func (c *LFUCache) Stats() *Stats {
	c.mu.RLock()
	currentSize := c.currentSize
	c.mu.RUnlock()

	accessCount := atomic.LoadInt64(&c.accessCount)
	evictionCount := atomic.LoadInt64(&c.evictionCount)
	hitCount := atomic.LoadInt64(&c.hitCount)
	missCount := atomic.LoadInt64(&c.missCount)

	var hitRate float64
	if accessCount > 0 {
		hitRate = float64(hitCount) / float64(accessCount)
	}

	return &Stats{
		Type:          "lfu",
		TotalKeys:     int64(currentSize),
		AccessCount:   accessCount,
		EvictionCount: evictionCount,
		HitRate:       hitRate,
		Extra: map[string]interface{}{
			"hit_count":   hitCount,
			"miss_count":  missCount,
			"max_entries": c.maxEntries,
		},
	}
}

// evictLFULocked 在持锁状态下执行堆弹出与内部删除，返回被淘汰项（不要在锁内调用回调）
func (c *LFUCache) evictLFULocked() *lfuCacheItem {
	if c.frequencies.Len() == 0 {
		return nil
	}
	victim := heap.Pop(c.frequencies).(*lfuCacheItem)
	delete(c.items, victim.key)
	c.currentSize--
	atomic.AddInt64(&c.evictionCount, 1)
	return victim
}
