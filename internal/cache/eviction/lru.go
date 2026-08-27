package eviction

import (
	"sync"
	"sync/atomic"
	"time"

	"turbocache/internal/cache/value"
)

// LRUPolicy LRU淘汰策略实现,实现Policy接口的LRU策略
type LRUPolicy struct {
	mu          sync.RWMutex
	items       map[string]*lruItem
	head        *lruItem // 最近使用的项目
	tail        *lruItem // 最久未使用的项目
	maxEntries  int64
	currentSize int64

	// 统计信息
	accessCount   int64
	evictionCount int64
	hitCount      int64
	missCount     int64

	config *Config
}

// lruItem LRU项目,LRU链表节点，包含键值对和访问时间信息
type lruItem struct {
	key        string
	entry      *value.Entry
	accessTime int64
	prev       *lruItem
	next       *lruItem
}

// NewLRUPolicy 创建LRU策略
func NewLRUPolicy(config *Config) *LRUPolicy {
	if config == nil {
		config = &Config{
			MaxEntries: 10000,
		}
	}

	lru := &LRUPolicy{
		items:      make(map[string]*lruItem),
		maxEntries: config.MaxEntries,
		config:     config,
	}

	// 创建哨兵节点
	lru.head = &lruItem{}
	lru.tail = &lruItem{}
	lru.head.next = lru.tail
	lru.tail.prev = lru.head

	return lru
}

// OnAccess 记录访问事件
func (lru *LRUPolicy) OnAccess(key string) {
	atomic.AddInt64(&lru.accessCount, 1)

	lru.mu.Lock()
	defer lru.mu.Unlock()

	if item, exists := lru.items[key]; exists {
		// 移动到头部（最近使用）
		lru.moveToHead(item)
		item.accessTime = time.Now().UnixNano()
		atomic.AddInt64(&lru.hitCount, 1)
	} else {
		atomic.AddInt64(&lru.missCount, 1)
	}
}

// OnSet 记录设置事件
func (lru *LRUPolicy) OnSet(key string, entry *value.Entry) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if item, exists := lru.items[key]; exists {
		// 更新现有项目
		item.entry = entry
		item.accessTime = time.Now().UnixNano()
		lru.moveToHead(item)
	} else {
		// 创建新项目
		item := &lruItem{
			key:        key,
			entry:      entry,
			accessTime: time.Now().UnixNano(),
		}

		lru.items[key] = item
		lru.addToHead(item)
		lru.currentSize++

		// 检查是否需要淘汰
		if lru.maxEntries > 0 && lru.currentSize > lru.maxEntries {
			lru.evictLRU()
		}
	}
}

// OnDelete 记录删除事件
func (lru *LRUPolicy) OnDelete(key string) {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	if item, exists := lru.items[key]; exists {
		lru.removeItem(item)
		delete(lru.items, key)
		lru.currentSize--
	}
}

// SelectEvictKeys 选择要淘汰的键（防止链表异常导致无限循环），可以一下子批量选择很多的
func (lru *LRUPolicy) SelectEvictKeys(count int) []string {
	if count <= 0 {
		return nil
	}

	lru.mu.RLock()
	defer lru.mu.RUnlock()

	var keys []string
	current := lru.tail.prev
	collected := 0

	// 安全上限：最多遍历 items 数量次，以防链表结构损坏造成无限循环
	maxIter := len(lru.items)
	if maxIter <= 0 {
		return nil
	}

	for i := 0; i < maxIter && current != lru.head && collected < count; i++ {
		if current == nil {
			break
		}
		keys = append(keys, current.key)
		collected++
		current = current.prev
	}

	return keys
}

// Stats 获取统计信息
func (lru *LRUPolicy) Stats() *Stats {
	lru.mu.RLock()
	currentSize := lru.currentSize
	lru.mu.RUnlock()

	accessCount := atomic.LoadInt64(&lru.accessCount)
	evictionCount := atomic.LoadInt64(&lru.evictionCount)
	hitCount := atomic.LoadInt64(&lru.hitCount)
	missCount := atomic.LoadInt64(&lru.missCount)

	var hitRate float64
	if accessCount > 0 {
		hitRate = float64(hitCount) / float64(accessCount)
	}

	return &Stats{
		Type:          "lru",
		TotalKeys:     currentSize,
		AccessCount:   accessCount,
		EvictionCount: evictionCount,
		HitRate:       hitRate,
		Extra: map[string]interface{}{
			"hit_count":   hitCount,
			"miss_count":  missCount,
			"max_entries": lru.maxEntries,
		},
	}
}

// Reset 重置策略
func (lru *LRUPolicy) Reset() {
	lru.mu.Lock()
	defer lru.mu.Unlock()

	lru.items = make(map[string]*lruItem)
	lru.head.next = lru.tail
	lru.tail.prev = lru.head
	lru.currentSize = 0

	atomic.StoreInt64(&lru.accessCount, 0)
	atomic.StoreInt64(&lru.evictionCount, 0)
	atomic.StoreInt64(&lru.hitCount, 0)
	atomic.StoreInt64(&lru.missCount, 0)
}

// Close 关闭策略
func (lru *LRUPolicy) Close() error {
	lru.Reset()
	return nil
}

// addToHead 添加项目到头部（带空指针保护）
func (lru *LRUPolicy) addToHead(item *lruItem) {
	if item == nil || lru.head == nil {
		return
	}
	item.prev = lru.head
	item.next = lru.head.next
	if lru.head.next != nil {
		lru.head.next.prev = item
	}
	lru.head.next = item
}

// removeItem 移除项目（带安全检查，防止破坏链表导致循环）
func (lru *LRUPolicy) removeItem(item *lruItem) {
	if item == nil || item.prev == nil || item.next == nil {
		return
	}
	item.prev.next = item.next
	item.next.prev = item.prev
	// 断开被移除节点的指针，避免悬挂引用
	item.prev = nil
	item.next = nil
}

// moveToHead 移动项目到头部
func (lru *LRUPolicy) moveToHead(item *lruItem) {
	// 先尝试安全移除，再加入头部
	lru.removeItem(item)
	lru.addToHead(item)
}

// removeTail 移除尾部项目
func (lru *LRUPolicy) removeTail() *lruItem {
	if lru.tail == nil || lru.head == nil {
		return nil
	}
	last := lru.tail.prev
	if last == nil || last == lru.head {
		return nil
	}
	lru.removeItem(last)
	return last
}

// evictLRU 淘汰最久未使用的项目
func (lru *LRUPolicy) evictLRU() {
	victim := lru.removeTail()
	if victim != nil {
		delete(lru.items, victim.key)
		lru.currentSize--
		atomic.AddInt64(&lru.evictionCount, 1)
	}
}

// LRUCache LRU缓存实现（实现Evictor接口）,实现Evictor接口的LRU缓存
type LRUCache struct {
	mu          sync.RWMutex
	items       map[string]*lruItem
	head        *lruItem
	tail        *lruItem
	maxEntries  int
	currentSize int

	evictCallback EvictCallback

	// 统计信息
	accessCount   int64
	evictionCount int64
	hitCount      int64
	missCount     int64
}

// NewLRUCache 创建LRU缓存
func NewLRUCache(maxEntries int) *LRUCache {
	cache := &LRUCache{
		items:      make(map[string]*lruItem),
		maxEntries: maxEntries,
	}

	// 创建哨兵节点
	cache.head = &lruItem{}
	cache.tail = &lruItem{}
	cache.head.next = cache.tail
	cache.tail.prev = cache.head

	return cache
}

// Add 添加条目
func (c *LRUCache) Add(key string, entry *value.Entry) bool {
	c.mu.Lock()

	if item, exists := c.items[key]; exists {
		// 更新现有项目
		oldEntry := item.entry
		item.entry = entry
		item.accessTime = time.Now().UnixNano()
		c.moveToHead(item)

		// 先抓住回调引用，之后在锁外调用
		cb := c.evictCallback
		c.mu.Unlock()

		if cb != nil {
			cb(key, oldEntry)
		}

		return false // 表示是更新操作
	}

	// 创建新项目
	item := &lruItem{
		key:        key,
		entry:      entry,
		accessTime: time.Now().UnixNano(),
	}

	c.items[key] = item
	c.addToHead(item)
	c.currentSize++

	// 特殊处理：容量为0时，立即淘汰刚添加的条目（保持语义：允许添加但立即淘汰）
	if c.maxEntries == 0 {
		cb := c.evictCallback
		vkey, ventry, ok := c.evictLRU()
		c.mu.Unlock()

		if ok && cb != nil {
			cb(vkey, ventry)
		}

		return true
	}

	// 检查是否需要淘汰（常规情况）
	if c.maxEntries > 0 && c.currentSize > c.maxEntries {
		// evictLRU 在锁内完成删除与计数，但不要在锁内调用回调
		cb := c.evictCallback
		vkey, ventry, ok := c.evictLRU()
		c.mu.Unlock()

		if ok && cb != nil {
			cb(vkey, ventry)
		}

		return true // 表示新增
	}

	c.mu.Unlock()
	return true // 表示新增
}

// Get 获取条目
func (c *LRUCache) Get(key string) (*value.Entry, bool) {
	atomic.AddInt64(&c.accessCount, 1)

	c.mu.Lock()
	defer c.mu.Unlock()

	if item, exists := c.items[key]; exists {
		item.accessTime = time.Now().UnixNano()
		c.moveToHead(item)
		atomic.AddInt64(&c.hitCount, 1)
		return item.entry, true
	}

	atomic.AddInt64(&c.missCount, 1)
	return nil, false
}

// Remove 删除条目
func (c *LRUCache) Remove(key string) bool {
	c.mu.Lock()
	if item, exists := c.items[key]; exists {
		// 保存要回调的信息
		cb := c.evictCallback
		entry := item.entry

		c.removeItem(item)
		delete(c.items, key)
		c.currentSize--
		c.mu.Unlock()

		if cb != nil {
			cb(key, entry)
		}
		return true
	}
	c.mu.Unlock()
	return false
}

// Contains 检查是否包含键
func (c *LRUCache) Contains(key string) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists := c.items[key]
	return exists
}

// Len 获取当前大小
func (c *LRUCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.currentSize
}

// Clear 清空缓存
func (c *LRUCache) Clear() {
	// 收集要回调的项（在锁内）
	c.mu.Lock()
	var toNotify []struct {
		k string
		e *value.Entry
	}
	if c.evictCallback != nil {
		for key, item := range c.items {
			toNotify = append(toNotify, struct {
				k string
				e *value.Entry
			}{key, item.entry})
		}
	}

	// 清空内部结构
	c.items = make(map[string]*lruItem)
	c.head.next = c.tail
	c.tail.prev = c.head
	c.currentSize = 0
	c.mu.Unlock()

	// 在锁外调用回调
	for _, it := range toNotify {
		c.evictCallback(it.k, it.e)
	}
}

// SetEvictCallback 设置淘汰回调
func (c *LRUCache) SetEvictCallback(callback EvictCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.evictCallback = callback
}

// Stats 获取统计信息
func (c *LRUCache) Stats() *Stats {
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
		Type:          "lru",
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

// addToHead 添加项目到头部（带空指针保护）
func (c *LRUCache) addToHead(item *lruItem) {
	if item == nil || c.head == nil {
		return
	}
	item.prev = c.head
	item.next = c.head.next
	if c.head.next != nil {
		c.head.next.prev = item
	}
	c.head.next = item
}

// removeItem 移除项目（带安全检查）
func (c *LRUCache) removeItem(item *lruItem) {
	if item == nil || item.prev == nil || item.next == nil {
		return
	}
	item.prev.next = item.next
	item.next.prev = item.prev
	item.prev = nil
	item.next = nil
}

// moveToHead 移动项目到头部
func (c *LRUCache) moveToHead(item *lruItem) {
	c.removeItem(item)
	c.addToHead(item)
}

// removeTail 移除尾部项目
func (c *LRUCache) removeTail() *lruItem {
	if c.tail == nil || c.head == nil {
		return nil
	}
	last := c.tail.prev
	if last == nil || last == c.head {
		return nil
	}
	c.removeItem(last)
	return last
}

// evictLRU 淘汰最久未使用的项目，返回被淘汰的键和值（在持锁时调用）
func (c *LRUCache) evictLRU() (string, *value.Entry, bool) {
	victim := c.removeTail()
	if victim == nil {
		return "", nil, false
	}

	// 删除并更新计数（在持锁时完成）
	delete(c.items, victim.key)
	c.currentSize--
	atomic.AddInt64(&c.evictionCount, 1)

	return victim.key, victim.entry, true
}

// evictLRU (policy) earlier exists above for LRUPolicy
