package eviction

import (
	"sync"
	"sync/atomic"
	"time"

	"turbocache/internal/cache/value"
)

// ==============================
// shared types / 数据结构
// ==============================

// 被真正淘汰（需要回调）的项（供策略返回给缓存包装器以便解锁后回调）
type evictedItem struct {
	key   string
	entry *value.Entry
}

// 双向链表节点：存储 key + 可选 entry（entry == nil 表示 ghost 节点）
type arcItem struct {
	key        string
	entry      *value.Entry
	accessTime int64
	prev       *arcItem
	next       *arcItem
	list       *arcList
}

// 带头尾哨兵的双向链表，size 为实际元素数量（不含哨兵）
type arcList struct {
	name string
	head *arcItem
	tail *arcItem
	size int64
}

// 创建新链表（带哨兵节点）
func newARCList(name string) *arcList {
	list := &arcList{name: name}
	list.head = &arcItem{}
	list.tail = &arcItem{}
	list.head.next = list.tail
	list.tail.prev = list.head
	list.size = 0
	return list
}

// ==============================
// ARCPolicy（策略实现）
// - 该实现负责核心 ARC 算法并返回被真正淘汰的 items 列表（供 ARCCache 解锁后回调）
// ==============================

type ARCPolicy struct {
	mu sync.RWMutex

	t1 *arcList
	t2 *arcList
	b1 *arcList
	b2 *arcList

	// 容量与自适应参数 p（均使用 int64，保持一致）
	c int64
	p int64

	// 当前真实缓存项（key -> node），ghost 列表使用 b1Map / b2Map 加速查找
	items map[string]*arcItem
	b1Map map[string]*arcItem
	b2Map map[string]*arcItem

	// 统计计数（原子更新）
	accessCount   int64
	evictionCount int64
	hitCount      int64
	missCount     int64

	config *Config
}

// 构造函数：传入 Config（含 MaxEntries）
func NewARCPolicy(config *Config) *ARCPolicy {
	if config == nil {
		config = &Config{MaxEntries: 10000}
	}

	arc := &ARCPolicy{
		c:      config.MaxEntries,
		p:      0,
		items:  make(map[string]*arcItem),
		b1Map:  make(map[string]*arcItem),
		b2Map:  make(map[string]*arcItem),
		config: config,
	}

	arc.t1 = newARCList("T1")
	arc.t2 = newARCList("T2")
	arc.b1 = newARCList("B1")
	arc.b2 = newARCList("B2")

	return arc
}

// OnAccess：模拟访问（只做统计与命中处理），加锁内部处理链表移动
func (arc *ARCPolicy) OnAccess(key string) {
	atomic.AddInt64(&arc.accessCount, 1)

	arc.mu.Lock()
	defer arc.mu.Unlock()

	if item, exists := arc.items[key]; exists {
		arc.handleHit(item)
		atomic.AddInt64(&arc.hitCount, 1)
	} else {
		atomic.AddInt64(&arc.missCount, 1)
	}
}

// OnSet：设置/更新条目；如果已存在当作命中处理，否则按 miss 处理。
// 注意：策略层将返回被淘汰的 items（如果需要），但 ARCPolicy 作为纯策略通常不直接回调外部。
// 为了兼容旧接口，这里忽略返回值；外层 ARCCache 调用时会接收并处理返回值。
func (arc *ARCPolicy) OnSet(key string, entry *value.Entry) {
	arc.mu.Lock()
	defer arc.mu.Unlock()

	if item, exists := arc.items[key]; exists {
		item.entry = entry
		item.accessTime = time.Now().UnixNano()
		arc.handleHit(item)
	} else {
		// 调用 handleMiss 并丢弃返回的 evicted items（如需回调请用 ARCCache）
		_ = arc.handleMiss(key, entry)
	}
}

// OnDelete：从真实缓存与幽灵列表中删除 key
func (arc *ARCPolicy) OnDelete(key string) {
	arc.mu.Lock()
	defer arc.mu.Unlock()

	if item, exists := arc.items[key]; exists {
		arc.removeFromList(item)
		delete(arc.items, key)
	}
	if ghostItem, ok := arc.b1Map[key]; ok {
		arc.removeFromList(ghostItem)
		delete(arc.b1Map, key)
	}
	if ghostItem, ok := arc.b2Map[key]; ok {
		arc.removeFromList(ghostItem)
		delete(arc.b2Map, key)
	}
}

// SelectEvictKeys：用于外部查看可能被淘汰的键（只遍历 T1/T2 的尾部）
// 返回不超过 count 的键（不会修改任何状态）
func (arc *ARCPolicy) SelectEvictKeys(count int) []string {
	if count <= 0 {
		return nil
	}

	arc.mu.RLock()
	defer arc.mu.RUnlock()

	var keys []string
	collected := 0
	t1Count := int(arc.t1.size)
	t2Count := int(arc.t2.size)
	if t1Count+t2Count == 0 {
		return nil
	}

	// 先从 T1 的尾部向前收集
	current := arc.t1.tail.prev
	for i := 0; i < t1Count && current != nil && current != arc.t1.head && collected < count; i++ {
		keys = append(keys, current.key)
		collected++
		current = current.prev
	}

	// 不够再从 T2 的尾部向前收集
	if collected < count {
		current = arc.t2.tail.prev
		for i := 0; i < t2Count && current != nil && current != arc.t2.head && collected < count; i++ {
			keys = append(keys, current.key)
			collected++
			current = current.prev
		}
	}

	return keys
}

// Stats：收集统计信息（只读）
func (arc *ARCPolicy) Stats() *Stats {
	arc.mu.RLock()
	t1Size := arc.t1.size
	t2Size := arc.t2.size
	b1Size := arc.b1.size
	b2Size := arc.b2.size
	targetP := arc.p
	arc.mu.RUnlock()

	accessCount := atomic.LoadInt64(&arc.accessCount)
	evictionCount := atomic.LoadInt64(&arc.evictionCount)
	hitCount := atomic.LoadInt64(&arc.hitCount)
	missCount := atomic.LoadInt64(&arc.missCount)

	var hitRate float64
	if accessCount > 0 {
		hitRate = float64(hitCount) / float64(accessCount)
	}

	return &Stats{
		Type:          "arc",
		TotalKeys:     t1Size + t2Size,
		AccessCount:   accessCount,
		EvictionCount: evictionCount,
		HitRate:       hitRate,
		Extra: map[string]interface{}{
			"hit_count":   hitCount,
			"miss_count":  missCount,
			"t1_size":     t1Size,
			"t2_size":     t2Size,
			"b1_size":     b1Size,
			"b2_size":     b2Size,
			"target_p":    targetP,
			"max_entries": arc.c,
		},
	}
}

// Reset：重置策略状态
func (arc *ARCPolicy) Reset() {
	arc.mu.Lock()
	defer arc.mu.Unlock()

	arc.items = make(map[string]*arcItem)
	arc.b1Map = make(map[string]*arcItem)
	arc.b2Map = make(map[string]*arcItem)
	arc.t1 = newARCList("T1")
	arc.t2 = newARCList("T2")
	arc.b1 = newARCList("B1")
	arc.b2 = newARCList("B2")
	arc.p = 0

	atomic.StoreInt64(&arc.accessCount, 0)
	atomic.StoreInt64(&arc.evictionCount, 0)
	atomic.StoreInt64(&arc.hitCount, 0)
	atomic.StoreInt64(&arc.missCount, 0)
}

// Close：资源清理（目前只是 reset）
func (arc *ARCPolicy) Close() error {
	arc.Reset()
	return nil
}

// -----------------------------
// ARC 核心实现：handleHit / handleMiss / replace
// 设计要点：
// - handleMiss 返回被真正淘汰（entry != nil）的项列表，供上层在解锁后回调
// - ghost 列表使用 b1Map/b2Map 做 O(1) 查找
// -----------------------------

// handleHit：命中时处理（把 T1 -> T2，或把 T2 移到头部）
func (arc *ARCPolicy) handleHit(item *arcItem) {
	item.accessTime = time.Now().UnixNano()

	if item.list == arc.t1 {
		arc.removeFromList(item)
		arc.addToHead(arc.t2, item)
	} else if item.list == arc.t2 {
		arc.removeFromList(item)
		arc.addToHead(arc.t2, item)
	}
}

// handleMiss：处理未命中，返回被淘汰的真实条目列表
func (arc *ARCPolicy) handleMiss(key string, entry *value.Entry) []evictedItem {
	var toCallback []evictedItem

	// 先检查幽灵命中
	if ev, handled := arc.handleGhostHit(key, entry); handled {
		// 若 ghost 命中，handleGhostHit 会在内部做 promote/replace 并返回被淘汰的项
		if len(ev) > 0 {
			toCallback = append(toCallback, ev...)
		}
		return toCallback
	}

	// 真实新插入到 T1
	item := &arcItem{
		key:        key,
		entry:      entry,
		accessTime: time.Now().UnixNano(),
	}
	arc.items[key] = item

	// 如果真实缓存已满，执行替换（replace 会返回真正被淘汰的 entry）
	if arc.t1.size+arc.t2.size >= arc.c {
		ev := arc.replace()
		if len(ev) > 0 {
			toCallback = append(toCallback, ev...)
		}
	}

	arc.addToHead(arc.t1, item)
	return toCallback
}

// handleGhostHit：处理 ghost 列表命中（b1 或 b2），返回被淘汰的项以及是否处理成功
func (arc *ARCPolicy) handleGhostHit(key string, entry *value.Entry) ([]evictedItem, bool) {
	var toCallback []evictedItem

	// b1 ghost hit: 增加 p
	if ghostItem, ok := arc.b1Map[key]; ok {
		delta := int64(1)
		if arc.b1.size > 0 && arc.b2.size > 0 {
			delta = max(1, arc.b2.size/arc.b1.size)
		}
		arc.p = min(arc.c, arc.p+delta)

		// 从 b1 中移除并删除 map 记录
		arc.removeFromList(ghostItem)
		delete(arc.b1Map, key)

		// promote：设置 entry，加入 items map（成为真实缓存）
		ghostItem.entry = entry
		ghostItem.accessTime = time.Now().UnixNano()
		arc.items[key] = ghostItem

		// 如需替换，收集被淘汰项
		if arc.t1.size+arc.t2.size >= arc.c {
			ev := arc.replace()
			if len(ev) > 0 {
				toCallback = append(toCallback, ev...)
			}
		}

		arc.addToHead(arc.t2, ghostItem)
		return toCallback, true
	}

	// b2 ghost hit: 减小 p
	if ghostItem, ok := arc.b2Map[key]; ok {
		delta := int64(1)
		if arc.b2.size > 0 && arc.b1.size > 0 {
			delta = max(1, arc.b1.size/arc.b2.size)
		}
		arc.p = max(0, arc.p-delta)

		arc.removeFromList(ghostItem)
		delete(arc.b2Map, key)

		ghostItem.entry = entry
		ghostItem.accessTime = time.Now().UnixNano()
		arc.items[key] = ghostItem

		if arc.t1.size+arc.t2.size >= arc.c {
			ev := arc.replace()
			if len(ev) > 0 {
				toCallback = append(toCallback, ev...)
			}
		}

		arc.addToHead(arc.t2, ghostItem)
		return toCallback, true
	}

	return nil, false
}

// replace：按照 ARC 规则从 T1 或 T2 淘汰一个条目，返回被淘汰的真实 entry 列表
func (arc *ARCPolicy) replace() []evictedItem {
	var toCallback []evictedItem

	// 按 ARC 规则决定从 T1 还是 T2 淘汰
	if arc.t1.size > 0 &&
		(arc.t1.size > arc.p || (arc.t1.size == arc.p && arc.b2.size > 0)) {

		victim := arc.t1.tail.prev
		if victim != nil && victim != arc.t1.head {
			arc.removeFromList(victim)

			if victim.entry != nil {
				// 收集被淘汰的真实 entry（用于回调）
				toCallback = append(toCallback, evictedItem{key: victim.key, entry: victim.entry})
				// 从活跃 items map 中删除
				delete(arc.items, victim.key)
				// 把 node 转为 ghost（清空 entry，放到 b1，并登记到 b1Map）
				victim.entry = nil
				arc.addToHead(arc.b1, victim)
				arc.b1Map[victim.key] = victim
				atomic.AddInt64(&arc.evictionCount, 1)
			}
		}
	} else if arc.t2.size > 0 {
		victim := arc.t2.tail.prev
		if victim != nil && victim != arc.t2.head {
			arc.removeFromList(victim)

			if victim.entry != nil {
				toCallback = append(toCallback, evictedItem{key: victim.key, entry: victim.entry})
				delete(arc.items, victim.key)
				victim.entry = nil
				arc.addToHead(arc.b2, victim)
				arc.b2Map[victim.key] = victim
				atomic.AddInt64(&arc.evictionCount, 1)
			}
		}
	}

	// 裁剪 ghost 列表到上限
	arc.limitGhostLists()
	return toCallback
}

// limitGhostLists：保证 b1/b2 不超过 c（超过则从尾部删除并维护 map）
func (arc *ARCPolicy) limitGhostLists() {
	for arc.b1.size > arc.c {
		victim := arc.b1.tail.prev
		if victim != nil && victim != arc.b1.head {
			arc.removeFromList(victim)
			delete(arc.b1Map, victim.key)
		} else {
			break
		}
	}

	for arc.b2.size > arc.c {
		victim := arc.b2.tail.prev
		if victim != nil && victim != arc.b2.head {
			arc.removeFromList(victim)
			delete(arc.b2Map, victim.key)
		} else {
			break
		}
	}
}

// addToHead：把 item 放到指定链表头部（含从其他列表移动的逻辑）
// 说明：调用者负责在需要时维护 b1Map/b2Map（例如在 promote/remove 时处理 map）
// 本函数会维护 list.size、item.list 与 prev/next 指针
func (arc *ARCPolicy) addToHead(list *arcList, item *arcItem) {
	if list == nil || item == nil {
		return
	}

	// 若 item 已在目标列表内：将其移动到头部（size 不变）
	if item.list == list {
		// 从当前位置摘下（保持 size 不变）
		if item.prev != nil && item.next != nil {
			item.prev.next = item.next
			item.next.prev = item.prev
		}
		// 插到头部
		item.prev = list.head
		item.next = list.head.next
		if list.head.next != nil {
			list.head.next.prev = item
		}
		list.head.next = item
		// item.list 保持不变
		return
	}

	// 若 item 在其他列表中：从原列表完全移除（减小原列表 size）
	if item.list != nil {
		if item.prev != nil && item.next != nil {
			item.prev.next = item.next
			item.next.prev = item.prev
			item.list.size--
		}
		item.prev = nil
		item.next = nil
		item.list = nil
	}

	// 插入到目标列表头部
	item.prev = list.head
	item.next = list.head.next
	if list.head.next != nil {
		list.head.next.prev = item
	}
	list.head.next = item
	item.list = list
	list.size++
}

// removeFromList：从当前 list 中移除节点并清理指针与 size
func (arc *ARCPolicy) removeFromList(item *arcItem) {
	if item == nil || item.list == nil || item.prev == nil || item.next == nil {
		return
	}

	item.prev.next = item.next
	item.next.prev = item.prev
	item.list.size--
	item.list = nil
	item.prev = nil
	item.next = nil
}

// ==============================
// ARCCache（缓存包装器）
// - 复用 ARCPolicy，实现回调逻辑：由策略返回被淘汰项，ARCCache 在解锁后调用回调
// - 使用 double-check 模式避免并发插入重复问题
// ==============================

type ARCCache struct {
	policy        *ARCPolicy
	evictCallback EvictCallback
	mu            sync.Mutex // 保护 evictCallback 的设置与读取（轻量）
}

// NewARCCache：创建 ARCCache，统一通过 ARCPolicy 实现策略
func NewARCCache(maxEntries int) *ARCCache {
	return &ARCCache{
		policy: NewARCPolicy(&Config{MaxEntries: int64(maxEntries)}),
	}
}

// Add：添加或更新条目
// 返回 true 表示执行了“新插入”（即 key 原先不存在）；
// 返回 false 表示 key 已存在并被更新（视为命中）。
// 关键并发处理：先 RLock 快速检测，若不存在则升级为 Lock 并 double-check，最后在持锁下调用 policy.handleMiss
func (c *ARCCache) Add(key string, entry *value.Entry) bool {
	// 快速路径：读锁检查是否已存在
	c.policy.mu.RLock()
	_, exists := c.policy.items[key]
	c.policy.mu.RUnlock()

	if exists {
		// 存在则当作更新（命中），直接调用 policy.OnSet（内部会加锁）
		c.policy.OnSet(key, entry)
		return false
	}

	// 若不存在，进入写锁前的准备：升级为写锁并 double-check（避免并发插入）
	c.policy.mu.Lock()
	// double-check：其他 goroutine 可能在上面释放 RLock 到这里之间插入了该 key
	if item, exists2 := c.policy.items[key]; exists2 {
		// 已被插入：当作更新处理，释放锁并返回 false
		item.entry = entry
		item.accessTime = time.Now().UnixNano()
		c.policy.handleHit(item)
		c.policy.mu.Unlock()
		return false
	}

	// 到这里可以安全地在策略中执行 handleMiss（策略将返回被淘汰的真实条目列表）
	toEvict := c.policy.handleMiss(key, entry)
	c.policy.mu.Unlock()

	// 在解锁后调用回调（如果有），避免在持锁时执行用户代码
	if len(toEvict) > 0 {
		c.mu.Lock()
		callback := c.evictCallback
		c.mu.Unlock()

		if callback != nil {
			for _, e := range toEvict {
				// 仅当 entry 不为 nil 时回调（策略保证被淘汰的 entry != nil）
				if e.entry != nil {
					callback(e.key, e.entry)
				}
			}
		}
	}

	return true
}

// Get：获取条目，命中则移动到 T2（内部持写锁修改链表）
func (c *ARCCache) Get(key string) (*value.Entry, bool) {
	atomic.AddInt64(&c.policy.accessCount, 1)

	c.policy.mu.Lock()
	defer c.policy.mu.Unlock()

	if item, exists := c.policy.items[key]; exists {
		c.policy.handleHit(item)
		atomic.AddInt64(&c.policy.hitCount, 1)
		return item.entry, true
	}

	atomic.AddInt64(&c.policy.missCount, 1)
	return nil, false
}

// Remove：删除 key，如果有真实 entry 则在解锁后调用回调
func (c *ARCCache) Remove(key string) bool {
	c.policy.mu.Lock()
	var entry *value.Entry

	if item, exists := c.policy.items[key]; exists {
		entry = item.entry
		c.policy.removeFromList(item)
		delete(c.policy.items, key)
	}

	// 同时从 ghost map 中删掉（若存在）
	if ghostItem, ok := c.policy.b1Map[key]; ok {
		c.policy.removeFromList(ghostItem)
		delete(c.policy.b1Map, key)
	}
	if ghostItem, ok := c.policy.b2Map[key]; ok {
		c.policy.removeFromList(ghostItem)
		delete(c.policy.b2Map, key)
	}

	c.policy.mu.Unlock()

	// 若有真实 entry，则回调（在非持锁状态）
	if entry != nil {
		c.mu.Lock()
		callback := c.evictCallback
		c.mu.Unlock()

		if callback != nil {
			callback(key, entry)
		}
		return true
	}
	return false
}

// Contains：是否存在于当前真实缓存（不含 ghost）
func (c *ARCCache) Contains(key string) bool {
	c.policy.mu.RLock()
	defer c.policy.mu.RUnlock()
	_, exists := c.policy.items[key]
	return exists
}

// Len：返回当前真实缓存项数量（T1 + T2）
func (c *ARCCache) Len() int {
	c.policy.mu.RLock()
	defer c.policy.mu.RUnlock()
	return int(c.policy.t1.size + c.policy.t2.size)
}

// Clear：清空缓存，若有回调则在解锁后调用所有真实 entry 的回调
func (c *ARCCache) Clear() {
	c.policy.mu.Lock()
	var toCallback []evictedItem

	c.mu.Lock()
	callback := c.evictCallback
	c.mu.Unlock()

	if callback != nil {
		for key, item := range c.policy.items {
			if item.entry != nil {
				toCallback = append(toCallback, evictedItem{key: key, entry: item.entry})
			}
		}
	}

	// 重置策略内部结构
	c.policy.items = make(map[string]*arcItem)
	c.policy.t1 = newARCList("T1")
	c.policy.t2 = newARCList("T2")
	c.policy.b1 = newARCList("B1")
	c.policy.b2 = newARCList("B2")
	c.policy.b1Map = make(map[string]*arcItem)
	c.policy.b2Map = make(map[string]*arcItem)
	c.policy.p = 0

	atomic.StoreInt64(&c.policy.accessCount, 0)
	atomic.StoreInt64(&c.policy.evictionCount, 0)
	atomic.StoreInt64(&c.policy.hitCount, 0)
	atomic.StoreInt64(&c.policy.missCount, 0)

	c.policy.mu.Unlock()

	// 在外部调用回调
	if callback != nil {
		for _, e := range toCallback {
			callback(e.key, e.entry)
		}
	}
}

// SetEvictCallback：设置用户的淘汰回调（保护回调指针的并发访问）
func (c *ARCCache) SetEvictCallback(callback EvictCallback) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.evictCallback = callback
}

// Stats：转发到策略获取统计信息
func (c *ARCCache) Stats() *Stats {
	return c.policy.Stats()
}

// ==============================
// 辅助函数
// ==============================
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
