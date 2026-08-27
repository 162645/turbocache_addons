package engine

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"turbocache/internal/cache/eviction"
	"turbocache/internal/cache/storage"
	"turbocache/internal/cache/value"
	"turbocache/internal/config"
	"turbocache/internal/utils/logger"
)

// L1Cache L1本地缓存引擎
type L1Cache struct {
	storage  storage.Storage
	eviction *eviction.Manager
	config   *config.L1CacheConfig
	logger   logger.Logger

	// 统计信息
	stats *L1Stats

	// 生命周期管理
	closed    int32
	closeOnce sync.Once

	// 新增：用于管理 L1 层自己启动的异步任务（如 Get 中的异步删除）
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// L1Stats L1缓存统计信息
type L1Stats struct {
	// 基本统计
	totalKeys int64
	totalSize int64
	maxSize   int64

	// 操作统计
	getCount    int64
	setCount    int64
	deleteCount int64
	clearCount  int64

	// 命中统计
	hitCount  int64
	missCount int64

	// 淘汰统计
	evictionCount int64
	expiredCount  int64

	// 时间统计
	startTime  int64
	lastAccess int64
	lastUpdate int64
}

// NewL1Cache 创建L1缓存实例
func NewL1Cache(config *config.L1CacheConfig) (*L1Cache, error) {
	if config == nil {
		return nil, fmt.Errorf("L1 cache config is required")
	}

	// 创建存储后端
	storageConfig := &storage.Config{
		MaxSize:       config.MaxSize,
		MaxEntries:    config.MaxEntries,
		ShardCount:    config.ShardCount,
		CleanInterval: time.Minute, // 固定清理间隔（可按需改）
	}

	memStorage := storage.NewMemoryStorage(storageConfig)

	// 创建淘汰策略管理器
	policy := config.EvictionPolicy
	if policy == "" {
		policy = "lru"
	}
	evictionConfig := &eviction.Config{
		Type:       policy,
		MaxEntries: config.MaxEntries,
	}

	evictionMgr, err := eviction.NewManager(evictionConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create eviction manager: %v", err)
	}

	cache := &L1Cache{
		storage:  memStorage,
		eviction: evictionMgr,
		config:   config,
		logger:   logger.WithField("component", "l1_cache"),
		stats: &L1Stats{
			maxSize:   config.MaxSize,
			startTime: time.Now().UnixNano(),
		},
		// 初始化 stop 通知通道
		stopCh: make(chan struct{}),
	}

	cache.logger.Infof("L1 cache initialized: max_size=%d, max_entries=%d, shards=%d, eviction=%s",
		config.MaxSize, config.MaxEntries, config.ShardCount, policy)

	return cache, nil
}

// Get 获取缓存值
func (c *L1Cache) Get(ctx context.Context, key string) (*value.Entry, error) {
	if c.isClosed() {
		return nil, fmt.Errorf("L1 cache is closed")
	}

	atomic.AddInt64(&c.stats.getCount, 1)
	atomic.StoreInt64(&c.stats.lastAccess, time.Now().UnixNano())

	entry, err := c.storage.Get(ctx, key)
	if err != nil {
		if storage.IsNotFound(err) {
			atomic.AddInt64(&c.stats.missCount, 1)
		}
		return nil, err
	}

	// 检查是否过期
	if entry.IsExpired() {
		// 异步删除过期条目 —— 由 L1 层管理（wg/stopCh），避免 Close 时残留并发调用下游组件
		if !c.isClosed() {
			c.wg.Add(1)
			go func(k string) {
				defer c.wg.Done()

				// 如果已经开始关闭，尽快返回
				select {
				case <-c.stopCh:
					return
				default:
				}

				// 执行删除；如果 Delete 阻塞（依赖下游），Close 会等待 wg 完成
				if deleteErr := c.storage.Delete(context.Background(), k); deleteErr == nil {
					// OnDelete 可能会修改 eviction 的内部结构
					// 因为 Close 会等待 wg，所以这里不会在 eviction 被关闭后调用
					c.eviction.OnDelete(k)
					atomic.AddInt64(&c.stats.expiredCount, 1)
				}
			}(key)
		}

		atomic.AddInt64(&c.stats.missCount, 1)
		return nil, storage.NewKeyNotFoundError(key)
	}

	// 记录命中和访问事件
	atomic.AddInt64(&c.stats.hitCount, 1)
	c.eviction.OnAccess(key)

	return entry, nil
}

// Set 设置缓存值
func (c *L1Cache) Set(ctx context.Context, key string, entry *value.Entry) error {
	if c.isClosed() {
		return fmt.Errorf("L1 cache is closed")
	}

	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}

	if entry == nil {
		return fmt.Errorf("entry cannot be nil")
	}

	atomic.AddInt64(&c.stats.setCount, 1)
	atomic.StoreInt64(&c.stats.lastUpdate, time.Now().UnixNano())

	// 检查是否需要淘汰
	if err := c.maybeEvict(entry.Size()); err != nil {
		return err
	}

	// 设置到存储
	err := c.storage.Set(ctx, key, entry)
	if err != nil {
		return err
	}

	// 记录设置事件
	c.eviction.OnSet(key, entry)

	// 更新统计信息
	c.updateSizeStats()

	return nil
}

// Delete 删除缓存值
func (c *L1Cache) Delete(ctx context.Context, key string) error {
	if c.isClosed() {
		return fmt.Errorf("L1 cache is closed")
	}

	atomic.AddInt64(&c.stats.deleteCount, 1)

	err := c.storage.Delete(ctx, key)
	if err != nil && !storage.IsNotFound(err) {
		return err
	}

	// 记录删除事件
	c.eviction.OnDelete(key)

	// 更新统计信息
	c.updateSizeStats()

	return nil
}

// Exists 检查键是否存在
func (c *L1Cache) Exists(ctx context.Context, key string) (bool, error) {
	if c.isClosed() {
		return false, fmt.Errorf("L1 cache is closed")
	}

	exists, err := c.storage.Exists(ctx, key)
	if err != nil {
		return false, err
	}

	if exists {
		c.eviction.OnAccess(key)
	}

	return exists, nil
}

// Clear 清空缓存
func (c *L1Cache) Clear(ctx context.Context) error {
	if c.isClosed() {
		return fmt.Errorf("L1 cache is closed")
	}

	atomic.AddInt64(&c.stats.clearCount, 1)

	err := c.storage.Clear(ctx)
	if err != nil {
		return err
	}

	// 重置淘汰策略
	c.eviction.Reset()

	// 重置统计信息
	c.resetSizeStats()

	return nil
}

// Stats 获取缓存统计信息
func (c *L1Cache) Stats() (*CacheStats, error) {
	if c.isClosed() {
		return nil, fmt.Errorf("L1 cache is closed")
	}

	storageStats, err := c.storage.Stats()
	if err != nil {
		return nil, err
	}

	evictionStats := c.eviction.Stats()

	// 构建统计信息
	stats := &CacheStats{
		Level:      "L1",
		Type:       "memory",
		TotalKeys:  atomic.LoadInt64(&c.stats.totalKeys),
		TotalSize:  atomic.LoadInt64(&c.stats.totalSize),
		MaxSize:    atomic.LoadInt64(&c.stats.maxSize),
		UsedMemory: storageStats.UsedMemory,

		// 操作统计
		GetCount:    atomic.LoadInt64(&c.stats.getCount),
		SetCount:    atomic.LoadInt64(&c.stats.setCount),
		DeleteCount: atomic.LoadInt64(&c.stats.deleteCount),
		ClearCount:  atomic.LoadInt64(&c.stats.clearCount),

		// 命中统计
		HitCount:  atomic.LoadInt64(&c.stats.hitCount),
		MissCount: atomic.LoadInt64(&c.stats.missCount),
		HitRate:   c.calculateHitRate(),

		// 淘汰统计
		EvictionCount: atomic.LoadInt64(&c.stats.evictionCount),
		ExpiredCount:  atomic.LoadInt64(&c.stats.expiredCount),

		// 时间统计
		StartTime:  time.Unix(0, atomic.LoadInt64(&c.stats.startTime)),
		LastAccess: time.Unix(0, atomic.LoadInt64(&c.stats.lastAccess)),
		LastUpdate: time.Unix(0, atomic.LoadInt64(&c.stats.lastUpdate)),

		// 淘汰策略统计
		EvictionPolicy: evictionStats,

		// 配置信息
		Config: map[string]interface{}{
			"max_size":        c.config.MaxSize,
			"max_entries":     c.config.MaxEntries,
			"shard_count":     c.config.ShardCount,
			"eviction_policy": c.config.EvictionPolicy,
			"ttl":             c.config.TTL.String(),
			"stats_window":    c.config.StatsWindow.String(),
		},
	}

	return stats, nil
}

// SetTTL 设置键的生存时间
func (c *L1Cache) SetTTL(ctx context.Context, key string, ttl time.Duration) error {
	if c.isClosed() {
		return fmt.Errorf("L1 cache is closed")
	}

	return c.storage.Expire(ctx, key, ttl)
}

// GetTTL 获取键的剩余生存时间
func (c *L1Cache) GetTTL(ctx context.Context, key string) (time.Duration, error) {
	if c.isClosed() {
		return 0, fmt.Errorf("L1 cache is closed")
	}

	return c.storage.TTL(ctx, key)
}

// Keys returns a point-in-time list of non-expired local keys. It is used by
// the cluster Bloom rebuild path and deliberately does not expose values.
func (c *L1Cache) Keys(ctx context.Context) ([]string, error) {
	if c.isClosed() {
		return nil, fmt.Errorf("L1 cache is closed")
	}
	iter, err := c.storage.Iterator(ctx)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	keys := make([]string, 0)
	for iter.Next() {
		entry, valueErr := iter.Value()
		if valueErr != nil || entry == nil || entry.IsExpired() {
			continue
		}
		keys = append(keys, iter.Key())
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return keys, nil
}

// Close 关闭缓存（顺序：标记关闭 -> 广播 stop -> 等待本层异步任务退出 -> 关闭子组件）
func (c *L1Cache) Close() error {
	var err error
	c.closeOnce.Do(func() {
		// 标记为已关闭，防止新请求进入
		atomic.StoreInt32(&c.closed, 1)

		// 广播 stop，唤醒所有本层 goroutine
		// 如果 stopCh 已为 nil（理论上不会），先做保护
		if c.stopCh != nil {
			close(c.stopCh)
		}

		// 等待 L1 层自身启动的异步任务（如 Get 中的异步删除）退出
		c.wg.Wait()

		// 之后再关闭 eviction 和 storage（避免它们在还有任务调用它们时被关闭）
		if c.eviction != nil {
			if closeErr := c.eviction.Close(); closeErr != nil {
				err = fmt.Errorf("failed to close eviction manager: %v", closeErr)
			}
		}

		if c.storage != nil {
			if closeErr := c.storage.Close(); closeErr != nil {
				if err == nil {
					err = fmt.Errorf("failed to close storage: %v", closeErr)
				}
			}
		}

		c.logger.Info("L1 cache closed")
	})

	return err
}

// isClosed 检查缓存是否已关闭
func (c *L1Cache) isClosed() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

// maybeEvict 检查是否需要淘汰并执行淘汰
func (c *L1Cache) maybeEvict(newEntrySize int64) error {
	currentSize, err := c.storage.Size()
	if err != nil {
		return err
	}

	// 检查大小限制
	if c.config.MaxSize > 0 && currentSize+newEntrySize > c.config.MaxSize {
		return c.evictToMakeSpace(newEntrySize)
	}

	// 检查条目数限制
	currentCount, err := c.storage.Count()
	if err != nil {
		return err
	}

	if c.config.MaxEntries > 0 && currentCount >= c.config.MaxEntries {
		return c.evictEntries(1)
	}

	return nil
}

// evictToMakeSpace 淘汰条目以释放空间
func (c *L1Cache) evictToMakeSpace(requiredSpace int64) error {
	currentSize, err := c.storage.Size()
	if err != nil {
		return err
	}

	targetSize := c.config.MaxSize - requiredSpace
	spaceToFree := currentSize - targetSize

	if spaceToFree <= 0 {
		return nil
	}

	// 估算需要淘汰的条目数
	avgEntrySize := int64(1024) // 默认估算值
	if count, err := c.storage.Count(); err == nil && count > 0 {
		avgEntrySize = currentSize / count
	}

	evictCount := int(spaceToFree/avgEntrySize) + 1
	return c.evictEntries(evictCount)
}

// evictEntries 淘汰指定数量的条目
func (c *L1Cache) evictEntries(count int) error {
	keysToEvict := c.eviction.SelectEvictKeys(count)

	ctx := context.Background()
	evictedCount := 0

	for _, key := range keysToEvict {
		if err := c.storage.Delete(ctx, key); err == nil {
			c.eviction.OnDelete(key)
			evictedCount++
		}
	}

	atomic.AddInt64(&c.stats.evictionCount, int64(evictedCount))

	if evictedCount == 0 && len(keysToEvict) > 0 {
		return fmt.Errorf("failed to evict any entries")
	}

	return nil
}

// updateSizeStats 更新大小统计信息
func (c *L1Cache) updateSizeStats() {
	if size, err := c.storage.Size(); err == nil {
		atomic.StoreInt64(&c.stats.totalSize, size)
	}

	if count, err := c.storage.Count(); err == nil {
		atomic.StoreInt64(&c.stats.totalKeys, count)
	}
}

// resetSizeStats 重置大小统计信息
func (c *L1Cache) resetSizeStats() {
	atomic.StoreInt64(&c.stats.totalSize, 0)
	atomic.StoreInt64(&c.stats.totalKeys, 0)
}

// calculateHitRate 计算命中率
func (c *L1Cache) calculateHitRate() float64 {
	hitCount := atomic.LoadInt64(&c.stats.hitCount)
	missCount := atomic.LoadInt64(&c.stats.missCount)

	totalRequests := hitCount + missCount
	if totalRequests == 0 {
		return 0
	}

	return float64(hitCount) / float64(totalRequests)
}

// CacheStats 缓存统计信息
type CacheStats struct {
	// 基本信息
	Level      string `json:"level"`
	Type       string `json:"type"`
	TotalKeys  int64  `json:"total_keys"`
	TotalSize  int64  `json:"total_size"`
	MaxSize    int64  `json:"max_size"`
	UsedMemory int64  `json:"used_memory"`

	// 操作统计
	GetCount    int64 `json:"get_count"`
	SetCount    int64 `json:"set_count"`
	DeleteCount int64 `json:"delete_count"`
	ClearCount  int64 `json:"clear_count"`

	// 命中统计
	HitCount  int64   `json:"hit_count"`
	MissCount int64   `json:"miss_count"`
	HitRate   float64 `json:"hit_rate"`

	// 淘汰统计
	EvictionCount int64 `json:"eviction_count"`
	ExpiredCount  int64 `json:"expired_count"`

	// 时间统计
	StartTime  time.Time `json:"start_time"`
	LastAccess time.Time `json:"last_access"`
	LastUpdate time.Time `json:"last_update"`

	// 淘汰策略统计
	EvictionPolicy *eviction.Stats `json:"eviction_policy"`

	// 配置信息
	Config map[string]interface{} `json:"config"`
}
