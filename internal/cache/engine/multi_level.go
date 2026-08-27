package engine

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"turbocache/internal/cache/value"
	"turbocache/internal/config"
	"turbocache/internal/utils/logger"
)

// ============================================================
// 🔧 新增：定义缓存接口，使测试可以注入 Mock
// ============================================================

// L1CacheInterface L1缓存接口
type L1CacheInterface interface {
	Get(ctx context.Context, key string) (*value.Entry, error)
	Set(ctx context.Context, key string, entry *value.Entry) error
	Delete(ctx context.Context, key string) error
	Clear(ctx context.Context) error
	Stats() (*CacheStats, error)
	Close() error
}

// L2CacheInterface L2缓存接口
type L2CacheInterface interface {
	Get(ctx context.Context, key string) (*value.Entry, error)
	Set(ctx context.Context, key string, entry *value.Entry) error
	Delete(ctx context.Context, key string) error
	Stats() (*L2CacheStats, error)
	Close() error
}

// 确保 L1Cache 和 L2Cache 实现接口（编译时检查）
var (
	_ L1CacheInterface = (*L1Cache)(nil)
	_ L2CacheInterface = (*L2Cache)(nil)
)

// ============================================================
// MultiLevelCache 多级缓存管理器
// ============================================================

// MultiLevelCache 多级缓存管理器
type MultiLevelCache struct {
	l1Cache  L1CacheInterface // 🔧 改为接口类型
	l2Cache  L2CacheInterface // 🔧 改为接口类型
	config   *config.CacheConfig
	logger   logger.Logger
	strategy CacheStrategy
	stats    *MultiLevelStats
	closed   int32
	mu       sync.RWMutex
}

// CacheStrategy 缓存策略
type CacheStrategy int

const (
	// WriteThrough 写穿透：同时写L1和L2
	WriteThrough CacheStrategy = iota
	// WriteBack 写回：先写L1，异步写L2
	WriteBack
	// WriteAround 写绕过：只写L2，不写L1
	WriteAround
)

// MultiLevelStats 多级缓存统计信息
type MultiLevelStats struct {
	// 基本统计
	totalRequests int64
	l1Hits        int64
	l2Hits        int64
	misses        int64

	// 操作统计
	getCount    int64
	setCount    int64
	deleteCount int64

	// 性能统计
	avgL1Latency int64 // 纳秒
	avgL2Latency int64 // 纳秒

	// 时间统计
	startTime  int64
	lastAccess int64
	lastUpdate int64
}

// ============================================================
// 🔧 修改：构造函数接受接口类型
// ============================================================

// NewMultiLevelCache 创建多级缓存管理器
func NewMultiLevelCache(
	config *config.CacheConfig,
	l1Cache L1CacheInterface,
	l2Cache L2CacheInterface,
) *MultiLevelCache {
	mlc := &MultiLevelCache{
		l1Cache:  l1Cache,
		l2Cache:  l2Cache,
		config:   config,
		logger:   logger.WithField("component", "multi_level_cache"),
		strategy: WriteThrough,
		stats: &MultiLevelStats{
			startTime: time.Now().UnixNano(),
		},
	}

	mlc.logger.Info("Multi-level cache initialized")
	return mlc
}

// ============================================================
// 🔧 新增：导出访问器方法（供测试使用）
// ============================================================

// L1 返回 L1 缓存接口（用于测试）
func (mlc *MultiLevelCache) L1() L1CacheInterface {
	return mlc.l1Cache
}

// L2 返回 L2 缓存接口（用于测试）
func (mlc *MultiLevelCache) L2() L2CacheInterface {
	return mlc.l2Cache
}

// ============================================================
// 核心方法实现
// ============================================================

// Get 多级缓存获取
func (mlc *MultiLevelCache) Get(ctx context.Context, key string) (*value.Entry, error) {
	if mlc.isClosed() {
		return nil, fmt.Errorf("multi-level cache is closed")
	}

	start := time.Now()
	atomic.AddInt64(&mlc.stats.totalRequests, 1)
	atomic.AddInt64(&mlc.stats.getCount, 1)
	atomic.StoreInt64(&mlc.stats.lastAccess, start.UnixNano())

	// 1. 尝试从L1缓存获取
	if mlc.l1Cache != nil {
		l1Start := time.Now()
		entry, err := mlc.l1Cache.Get(ctx, key)
		l1Latency := time.Since(l1Start).Nanoseconds()
		mlc.updateAvgLatency(&mlc.stats.avgL1Latency, l1Latency)

		if err == nil {
			atomic.AddInt64(&mlc.stats.l1Hits, 1)
			mlc.logger.Debugf("L1 cache hit for key: %s", key)
			return entry, nil
		}

		mlc.logger.Debugf("L1 cache miss for key: %s, error: %v", key, err)
	}

	// 2. 尝试从L2缓存获取
	if mlc.l2Cache != nil {
		l2Start := time.Now()
		entry, err := mlc.l2Cache.Get(ctx, key)
		l2Latency := time.Since(l2Start).Nanoseconds()
		mlc.updateAvgLatency(&mlc.stats.avgL2Latency, l2Latency)

		if err == nil {
			atomic.AddInt64(&mlc.stats.l2Hits, 1)
			mlc.logger.Debugf("L2 cache hit for key: %s", key)

			// 3. 将数据提升到L1缓存（缓存提升策略）
			if mlc.l1Cache != nil {
				go func() {
					if setErr := mlc.l1Cache.Set(context.Background(), key, entry); setErr != nil {
						mlc.logger.Warnf("Failed to promote key %s to L1: %v", key, setErr)
					} else {
						mlc.logger.Debugf("Key %s promoted to L1 cache", key)
					}
				}()
			}

			return entry, nil
		}

		mlc.logger.Debugf("L2 cache miss for key: %s, error: %v", key, err)
	}

	// 4. 所有级别都未命中
	atomic.AddInt64(&mlc.stats.misses, 1)
	return nil, fmt.Errorf("key not found in any cache level: %s", key)
}

// Set 多级缓存设置
func (mlc *MultiLevelCache) Set(ctx context.Context, key string, entry *value.Entry) error {
	if mlc.isClosed() {
		return fmt.Errorf("multi-level cache is closed")
	}

	atomic.AddInt64(&mlc.stats.setCount, 1)
	atomic.StoreInt64(&mlc.stats.lastUpdate, time.Now().UnixNano())

	switch mlc.strategy {
	case WriteThrough:
		return mlc.setWriteThrough(ctx, key, entry)
	case WriteBack:
		return mlc.setWriteBack(ctx, key, entry)
	case WriteAround:
		return mlc.setWriteAround(ctx, key, entry)
	default:
		return mlc.setWriteThrough(ctx, key, entry)
	}
}

// Delete 多级缓存删除
func (mlc *MultiLevelCache) Delete(ctx context.Context, key string) error {
	if mlc.isClosed() {
		return fmt.Errorf("multi-level cache is closed")
	}

	atomic.AddInt64(&mlc.stats.deleteCount, 1)

	var errors []error

	// 从所有级别删除
	if mlc.l1Cache != nil {
		if err := mlc.l1Cache.Delete(ctx, key); err != nil {
			errors = append(errors, fmt.Errorf("L1 delete failed: %v", err))
		}
	}

	if mlc.l2Cache != nil {
		if err := mlc.l2Cache.Delete(ctx, key); err != nil {
			errors = append(errors, fmt.Errorf("L2 delete failed: %v", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("delete errors: %v", errors)
	}

	return nil
}

// Stats 获取多级缓存统计信息
func (mlc *MultiLevelCache) Stats() (*MultiLevelCacheStats, error) {
	if mlc.isClosed() {
		return nil, fmt.Errorf("multi-level cache is closed")
	}

	stats := &MultiLevelCacheStats{
		TotalRequests: atomic.LoadInt64(&mlc.stats.totalRequests),
		L1Hits:        atomic.LoadInt64(&mlc.stats.l1Hits),
		L2Hits:        atomic.LoadInt64(&mlc.stats.l2Hits),
		Misses:        atomic.LoadInt64(&mlc.stats.misses),
		GetCount:      atomic.LoadInt64(&mlc.stats.getCount),
		SetCount:      atomic.LoadInt64(&mlc.stats.setCount),
		DeleteCount:   atomic.LoadInt64(&mlc.stats.deleteCount),
		AvgL1Latency:  time.Duration(atomic.LoadInt64(&mlc.stats.avgL1Latency)),
		AvgL2Latency:  time.Duration(atomic.LoadInt64(&mlc.stats.avgL2Latency)),
		StartTime:     time.Unix(0, atomic.LoadInt64(&mlc.stats.startTime)),
		LastAccess:    time.Unix(0, atomic.LoadInt64(&mlc.stats.lastAccess)),
		LastUpdate:    time.Unix(0, atomic.LoadInt64(&mlc.stats.lastUpdate)),
		Strategy:      mlc.strategy.String(),
	}

	// 计算命中率
	if stats.TotalRequests > 0 {
		stats.L1HitRate = float64(stats.L1Hits) / float64(stats.TotalRequests)
		stats.L2HitRate = float64(stats.L2Hits) / float64(stats.TotalRequests)
		stats.OverallHitRate = float64(stats.L1Hits+stats.L2Hits) / float64(stats.TotalRequests)
		stats.MissRate = float64(stats.Misses) / float64(stats.TotalRequests)
	}

	// 获取各级别的详细统计
	if mlc.l1Cache != nil {
		l1Stats, err := mlc.l1Cache.Stats()
		if err == nil {
			stats.L1Stats = l1Stats
		}
	}

	if mlc.l2Cache != nil {
		l2Stats, err := mlc.l2Cache.Stats()
		if err == nil {
			stats.L2Stats = l2Stats
		}
	}

	return stats, nil
}

// SetStrategy 设置缓存策略
func (mlc *MultiLevelCache) SetStrategy(strategy CacheStrategy) {
	mlc.mu.Lock()
	defer mlc.mu.Unlock()

	mlc.strategy = strategy
	mlc.logger.Infof("Cache strategy changed to: %s", strategy.String())
}

// GetStrategy 获取当前缓存策略
func (mlc *MultiLevelCache) GetStrategy() CacheStrategy {
	mlc.mu.RLock()
	defer mlc.mu.RUnlock()

	return mlc.strategy
}

// Clear 清空所有级别的缓存
func (mlc *MultiLevelCache) Clear(ctx context.Context) error {
	if mlc.isClosed() {
		return fmt.Errorf("multi-level cache is closed")
	}

	var errors []error

	if mlc.l1Cache != nil {
		if err := mlc.l1Cache.Clear(ctx); err != nil {
			errors = append(errors, fmt.Errorf("L1 clear failed: %v", err))
		}
	}

	if mlc.l2Cache != nil {
		// L2缓存的Clear需要特殊处理，这里简化
		mlc.logger.Warn("L2 cache clear not implemented")
	}

	// 重置统计信息
	atomic.StoreInt64(&mlc.stats.totalRequests, 0)
	atomic.StoreInt64(&mlc.stats.l1Hits, 0)
	atomic.StoreInt64(&mlc.stats.l2Hits, 0)
	atomic.StoreInt64(&mlc.stats.misses, 0)
	atomic.StoreInt64(&mlc.stats.getCount, 0)
	atomic.StoreInt64(&mlc.stats.setCount, 0)
	atomic.StoreInt64(&mlc.stats.deleteCount, 0)

	if len(errors) > 0 {
		return fmt.Errorf("clear errors: %v", errors)
	}

	return nil
}

// Close 关闭多级缓存（增强版，防止 typed-nil 导致 panic）
func (mlc *MultiLevelCache) Close() error {
	// 额外防护：如果接收者本身为 nil，安全返回
	if mlc == nil {
		return nil
	}

	// 只允许第一次关闭成功
	if !atomic.CompareAndSwapInt32(&mlc.closed, 0, 1) {
		return nil // 已经关闭或正在关闭
	}

	var errors []error

	// Helper: 检测接口的底层是否为 typed-nil
	isTypedNil := func(i interface{}) bool {
		if i == nil {
			return true
		}
		vi := reflect.ValueOf(i)
		// 如果是指针/chan/map/func/slice/interface 且是零值 (IsNil)
		switch vi.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
			return vi.IsNil()
		default:
			return false
		}
	}

	if mlc.l1Cache != nil && !isTypedNil(mlc.l1Cache) {
		if err := mlc.l1Cache.Close(); err != nil {
			errors = append(errors, fmt.Errorf("L1 close failed: %v", err))
		}
	}

	if mlc.l2Cache != nil && !isTypedNil(mlc.l2Cache) {
		if err := mlc.l2Cache.Close(); err != nil {
			errors = append(errors, fmt.Errorf("L2 close failed: %v", err))
		}
	}

	mlc.logger.Info("Multi-level cache closed")

	if len(errors) > 0 {
		return fmt.Errorf("close errors: %v", errors)
	}
	return nil
}

// ============================================================
// 私有方法
// ============================================================

// isClosed 检查是否已关闭
func (mlc *MultiLevelCache) isClosed() bool {
	return atomic.LoadInt32(&mlc.closed) != 0
}

// setWriteThrough 写穿透策略
func (mlc *MultiLevelCache) setWriteThrough(ctx context.Context, key string, entry *value.Entry) error {
	var errors []error

	// 同时写入L1和L2
	if mlc.l1Cache != nil {
		if err := mlc.l1Cache.Set(ctx, key, entry); err != nil {
			errors = append(errors, fmt.Errorf("L1 set failed: %v", err))
		}
	}

	if mlc.l2Cache != nil {
		if err := mlc.l2Cache.Set(ctx, key, entry); err != nil {
			errors = append(errors, fmt.Errorf("L2 set failed: %v", err))
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("write-through errors: %v", errors)
	}

	return nil
}

// setWriteBack 写回策略
func (mlc *MultiLevelCache) setWriteBack(ctx context.Context, key string, entry *value.Entry) error {
	// 先写L1
	var l1Err error
	if mlc.l1Cache != nil {
		l1Err = mlc.l1Cache.Set(ctx, key, entry)
	}

	// 异步写L2
	if mlc.l2Cache != nil {
		go func() {
			if err := mlc.l2Cache.Set(context.Background(), key, entry); err != nil {
				mlc.logger.Warnf("Async L2 write failed for key %s: %v", key, err)
			}
		}()
	}

	return l1Err
}

// setWriteAround 写绕过策略
func (mlc *MultiLevelCache) setWriteAround(ctx context.Context, key string, entry *value.Entry) error {
	// 只写L2，不写L1
	if mlc.l2Cache != nil {
		return mlc.l2Cache.Set(ctx, key, entry)
	}

	return fmt.Errorf("L2 cache not available for write-around")
}

// updateAvgLatency 更新平均延迟
func (mlc *MultiLevelCache) updateAvgLatency(avgLatency *int64, newLatency int64) {
	// 使用简单的移动平均算法
	for {
		old := atomic.LoadInt64(avgLatency)
		// 权重为0.1的指数移动平均
		new := old*9/10 + newLatency/10
		if atomic.CompareAndSwapInt64(avgLatency, old, new) {
			break
		}
	}
}

// String 返回策略的字符串表示
func (cs CacheStrategy) String() string {
	switch cs {
	case WriteThrough:
		return "write-through"
	case WriteBack:
		return "write-back"
	case WriteAround:
		return "write-around"
	default:
		return "unknown"
	}
}

// ============================================================
// 数据结构
// ============================================================

// MultiLevelCacheStats 多级缓存统计信息
type MultiLevelCacheStats struct {
	// 请求统计
	TotalRequests int64 `json:"total_requests"`
	L1Hits        int64 `json:"l1_hits"`
	L2Hits        int64 `json:"l2_hits"`
	Misses        int64 `json:"misses"`

	// 操作统计
	GetCount    int64 `json:"get_count"`
	SetCount    int64 `json:"set_count"`
	DeleteCount int64 `json:"delete_count"`

	// 命中率统计
	L1HitRate      float64 `json:"l1_hit_rate"`
	L2HitRate      float64 `json:"l2_hit_rate"`
	OverallHitRate float64 `json:"overall_hit_rate"`
	MissRate       float64 `json:"miss_rate"`

	// 性能统计
	AvgL1Latency time.Duration `json:"avg_l1_latency"`
	AvgL2Latency time.Duration `json:"avg_l2_latency"`

	// 时间统计
	StartTime  time.Time `json:"start_time"`
	LastAccess time.Time `json:"last_access"`
	LastUpdate time.Time `json:"last_update"`

	// 策略信息
	Strategy string `json:"strategy"`

	// 各级别详细统计
	L1Stats *CacheStats   `json:"l1_stats,omitempty"`
	L2Stats *L2CacheStats `json:"l2_stats,omitempty"`
}
