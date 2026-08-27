package negative

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// NegativeEntry 负缓存条目
type NegativeEntry struct {
	Key         string    `json:"key"`
	CreatedAt   time.Time `json:"created_at"`
	ExpiresAt   time.Time `json:"expires_at"`
	AccessCount int64     `json:"access_count"`
	Reason      string    `json:"reason"`     // 缓存原因
	ErrorType   string    `json:"error_type"` // 错误类型
}

// IsExpired 检查是否过期
func (ne *NegativeEntry) IsExpired() bool {
	return time.Now().After(ne.ExpiresAt)
}

// NegativeCache 负缓存实现
type NegativeCache struct {
	mu          sync.RWMutex
	entries     map[string]*NegativeEntry
	defaultTTL  time.Duration
	maxSize     int64
	currentSize int64

	// 清理相关
	cleanupInterval time.Duration
	stopCleanup     chan struct{}

	// 统计信息
	hitCount     int64
	missCount    int64
	setCount     int64
	evictCount   int64
	expiredCount int64

	// 生命周期
	closed bool
}

// Config 负缓存配置
type Config struct {
	DefaultTTL      time.Duration `yaml:"default_ttl" json:"default_ttl"`
	MaxSize         int64         `yaml:"max_size" json:"max_size"`
	CleanupInterval time.Duration `yaml:"cleanup_interval" json:"cleanup_interval"`
}

// NewNegativeCache 创建负缓存
func NewNegativeCache(config *Config) *NegativeCache {
	if config == nil {
		config = &Config{
			DefaultTTL:      5 * time.Minute,
			MaxSize:         10000,
			CleanupInterval: time.Minute,
		}
	}

	nc := &NegativeCache{
		entries:         make(map[string]*NegativeEntry),
		defaultTTL:      config.DefaultTTL,
		maxSize:         config.MaxSize,
		cleanupInterval: config.CleanupInterval,
		stopCleanup:     make(chan struct{}),
	}

	// 启动清理协程
	if config.CleanupInterval > 0 {
		go nc.startCleanup()
	}

	return nc
}

// Set 设置负缓存条目
func (nc *NegativeCache) Set(key string, reason string, errorType string) {
	nc.SetWithTTL(key, reason, errorType, nc.defaultTTL)
}

// SetWithTTL 设置带TTL的负缓存条目
func (nc *NegativeCache) SetWithTTL(key string, reason string, errorType string, ttl time.Duration) {
	if nc.closed {
		return
	}

	nc.mu.Lock()
	defer nc.mu.Unlock()

	now := time.Now()
	entry := &NegativeEntry{
		Key:         key,
		CreatedAt:   now,
		ExpiresAt:   now.Add(ttl),
		AccessCount: 0,
		Reason:      reason,
		ErrorType:   errorType,
	}

	// 如果已存在，替换
	if _, exists := nc.entries[key]; !exists {
		// 检查大小限制
		if nc.currentSize >= nc.maxSize {
			nc.evictOldest()
		}
		nc.currentSize++
	}

	nc.entries[key] = entry
	atomic.AddInt64(&nc.setCount, 1)
}

// Get 获取负缓存条目
func (nc *NegativeCache) Get(key string) (*NegativeEntry, bool) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.closed {
		return nil, false
	}
	entry, exists := nc.entries[key]

	if !exists {
		atomic.AddInt64(&nc.missCount, 1)
		return nil, false
	}

	// 检查是否过期
	if entry.IsExpired() {
		delete(nc.entries, key)
		nc.currentSize--
		atomic.AddInt64(&nc.expiredCount, 1)
		atomic.AddInt64(&nc.missCount, 1)
		return nil, false
	}

	// 更新访问计数
	atomic.AddInt64(&entry.AccessCount, 1)
	atomic.AddInt64(&nc.hitCount, 1)

	return entry, true
}

// Contains 检查是否包含键
func (nc *NegativeCache) Contains(key string) bool {
	_, exists := nc.Get(key)
	return exists
}

// Delete 删除负缓存条目
func (nc *NegativeCache) Delete(key string) bool {
	if nc.closed {
		return false
	}

	nc.mu.Lock()
	defer nc.mu.Unlock()

	if _, exists := nc.entries[key]; exists {
		delete(nc.entries, key)
		nc.currentSize--
		return true
	}

	return false
}

// Clear 清空所有条目
func (nc *NegativeCache) Clear() {
	if nc.closed {
		return
	}

	nc.mu.Lock()
	defer nc.mu.Unlock()

	nc.entries = make(map[string]*NegativeEntry)
	nc.currentSize = 0
}

// Stats 获取统计信息
func (nc *NegativeCache) Stats() *Stats {
	if nc.closed {
		return &Stats{}
	}

	nc.mu.RLock()
	currentSize := nc.currentSize
	activeEntries := int64(0)
	expiredEntries := int64(0)

	now := time.Now()
	for _, entry := range nc.entries {
		if entry.ExpiresAt.After(now) {
			activeEntries++
		} else {
			expiredEntries++
		}
	}
	nc.mu.RUnlock()

	return &Stats{
		TotalEntries:   currentSize,
		ActiveEntries:  activeEntries,
		ExpiredEntries: expiredEntries,
		MaxSize:        nc.maxSize,
		HitCount:       atomic.LoadInt64(&nc.hitCount),
		MissCount:      atomic.LoadInt64(&nc.missCount),
		SetCount:       atomic.LoadInt64(&nc.setCount),
		EvictCount:     atomic.LoadInt64(&nc.evictCount),
		ExpiredCount:   atomic.LoadInt64(&nc.expiredCount),
		HitRate:        nc.calculateHitRate(),
	}
}

// Stats 统计信息
type Stats struct {
	TotalEntries   int64   `json:"total_entries"`
	ActiveEntries  int64   `json:"active_entries"`
	ExpiredEntries int64   `json:"expired_entries"`
	MaxSize        int64   `json:"max_size"`
	HitCount       int64   `json:"hit_count"`
	MissCount      int64   `json:"miss_count"`
	SetCount       int64   `json:"set_count"`
	EvictCount     int64   `json:"evict_count"`
	ExpiredCount   int64   `json:"expired_count"`
	HitRate        float64 `json:"hit_rate"`
}

// calculateHitRate 计算命中率
func (nc *NegativeCache) calculateHitRate() float64 {
	hitCount := atomic.LoadInt64(&nc.hitCount)
	missCount := atomic.LoadInt64(&nc.missCount)

	total := hitCount + missCount
	if total == 0 {
		return 0
	}

	return float64(hitCount) / float64(total)
}

// startCleanup 启动清理协程
func (nc *NegativeCache) startCleanup() {
	ticker := time.NewTicker(nc.cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			nc.cleanup()
		case <-nc.stopCleanup:
			return
		}
	}
}

// cleanup 清理过期条目
func (nc *NegativeCache) cleanup() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	if nc.closed {
		return
	}

	now := time.Now()
	expiredKeys := make([]string, 0)

	for key, entry := range nc.entries {
		if entry.ExpiresAt.Before(now) {
			expiredKeys = append(expiredKeys, key)
		}
	}

	// 删除过期条目
	for _, key := range expiredKeys {
		delete(nc.entries, key)
		nc.currentSize--
		atomic.AddInt64(&nc.expiredCount, 1)
	}
}

// evictOldest 淘汰最老的条目
func (nc *NegativeCache) evictOldest() {
	if len(nc.entries) == 0 {
		return
	}

	var oldestKey string
	var oldestTime time.Time

	for key, entry := range nc.entries {
		if oldestKey == "" || entry.CreatedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.CreatedAt
		}
	}

	if oldestKey != "" {
		delete(nc.entries, oldestKey)
		nc.currentSize--
		atomic.AddInt64(&nc.evictCount, 1)
	}
}

// Close 关闭负缓存
func (nc *NegativeCache) Close() error {
	nc.mu.Lock()
	if nc.closed {
		nc.mu.Unlock()
		return nil
	}
	nc.closed = true
	nc.entries = make(map[string]*NegativeEntry)
	nc.currentSize = 0
	nc.mu.Unlock()

	// 停止清理协程
	close(nc.stopCleanup)

	return nil
}

// NegativeCacheWrapper 负缓存包装器
type NegativeCacheWrapper struct {
	cache         Cache
	negativeCache *NegativeCache
}

// Cache 缓存接口
type Cache interface {
	Get(ctx context.Context, key string) (interface{}, error)
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// NewNegativeCacheWrapper 创建负缓存包装器
func NewNegativeCacheWrapper(cache Cache, config *Config) *NegativeCacheWrapper {
	return &NegativeCacheWrapper{
		cache:         cache,
		negativeCache: NewNegativeCache(config),
	}
}

// Get 获取缓存值，支持负缓存
func (ncw *NegativeCacheWrapper) Get(ctx context.Context, key string) (interface{}, error) {
	// 首先检查负缓存
	if entry, exists := ncw.negativeCache.Get(key); exists {
		return nil, &NegativeCacheError{
			Key:       key,
			Reason:    entry.Reason,
			ErrorType: entry.ErrorType,
			CachedAt:  entry.CreatedAt,
		}
	}

	// 从实际缓存获取
	return ncw.cache.Get(ctx, key)
}

// Set 设置缓存值
func (ncw *NegativeCacheWrapper) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	// 从负缓存中移除该键
	ncw.negativeCache.Delete(key)

	// 设置到实际缓存
	return ncw.cache.Set(ctx, key, value, ttl)
}

// Delete 删除缓存值
func (ncw *NegativeCacheWrapper) Delete(ctx context.Context, key string) error {
	// 从负缓存中移除
	ncw.negativeCache.Delete(key)

	// 从实际缓存中删除
	return ncw.cache.Delete(ctx, key)
}

// SetNegative 设置负缓存
func (ncw *NegativeCacheWrapper) SetNegative(key string, reason string, errorType string) {
	ncw.negativeCache.Set(key, reason, errorType)
}

// SetNegativeWithTTL 设置带TTL的负缓存
func (ncw *NegativeCacheWrapper) SetNegativeWithTTL(key string, reason string, errorType string, ttl time.Duration) {
	ncw.negativeCache.SetWithTTL(key, reason, errorType, ttl)
}

// GetStats 获取负缓存统计信息
func (ncw *NegativeCacheWrapper) GetStats() *Stats {
	return ncw.negativeCache.Stats()
}

// Close 关闭包装器
func (ncw *NegativeCacheWrapper) Close() error {
	return ncw.negativeCache.Close()
}

// NegativeCacheError 负缓存错误
type NegativeCacheError struct {
	Key       string    `json:"key"`
	Reason    string    `json:"reason"`
	ErrorType string    `json:"error_type"`
	CachedAt  time.Time `json:"cached_at"`
}

// Error 实现error接口
func (e *NegativeCacheError) Error() string {
	return fmt.Sprintf("negative cache hit for key '%s': %s (type: %s, cached at: %v)",
		e.Key, e.Reason, e.ErrorType, e.CachedAt.Format(time.RFC3339))
}

// IsNegativeCacheError 检查是否是负缓存错误
func IsNegativeCacheError(err error) bool {
	_, ok := err.(*NegativeCacheError)
	return ok
}

// 预定义的错误类型
const (
	ErrorTypeNotFound    = "not_found"
	ErrorTypeTimeout     = "timeout"
	ErrorTypePermission  = "permission_denied"
	ErrorTypeRateLimit   = "rate_limit"
	ErrorTypeServerError = "server_error"
	ErrorTypeValidation  = "validation_error"
)

// SmartNegativeCache 智能负缓存，支持动态TTL调整
type SmartNegativeCache struct {
	*NegativeCache

	// 动态TTL配置
	minTTL     time.Duration
	maxTTL     time.Duration
	multiplier float64

	// 错误类型到TTL的映射
	errorTypeTTL map[string]time.Duration
}

// SmartConfig 智能负缓存配置
type SmartConfig struct {
	*Config
	MinTTL       time.Duration            `yaml:"min_ttl" json:"min_ttl"`
	MaxTTL       time.Duration            `yaml:"max_ttl" json:"max_ttl"`
	Multiplier   float64                  `yaml:"multiplier" json:"multiplier"`
	ErrorTypeTTL map[string]time.Duration `yaml:"error_type_ttl" json:"error_type_ttl"`
}

// NewSmartNegativeCache 创建智能负缓存
func NewSmartNegativeCache(config *SmartConfig) *SmartNegativeCache {
	if config.Config == nil {
		config.Config = &Config{
			DefaultTTL:      5 * time.Minute,
			MaxSize:         10000,
			CleanupInterval: time.Minute,
		}
	}

	if config.MinTTL == 0 {
		config.MinTTL = 30 * time.Second
	}
	if config.MaxTTL == 0 {
		config.MaxTTL = 30 * time.Minute
	}
	if config.Multiplier == 0 {
		config.Multiplier = 1.5
	}
	if config.ErrorTypeTTL == nil {
		config.ErrorTypeTTL = map[string]time.Duration{
			ErrorTypeNotFound:    2 * time.Minute,
			ErrorTypeTimeout:     30 * time.Second,
			ErrorTypePermission:  10 * time.Minute,
			ErrorTypeRateLimit:   5 * time.Minute,
			ErrorTypeServerError: time.Minute,
			ErrorTypeValidation:  5 * time.Minute,
		}
	}

	return &SmartNegativeCache{
		NegativeCache: NewNegativeCache(config.Config),
		minTTL:        config.MinTTL,
		maxTTL:        config.MaxTTL,
		multiplier:    config.Multiplier,
		errorTypeTTL:  config.ErrorTypeTTL,
	}
}

// SetSmart 智能设置负缓存条目
func (snc *SmartNegativeCache) SetSmart(key string, reason string, errorType string) {
	ttl := snc.calculateTTL(key, errorType)
	snc.SetWithTTL(key, reason, errorType, ttl)
}

// calculateTTL 计算动态TTL
func (snc *SmartNegativeCache) calculateTTL(key string, errorType string) time.Duration {
	// 基础TTL（根据错误类型）
	baseTTL := snc.defaultTTL
	if specificTTL, exists := snc.errorTypeTTL[errorType]; exists {
		baseTTL = specificTTL
	}

	// 推荐：根据 entry.AccessCount（原子读）动态调整 TTL
	if entry, exists := snc.Get(key); exists {
		access := atomic.LoadInt64(&entry.AccessCount)

		// 根据访问次数调整倍数（示例策略，可按需修改）
		factor := snc.multiplier
		if access > 20 {
			factor = factor * 2.0
		} else if access > 10 {
			factor = factor * 1.5
		} else if access > 3 {
			factor = factor * 1.2
		}

		newTTL := time.Duration(float64(baseTTL) * factor)
		if newTTL > snc.maxTTL {
			newTTL = snc.maxTTL
		}
		return newTTL
	}

	// 确保TTL在范围内
	if baseTTL < snc.minTTL {
		baseTTL = snc.minTTL
	}
	if baseTTL > snc.maxTTL {
		baseTTL = snc.maxTTL
	}

	return baseTTL
}

// BatchNegativeCache 批量负缓存操作
type BatchNegativeCache struct {
	*NegativeCache
	batchSize   int
	batchBuffer map[string]*NegativeEntry
	mu          sync.Mutex
}

// NewBatchNegativeCache 创建批量负缓存
func NewBatchNegativeCache(config *Config, batchSize int) *BatchNegativeCache {
	return &BatchNegativeCache{
		NegativeCache: NewNegativeCache(config),
		batchSize:     batchSize,
		batchBuffer:   make(map[string]*NegativeEntry),
	}
}

// BatchSet 批量设置负缓存条目
func (bnc *BatchNegativeCache) BatchSet(entries map[string]*NegativeEntry) {
	bnc.mu.Lock()
	defer bnc.mu.Unlock()

	// 添加到批量缓冲区
	for key, entry := range entries {
		bnc.batchBuffer[key] = entry
	}

	// 如果达到批量大小，执行批量写入
	if len(bnc.batchBuffer) >= bnc.batchSize {
		bnc.flushBatch()
	}
}

// flushBatch 刷新批量缓冲区
func (bnc *BatchNegativeCache) flushBatch() {
	if len(bnc.batchBuffer) == 0 {
		return
	}

	bnc.NegativeCache.mu.Lock()
	defer bnc.NegativeCache.mu.Unlock()

	for key, entry := range bnc.batchBuffer {
		// 检查大小限制
		if _, exists := bnc.entries[key]; !exists {
			if bnc.currentSize >= bnc.maxSize {
				bnc.evictOldest()
			}
			bnc.currentSize++
		}

		bnc.entries[key] = entry
		atomic.AddInt64(&bnc.setCount, 1)
	}

	// 清空缓冲区
	bnc.batchBuffer = make(map[string]*NegativeEntry)
}

// Flush 强制刷新缓冲区
func (bnc *BatchNegativeCache) Flush() {
	bnc.mu.Lock()
	defer bnc.mu.Unlock()

	bnc.flushBatch()
}

// Close 关闭批量负缓存
func (bnc *BatchNegativeCache) Close() error {
	bnc.Flush()
	return bnc.NegativeCache.Close()
}
