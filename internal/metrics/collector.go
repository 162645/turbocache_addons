package metrics

import (
	"runtime"
	"sync"
	"time"

	"turbocache/internal/utils/logger"
)

// Collector 指标收集器（用于内部快照、Admin API 等）
// 注：业务路径最好同时直接向 Prometheus 的 Counter/Histogram 打点，Collector 用于快照与导出。
type Collector struct {
	mu     sync.RWMutex
	logger logger.Logger

	// 缓存指标（累计）
	hitCount      int64
	missCount     int64
	setCount      int64
	deleteCount   int64
	evictionCount int64
	errorCount    int64

	// 大小指标（快照）
	totalSize int64
	totalKeys int64
	l1Size    int64
	l1Keys    int64
	l2Size    int64
	l2Keys    int64

	// 延迟指标（基于 bucket 的聚合，用于快照）
	getLatencies    *LatencyCollector
	setLatencies    *LatencyCollector
	deleteLatencies *LatencyCollector

	// 系统指标
	startTime time.Time

	// 配置
	config *CollectorConfig
}

// CollectorConfig 收集器配置
type CollectorConfig struct {
	EnableDetailedMetrics bool          `yaml:"enable_detailed_metrics"`
	LatencyBuckets        []float64     `yaml:"latency_buckets"`     // 单位：毫秒
	CollectionInterval    time.Duration `yaml:"collection_interval"` // exporter 轮询间隔，Collector 也可参照
}

// DefaultCollectorConfig 默认配置
func DefaultCollectorConfig() *CollectorConfig {
	return &CollectorConfig{
		EnableDetailedMetrics: true,
		LatencyBuckets:        []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0},
		CollectionInterval:    15 * time.Second,
	}
}

// NewCollector 创建指标收集器
func NewCollector(config *CollectorConfig) *Collector {
	if config == nil {
		config = DefaultCollectorConfig()
	}

	c := &Collector{
		logger:          logger.WithField("component", "metrics_collector"),
		config:          config,
		startTime:       time.Now(),
		getLatencies:    NewLatencyCollector(config.LatencyBuckets),
		setLatencies:    NewLatencyCollector(config.LatencyBuckets),
		deleteLatencies: NewLatencyCollector(config.LatencyBuckets),
	}

	return c
}

// RecordHit 记录缓存命中（累计）
func (c *Collector) RecordHit() {
	if c == nil {
		return
	}
	// atomic not strictly needed for simple int64 increments if not concurrently read in non-atomic way,
	// but keep operations under lock to maintain consistency for snapshot reads.
	c.mu.Lock()
	c.hitCount++
	c.mu.Unlock()
}

// RecordMiss 记录缓存未命中
func (c *Collector) RecordMiss() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.missCount++
	c.mu.Unlock()
}

// RecordSet 记录设置操作
func (c *Collector) RecordSet() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.setCount++
	c.mu.Unlock()
}

// RecordDelete 记录删除操作
func (c *Collector) RecordDelete() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.deleteCount++
	c.mu.Unlock()
}

// RecordEviction 记录驱逐操作
func (c *Collector) RecordEviction() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.evictionCount++
	c.mu.Unlock()
}

// RecordError 记录错误
func (c *Collector) RecordError() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.errorCount++
	c.mu.Unlock()
}

// RecordGetLatency 记录Get操作延迟（在业务路径调用）
func (c *Collector) RecordGetLatency(duration time.Duration) {
	if c == nil || c.getLatencies == nil {
		return
	}
	c.getLatencies.Record(duration)
}

// RecordSetLatency 记录Set操作延迟
func (c *Collector) RecordSetLatency(duration time.Duration) {
	if c == nil || c.setLatencies == nil {
		return
	}
	c.setLatencies.Record(duration)
}

// RecordDeleteLatency 记录Delete操作延迟
func (c *Collector) RecordDeleteLatency(duration time.Duration) {
	if c == nil || c.deleteLatencies == nil {
		return
	}
	c.deleteLatencies.Record(duration)
}

// UpdateCacheSize 更新缓存大小指标（覆盖式快照）
func (c *Collector) UpdateCacheSize(level string, keys, size int64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch level {
	case "L1":
		c.l1Keys = keys
		c.l1Size = size
	case "L2":
		c.l2Keys = keys
		c.l2Size = size
	}
	c.totalKeys = c.l1Keys + c.l2Keys
	c.totalSize = c.l1Size + c.l2Size
}

// GetMetrics 获取所有指标快照（线程安全）
func (c *Collector) GetMetrics() *Metrics {
	if c == nil {
		return &Metrics{
			Timestamp: time.Now(),
		}
	}

	c.mu.RLock()
	// copy top-level numeric snapshot under lock
	hitCount := c.hitCount
	missCount := c.missCount
	setCount := c.setCount
	deleteCount := c.deleteCount
	evictionCount := c.evictionCount
	errorCount := c.errorCount

	l1Keys := c.l1Keys
	l1Size := c.l1Size
	l2Keys := c.l2Keys
	l2Size := c.l2Size
	totalKeys := c.totalKeys
	totalSize := c.totalSize
	startTime := c.startTime
	getLat := c.getLatencies
	setLat := c.setLatencies
	delLat := c.deleteLatencies
	c.mu.RUnlock()

	// runtime stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	now := time.Now()
	uptime := now.Sub(startTime)
	totalRequests := hitCount + missCount
	var hitRate float64
	if totalRequests > 0 {
		hitRate = float64(hitCount) / float64(totalRequests)
	}

	// latency stats (safe getters)
	var getStats, setStats, deleteStats LatencyStats
	if getLat != nil {
		getStats = getLat.GetStats()
	}
	if setLat != nil {
		setStats = setLat.GetStats()
	}
	if delLat != nil {
		deleteStats = delLat.GetStats()
	}

	return &Metrics{
		Timestamp: now,
		Uptime:    uptime,
		Cache: CacheMetrics{
			HitCount:      hitCount,
			MissCount:     missCount,
			SetCount:      setCount,
			DeleteCount:   deleteCount,
			EvictionCount: evictionCount,
			ErrorCount:    errorCount,
			HitRate:       hitRate,
			TotalKeys:     totalKeys,
			TotalSize:     totalSize,
			L1: LevelMetrics{
				Keys: l1Keys,
				Size: l1Size,
			},
			L2: LevelMetrics{
				Keys: l2Keys,
				Size: l2Size,
			},
		},
		Latency: LatencyMetrics{
			Get:    getStats,
			Set:    setStats,
			Delete: deleteStats,
		},
		System: SystemMetrics{
			Goroutines:      int64(runtime.NumGoroutine()),
			HeapSize:        int64(memStats.HeapAlloc),
			HeapObjects:     int64(memStats.HeapObjects),
			StackSize:       int64(memStats.StackInuse),
			GCCount:         int64(memStats.NumGC),
			GCPauseTotal:    time.Duration(memStats.PauseTotalNs),
			LastGCTime:      time.Unix(0, int64(memStats.LastGC)),
			NextGC:          int64(memStats.NextGC),
			CPUCount:        int64(runtime.NumCPU()),
			MemoryAllocated: int64(memStats.Alloc),
			MemoryTotal:     int64(memStats.TotalAlloc),
		},
	}
}

// Reset 重置指标（慎用）
func (c *Collector) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hitCount = 0
	c.missCount = 0
	c.setCount = 0
	c.deleteCount = 0
	c.evictionCount = 0
	c.errorCount = 0
	c.totalSize = 0
	c.totalKeys = 0
	c.l1Size = 0
	c.l1Keys = 0
	c.l2Size = 0
	c.l2Keys = 0
	if c.getLatencies != nil {
		c.getLatencies.Reset()
	}
	if c.setLatencies != nil {
		c.setLatencies.Reset()
	}
	if c.deleteLatencies != nil {
		c.deleteLatencies.Reset()
	}
	c.startTime = time.Now()
	c.logger.Info("Metrics collector reset")
}

// ========== LatencyCollector（简化实现，使用锁保护） ==========

type LatencyCollector struct {
	mu      sync.RWMutex
	buckets []float64
	counts  []int64
	sum     int64 // nanoseconds
	count   int64
}

func NewLatencyCollector(buckets []float64) *LatencyCollector {
	// copy buckets and ensure overflow bucket exists
	b := append([]float64{}, buckets...)
	if len(b) == 0 || b[len(b)-1] < 1e9 {
		// add a very large bucket (ms) as overflow
		b = append(b, 1e12)
	}
	return &LatencyCollector{
		buckets: b,
		counts:  make([]int64, len(b)),
	}
}

func (lc *LatencyCollector) Record(d time.Duration) {
	if lc == nil {
		return
	}
	ms := float64(d.Nanoseconds()) / 1e6
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.sum += d.Nanoseconds()
	lc.count++
	for i, bucket := range lc.buckets {
		if ms <= bucket {
			lc.counts[i]++
			break
		}
	}
}

func (lc *LatencyCollector) GetStats() LatencyStats {
	if lc == nil {
		return LatencyStats{}
	}
	lc.mu.RLock()
	defer lc.mu.RUnlock()
	if lc.count == 0 {
		return LatencyStats{}
	}
	avg := float64(lc.sum) / float64(lc.count) / 1e6 // ms
	total := lc.count
	target50 := int64(float64(total) * 0.50)
	target95 := int64(float64(total) * 0.95)
	target99 := int64(float64(total) * 0.99)
	var cum int64
	var p50, p95, p99 float64
	for i, c := range lc.counts {
		cum += c
		if p50 == 0 && cum >= target50 {
			p50 = lc.buckets[i]
		}
		if p95 == 0 && cum >= target95 {
			p95 = lc.buckets[i]
		}
		if p99 == 0 && cum >= target99 {
			p99 = lc.buckets[i]
			break
		}
	}
	return LatencyStats{
		Count: lc.count,
		Sum:   time.Duration(lc.sum),
		Avg:   avg,
		P50:   p50,
		P95:   p95,
		P99:   p99,
	}
}

func (lc *LatencyCollector) Reset() {
	if lc == nil {
		return
	}
	lc.mu.Lock()
	defer lc.mu.Unlock()
	lc.sum = 0
	lc.count = 0
	for i := range lc.counts {
		lc.counts[i] = 0
	}
}

// ========== Metrics DTOs ==========

type Metrics struct {
	Timestamp time.Time      `json:"timestamp"`
	Uptime    time.Duration  `json:"uptime"`
	Cache     CacheMetrics   `json:"cache"`
	Latency   LatencyMetrics `json:"latency"`
	System    SystemMetrics  `json:"system"`
}

type CacheMetrics struct {
	HitCount      int64        `json:"hit_count"`
	MissCount     int64        `json:"miss_count"`
	SetCount      int64        `json:"set_count"`
	DeleteCount   int64        `json:"delete_count"`
	EvictionCount int64        `json:"eviction_count"`
	ErrorCount    int64        `json:"error_count"`
	HitRate       float64      `json:"hit_rate"`
	TotalKeys     int64        `json:"total_keys"`
	TotalSize     int64        `json:"total_size"`
	L1            LevelMetrics `json:"l1"`
	L2            LevelMetrics `json:"l2"`
}

type LevelMetrics struct {
	Keys int64 `json:"keys"`
	Size int64 `json:"size"`
}

type LatencyMetrics struct {
	Get    LatencyStats `json:"get"`
	Set    LatencyStats `json:"set"`
	Delete LatencyStats `json:"delete"`
}

type LatencyStats struct {
	Count int64         `json:"count"`
	Sum   time.Duration `json:"sum"`    // nanoseconds as Duration
	Avg   float64       `json:"avg_ms"` // ms
	P50   float64       `json:"p50_ms"` // ms
	P95   float64       `json:"p95_ms"` // ms
	P99   float64       `json:"p99_ms"` // ms
}

type SystemMetrics struct {
	Goroutines      int64         `json:"goroutines"`
	HeapSize        int64         `json:"heap_size"`
	HeapObjects     int64         `json:"heap_objects"`
	StackSize       int64         `json:"stack_size"`
	GCCount         int64         `json:"gc_count"`
	GCPauseTotal    time.Duration `json:"gc_pause_total"`
	LastGCTime      time.Time     `json:"last_gc_time"`
	NextGC          int64         `json:"next_gc"`
	CPUCount        int64         `json:"cpu_count"`
	MemoryAllocated int64         `json:"memory_allocated"`
	MemoryTotal     int64         `json:"memory_total"`
}

// 全局默认 Collector（可在程序启动时替换或直接使用）
var DefaultCollector = NewCollector(DefaultCollectorConfig())
