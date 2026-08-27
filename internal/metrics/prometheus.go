package metrics

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"turbocache/internal/config"
	"turbocache/internal/utils/logger"
)

// PrometheusExporter 将 Collector 的快照导出到 Prometheus
// 设计要点：
// - 对于累计计数（hit/miss/...），采用 delta 计算（防止重复 Add 全量导致数值爆炸）
// - 对于快照值（keys/size/hitRate/system），使用 Gauge Set
// - 延迟以快照的 p50/p95/p99/avg 导出为 Gauge（因为从聚合桶无法安全恢复原始 Observe）
// - 最好在业务路径同时直接调用 Prometheus 的 Counter/Histogram（推荐），Exporter 作为补充快照导出
type PrometheusExporter struct {
	collector *Collector
	config    *config.PrometheusConfig
	logger    logger.Logger

	// Prometheus 指标
	cacheHits      *prometheus.CounterVec
	cacheMisses    *prometheus.CounterVec
	cacheSets      *prometheus.CounterVec
	cacheDeletes   *prometheus.CounterVec
	cacheEvictions *prometheus.CounterVec
	cacheErrors    *prometheus.CounterVec

	cacheSize    *prometheus.GaugeVec
	cacheKeys    *prometheus.GaugeVec
	cacheHitRate prometheus.Gauge

	// latency percentiles/avg as Gauges (operation label: get,set,delete)
	latencyP50 *prometheus.GaugeVec
	latencyP95 *prometheus.GaugeVec
	latencyP99 *prometheus.GaugeVec
	latencyAvg *prometheus.GaugeVec

	// system
	goroutines  prometheus.Gauge
	heapSize    prometheus.Gauge
	gcCount     prometheus.Counter
	gcPauseTime prometheus.Gauge

	registry *prometheus.Registry

	// last seen totals for delta calculation
	lastHitCount      int64
	lastMissCount     int64
	lastSetCount      int64
	lastDeleteCount   int64
	lastEvictionCount int64
	lastErrorCount    int64
	lastGCCount       int64
}

// NewPrometheusExporter 创建 Prometheus exporter
func NewPrometheusExporter(collector *Collector, cfg *config.PrometheusConfig) *PrometheusExporter {
	// 如果没有传入配置，创建默认值
	if cfg == nil {
		cfg = &config.PrometheusConfig{
			Enabled:   true,
			Namespace: "turbocache",
			Subsystem: "",
		}
	}
	if !cfg.Enabled {
		return nil
	}
	registry := prometheus.NewRegistry()

	pe := &PrometheusExporter{
		collector: collector,
		config:    cfg,
		logger:    logger.WithField("component", "prometheus_exporter"),
		registry:  registry,
	}

	pe.initMetrics()
	pe.registerMetrics()

	// register process/go collectors for runtime/process-level metrics
	registry.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	registry.MustRegister(prometheus.NewGoCollector())

	// 启动更新循环（若 collector 为 nil，则不启动）
	if collector != nil {
		go pe.updateLoop()
	}

	return pe
}

func (pe *PrometheusExporter) initMetrics() {
	namespace := pe.config.Namespace
	subsystem := pe.config.Subsystem

	pe.cacheHits = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_hits_total",
			Help:      "Total number of cache hits (delta applied by exporter)",
		},
		[]string{"level"},
	)
	pe.cacheMisses = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_misses_total",
			Help:      "Total number of cache misses (delta applied by exporter)",
		},
		[]string{"level"},
	)
	pe.cacheSets = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_sets_total",
			Help:      "Total number of cache set operations (delta applied by exporter)",
		},
		[]string{"level"},
	)
	pe.cacheDeletes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_deletes_total",
			Help:      "Total number of cache delete operations (delta applied by exporter)",
		},
		[]string{"level"},
	)
	pe.cacheEvictions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_evictions_total",
			Help:      "Total number of cache evictions (delta applied by exporter)",
		},
		[]string{"level"},
	)
	pe.cacheErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_errors_total",
			Help:      "Total number of cache errors (delta applied by exporter)",
		},
		[]string{"operation"},
	)

	pe.cacheSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_size_bytes",
			Help:      "Current cache size in bytes (snapshot)",
		},
		[]string{"level"},
	)
	pe.cacheKeys = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_keys",
			Help:      "Current number of keys in cache (snapshot)",
		},
		[]string{"level"},
	)
	pe.cacheHitRate = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_hit_rate",
			Help:      "Current cache hit rate (snapshot)",
		},
	)

	// latency percentiles / avg as gauges
	pe.latencyP50 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_latency_p50_ms",
			Help:      "Cache latency p50 in ms (snapshot)",
		},
		[]string{"operation"},
	)
	pe.latencyP95 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_latency_p95_ms",
			Help:      "Cache latency p95 in ms (snapshot)",
		},
		[]string{"operation"},
	)
	pe.latencyP99 = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_latency_p99_ms",
			Help:      "Cache latency p99 in ms (snapshot)",
		},
		[]string{"operation"},
	)
	pe.latencyAvg = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "cache_latency_avg_ms",
			Help:      "Cache latency average in ms (snapshot)",
		},
		[]string{"operation"},
	)

	// system
	pe.goroutines = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "goroutines",
			Help:      "Current number of goroutines",
		},
	)
	pe.heapSize = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "heap_size_bytes",
			Help:      "Current heap size in bytes",
		},
	)
	pe.gcCount = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "gc_count_total",
			Help:      "Total number of garbage collections (delta applied by exporter)",
		},
	)
	pe.gcPauseTime = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "gc_pause_milliseconds",
			Help:      "GC pause total in milliseconds",
		},
	)
}

func (pe *PrometheusExporter) registerMetrics() {
	pe.registry.MustRegister(
		pe.cacheHits,
		pe.cacheMisses,
		pe.cacheSets,
		pe.cacheDeletes,
		pe.cacheEvictions,
		pe.cacheErrors,
		pe.cacheSize,
		pe.cacheKeys,
		pe.cacheHitRate,
		pe.latencyP50,
		pe.latencyP95,
		pe.latencyP99,
		pe.latencyAvg,
		pe.goroutines,
		pe.heapSize,
		pe.gcCount,
		pe.gcPauseTime,
	)
	pe.logger.Info("Prometheus metrics registered")
}

func (pe *PrometheusExporter) updateLoop() {
	interval := 15 * time.Second
	if pe.collector != nil && pe.collector.config != nil && pe.collector.config.CollectionInterval > 0 {
		interval = pe.collector.config.CollectionInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		pe.updateMetrics()
	}
}

func (pe *PrometheusExporter) updateMetrics() {
	if pe.collector == nil {
		return
	}
	m := pe.collector.GetMetrics()

	// --- Counters: add only delta (防止重复 Add 全量) ---
	pe.addDeltaCounter(pe.cacheHits.WithLabelValues("total"), &pe.lastHitCount, m.Cache.HitCount)
	pe.addDeltaCounter(pe.cacheMisses.WithLabelValues("total"), &pe.lastMissCount, m.Cache.MissCount)
	pe.addDeltaCounter(pe.cacheSets.WithLabelValues("total"), &pe.lastSetCount, m.Cache.SetCount)
	pe.addDeltaCounter(pe.cacheDeletes.WithLabelValues("total"), &pe.lastDeleteCount, m.Cache.DeleteCount)
	pe.addDeltaCounter(pe.cacheEvictions.WithLabelValues("total"), &pe.lastEvictionCount, m.Cache.EvictionCount)
	// errors might be labeled by operation elsewhere; here we keep a generic one
	pe.addDeltaCounter(pe.cacheErrors.WithLabelValues("unknown"), &pe.lastErrorCount, m.Cache.ErrorCount)

	// --- Gauges: snapshot set ---
	pe.cacheSize.WithLabelValues("L1").Set(float64(m.Cache.L1.Size))
	pe.cacheKeys.WithLabelValues("L1").Set(float64(m.Cache.L1.Keys))
	pe.cacheSize.WithLabelValues("L2").Set(float64(m.Cache.L2.Size))
	pe.cacheKeys.WithLabelValues("L2").Set(float64(m.Cache.L2.Keys))
	pe.cacheHitRate.Set(m.Cache.HitRate)

	// latency snapshots: set avg/p50/p95/p99 for each operation
	pe.latencyAvg.WithLabelValues("get").Set(m.Latency.Get.Avg)
	pe.latencyP50.WithLabelValues("get").Set(m.Latency.Get.P50)
	pe.latencyP95.WithLabelValues("get").Set(m.Latency.Get.P95)
	pe.latencyP99.WithLabelValues("get").Set(m.Latency.Get.P99)

	pe.latencyAvg.WithLabelValues("set").Set(m.Latency.Set.Avg)
	pe.latencyP50.WithLabelValues("set").Set(m.Latency.Set.P50)
	pe.latencyP95.WithLabelValues("set").Set(m.Latency.Set.P95)
	pe.latencyP99.WithLabelValues("set").Set(m.Latency.Set.P99)

	pe.latencyAvg.WithLabelValues("delete").Set(m.Latency.Delete.Avg)
	pe.latencyP50.WithLabelValues("delete").Set(m.Latency.Delete.P50)
	pe.latencyP95.WithLabelValues("delete").Set(m.Latency.Delete.P95)
	pe.latencyP99.WithLabelValues("delete").Set(m.Latency.Delete.P99)

	// system metrics
	pe.goroutines.Set(float64(m.System.Goroutines))
	pe.heapSize.Set(float64(m.System.HeapSize))
	// GC counter use delta
	pe.addDeltaCounter(pe.gcCount, &pe.lastGCCount, m.System.GCCount)
	pe.gcPauseTime.Set(float64(m.System.GCPauseTotal.Milliseconds()))
}

// addDeltaCounter 将 currentTotal - lastTotal 的差值加入到 Prometheus Counter，
// 如果 current < last（例如 Collector Reset），则将 last 设置为 current（不 Add 负值）
func (pe *PrometheusExporter) addDeltaCounter(c prometheus.Counter, last *int64, current int64) {
	prev := atomic.LoadInt64(last)
	if current < prev {
		atomic.StoreInt64(last, current)
		return
	}
	delta := current - prev
	if delta > 0 {
		c.Add(float64(delta))
		atomic.StoreInt64(last, current)
	}
}

// Handler 返回 Prometheus HTTP handler
func (pe *PrometheusExporter) Handler() http.Handler {
	return promhttp.HandlerFor(pe.registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// Registry 返回 Prometheus 注册表
func (pe *PrometheusExporter) Registry() *prometheus.Registry {
	return pe.registry
}

// StartServer 便捷函数：在指定 addr 上启动 metrics HTTP 服务（可选）
func (pe *PrometheusExporter) StartServer(ctx context.Context, addr string) error {
	if pe == nil {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", pe.Handler())
	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(context.Background())
	}()
	pe.logger.Infof("metrics server starting on %s", addr)
	return srv.ListenAndServe()
}
