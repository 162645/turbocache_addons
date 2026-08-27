package server

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metrics wraps prometheus metrics + a gRPC interceptor.
//
// Design notes:
//   - use a dedicated registry so that tests can start multiple servers in-process
//     without "duplicate metrics collector registration" panics.
//   - keep labels small and stable.
type Metrics struct {
	reg *prometheus.Registry

	grpcRequests  *prometheus.CounterVec
	grpcLatency   *prometheus.HistogramVec
	cacheGetTotal *prometheus.CounterVec
	cacheGetHit   *prometheus.CounterVec
	cacheGetMiss  *prometheus.CounterVec
	cacheGetFwd   *prometheus.CounterVec

	l1Keys          prometheus.Gauge
	l1Size          prometheus.Gauge
	ringSize        prometheus.Gauge
	bloomReady      prometheus.Gauge
	bloomStale      prometheus.Gauge
	bloomVersion    prometheus.Gauge
	bloomFailOpen   prometheus.Gauge
	bloomRejected   prometheus.Gauge
	bloomSyncErrors prometheus.Gauge
	bloomQueueDepth prometheus.Gauge
	bloomRebuilds   prometheus.Gauge
}

func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()

	m := &Metrics{
		reg: reg,
		grpcRequests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "turbocache_grpc_requests_total",
				Help: "Total number of gRPC requests handled by turbocache server.",
			},
			[]string{"method", "code"},
		),
		grpcLatency: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "turbocache_grpc_request_duration_seconds",
				Help:    "gRPC request latency distribution.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"method"},
		),
		cacheGetTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "turbocache_cache_get_total",
				Help: "Total cache GET requests.",
			},
			[]string{"result"}, // local_hit/forward_hit/miss/error
		),
		cacheGetHit: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "turbocache_cache_get_hit_total",
				Help: "Total cache GET hits.",
			},
			[]string{"level"}, // l1/forward
		),
		cacheGetMiss: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "turbocache_cache_get_miss_total",
				Help: "Total cache GET misses.",
			},
			[]string{"level"}, // l1/overall
		),
		cacheGetFwd: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "turbocache_cache_get_forward_total",
				Help: "Total cache GET forwards.",
			},
			[]string{"outcome"}, // ok/error
		),
		l1Keys: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "turbocache_l1_keys",
			Help: "Current number of keys in L1.",
		}),
		l1Size: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "turbocache_l1_bytes",
			Help: "Current bytes used by L1.",
		}),
		ringSize: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "turbocache_ring_nodes",
			Help: "Current number of nodes in hash ring.",
		}),
		bloomReady:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_ready", Help: "Whether the cluster Bloom view is authoritative."}),
		bloomStale:      prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_stale", Help: "Whether Bloom is stale and reads fail open."}),
		bloomVersion:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_version", Help: "Current cluster Bloom version."}),
		bloomFailOpen:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_fail_open", Help: "Cumulative reads allowed because Bloom was not authoritative."}),
		bloomRejected:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_rejected", Help: "Cumulative negative keys rejected by Bloom."}),
		bloomSyncErrors: prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_sync_errors", Help: "Cumulative Bloom propagation or rebuild errors."}),
		bloomQueueDepth: prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_update_queue_depth", Help: "Pending Bloom update waiters."}),
		bloomRebuilds:   prometheus.NewGauge(prometheus.GaugeOpts{Name: "turbocache_bloom_rebuilds", Help: "Completed cluster Bloom rebuilds."}),
	}

	reg.MustRegister(
		m.grpcRequests,
		m.grpcLatency,
		m.cacheGetTotal,
		m.cacheGetHit,
		m.cacheGetMiss,
		m.cacheGetFwd,
		m.l1Keys,
		m.l1Size,
		m.ringSize,
		m.bloomReady, m.bloomStale, m.bloomVersion, m.bloomFailOpen,
		m.bloomRejected, m.bloomSyncErrors, m.bloomQueueDepth, m.bloomRebuilds,
	)

	return m
}

func (m *Metrics) ObserveBloom(ready, stale bool, version uint64, failOpen, rejected, syncErrors, rebuilds int64, queueDepth int) {
	if ready {
		m.bloomReady.Set(1)
	} else {
		m.bloomReady.Set(0)
	}
	if stale {
		m.bloomStale.Set(1)
	} else {
		m.bloomStale.Set(0)
	}
	m.bloomVersion.Set(float64(version))
	m.bloomFailOpen.Set(float64(failOpen))
	m.bloomRejected.Set(float64(rejected))
	m.bloomSyncErrors.Set(float64(syncErrors))
	m.bloomRebuilds.Set(float64(rebuilds))
	m.bloomQueueDepth.Set(float64(queueDepth))
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func (m *Metrics) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)

		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}

		method := info.FullMethod
		m.grpcRequests.WithLabelValues(method, code.String()).Inc()
		m.grpcLatency.WithLabelValues(method).Observe(time.Since(start).Seconds())
		return resp, err
	}
}

// NOTE: we deliberately keep these helpers very small.
func (m *Metrics) ObserveL1(keys, bytes int64) {
	m.l1Keys.Set(float64(keys))
	m.l1Size.Set(float64(bytes))
}

func (m *Metrics) ObserveRingNodes(n int) {
	m.ringSize.Set(float64(n))
}

func (m *Metrics) IncGet(result string) {
	m.cacheGetTotal.WithLabelValues(result).Inc()
}

func (m *Metrics) IncHit(level string) {
	m.cacheGetHit.WithLabelValues(level).Inc()
}

func (m *Metrics) IncMiss(level string) {
	m.cacheGetMiss.WithLabelValues(level).Inc()
}

func (m *Metrics) IncForward(outcome string) {
	m.cacheGetFwd.WithLabelValues(outcome).Inc()
}
