package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"turbocache/internal/cluster/discovery"
	"turbocache/internal/config"
	"turbocache/internal/server"
)

func main() {
	var (
		nodeID        = flag.String("node-id", envOr("TC_NODE_ID", ""), "node id (required in multi-node deployment)")
		listenAddr    = flag.String("listen", envOr("TC_GRPC_ADDR", ":7070"), "gRPC listen addr")
		advertiseAddr = flag.String("advertise", envOr("TC_ADVERTISE_ADDR", ""), "advertise addr for peers (host:port)")
		metricsAddr   = flag.String("metrics", envOr("TC_METRICS_ADDR", ":9090"), "HTTP metrics listen addr")
		nodeWeight    = flag.Int("weight", envOrInt("TC_NODE_WEIGHT", 1), "node weight for consistent hashing")

		etcdEndpoints = flag.String("etcd", envOr("TC_ETCD_ENDPOINTS", ""), "comma-separated etcd endpoints, e.g. http://etcd:2379")
		registerPath  = flag.String("etcd-path", envOr("TC_ETCD_PATH", "/turbocache/nodes"), "etcd register path")
		leaseTTL      = flag.Duration("etcd-lease-ttl", envOrDur("TC_ETCD_LEASE_TTL", 10*time.Second), "etcd lease ttl")

		hashReplicas    = flag.Int("hash-replicas", envOrInt("TC_HASH_REPLICAS", 128), "virtual nodes per weight")
		maxForwardDepth = flag.Int("max-forward-depth", envOrInt("TC_FORWARD_MAX_DEPTH", 3), "max forward depth for Get")
		promoteForward  = flag.Bool("promote", envOrBool("TC_PROMOTE_ON_FORWARD", true), "promote forwarded Get() value to local L1")

		l1MaxSize      = flag.Int64("l1-max-bytes", envOrInt64("TC_L1_MAX_SIZE", 64*1024*1024), "L1 max bytes")
		l1MaxEntries   = flag.Int64("l1-max-entries", envOrInt64("TC_L1_MAX_ENTRIES", 100000), "L1 max entries")
		l1Shards       = flag.Int("l1-shards", envOrInt("TC_L1_SHARDS", 64), "L1 shard count")
		evictionPolicy = flag.String("eviction", envOr("TC_EVICTION_POLICY", "lru"), "L1 eviction policy: lru, lfu or arc")

		l2Enabled         = flag.Bool("l2", envOrBool("TC_L2_ENABLED", true), "enable distributed L2 path")
		replicationFactor = flag.Int("replication-factor", envOrInt("TC_REPLICATION_FACTOR", 2), "number of hash-ring replicas")
		consistency       = flag.String("consistency", envOr("TC_CONSISTENCY", "strong"), "replication consistency: strong or eventual")
		singleFlight      = flag.Bool("singleflight", envOrBool("TC_SINGLEFLIGHT_ENABLED", true), "coalesce concurrent L2 reads")
		circuitBreaker    = flag.Bool("circuit-breaker", envOrBool("TC_CIRCUIT_BREAKER_ENABLED", true), "enable per-peer circuit breakers")
		negativeCache     = flag.Bool("negative-cache", envOrBool("TC_NEGATIVE_CACHE_ENABLED", true), "cache distributed misses")
		bloomEnabled      = flag.Bool("bloom", envOrBool("TC_BLOOM_ENABLED", true), "enable fail-open cluster Bloom protection")
		bloomExpected     = flag.Int64("bloom-expected", envOrInt64("TC_BLOOM_EXPECTED_ITEMS", 200000), "expected cluster Bloom keys")
		bloomFPR          = flag.Float64("bloom-fpr", envOrFloat64("TC_BLOOM_FALSE_POSITIVE_RATE", 0.01), "target Bloom false-positive rate")
		bloomBatch        = flag.Int("bloom-batch", envOrInt("TC_BLOOM_BATCH_SIZE", 100), "maximum Bloom propagation batch")
		bloomFlush        = flag.Duration("bloom-flush", envOrDur("TC_BLOOM_FLUSH_INTERVAL", time.Millisecond), "Bloom batch flush interval")
		bloomRebuild      = flag.Duration("bloom-rebuild", envOrDur("TC_BLOOM_REBUILD_INTERVAL", 5*time.Minute), "cluster Bloom rebuild interval")
		bloomBootstrap    = flag.Duration("bloom-bootstrap", envOrDur("TC_BLOOM_BOOTSTRAP_DELAY", 2*time.Second), "Bloom snapshot bootstrap retry interval")

		enableHC  = flag.Bool("health-check", envOrBool("TC_HEALTH_CHECK", true), "enable peer health check")
		hcIntv    = flag.Duration("hc-interval", envOrDur("TC_HC_INTERVAL", 5*time.Second), "health check interval")
		hcTimeout = flag.Duration("hc-timeout", envOrDur("TC_HC_TIMEOUT", 2*time.Second), "health check timeout")
	)

	flag.Parse()

	if *nodeID == "" {
		// best-effort default: hostname
		if h, err := os.Hostname(); err == nil {
			*nodeID = h
		}
	}

	if *advertiseAddr == "" {
		*advertiseAddr = strings.TrimPrefix(*listenAddr, ":")
		// If listen is ":7070", advertise as "<hostname>:7070" is better in docker.
		// When running in docker-compose, you should explicitly set TC_ADVERTISE_ADDR.
		if !strings.Contains(*advertiseAddr, ":") {
			*advertiseAddr = fmt.Sprintf("%s%s", hostnameOr("127.0.0.1"), *listenAddr)
		}
	}

	l1Cfg := &config.L1CacheConfig{
		MaxSize:        *l1MaxSize,
		MaxEntries:     *l1MaxEntries,
		ShardCount:     *l1Shards,
		EvictionPolicy: strings.ToLower(*evictionPolicy),
	}
	l2Cfg := &config.L2CacheConfig{
		Enabled: *l2Enabled, ReplicationFactor: *replicationFactor,
		ConsistencyLevel: strings.ToLower(*consistency), SyncTimeout: 2 * time.Second,
	}
	protectionCfg := &config.ProtectionConfig{
		BloomFilter: config.BloomFilterConfig{
			Enabled: *bloomEnabled, ExpectedItems: *bloomExpected,
			FalsePositiveRate: *bloomFPR, BatchSize: *bloomBatch,
			FlushInterval: *bloomFlush, RefreshInterval: *bloomRebuild,
			QueueSize: 16384, BootstrapDelay: *bloomBootstrap,
		},
		SingleFlight:  config.SingleFlightConfig{Enabled: *singleFlight},
		NegativeCache: config.NegativeCacheConfig{Enabled: *negativeCache, TTL: 3 * time.Second, MaxSize: 100000},
		CircuitBreaker: config.CircuitBreakerConfig{
			Enabled: *circuitBreaker, Threshold: 5, Timeout: 5 * time.Second,
			MaxRequests: 3, FailureRatio: 0.5,
		},
	}

	srvCfg := &server.Config{
		NodeID:           *nodeID,
		ListenAddr:       *listenAddr,
		AdvertiseAddr:    *advertiseAddr,
		MetricsAddr:      *metricsAddr,
		NodeWeight:       *nodeWeight,
		HashReplicas:     *hashReplicas,
		MaxForwardDepth:  *maxForwardDepth,
		PromoteOnForward: *promoteForward,
		L1:               l1Cfg,
		L2:               l2Cfg,
		Protection:       protectionCfg,
		DisableDiscovery: *etcdEndpoints == "",
	}

	if *enableHC {
		srvCfg.HealthCheck = &discovery.HealthCheckConfig{
			Interval:         *hcIntv,
			Timeout:          *hcTimeout,
			FailureThreshold: 2,
			SuccessThreshold: 1,
			MaxConcurrency:   16,
		}
	}

	if *etcdEndpoints != "" {
		eps := splitComma(*etcdEndpoints)
		srvCfg.Discovery = &config.DiscoveryConfig{
			Endpoints:         eps,
			RegisterPath:      *registerPath,
			LeaseTTL:          *leaseTTL,
			HeartbeatInterval: *leaseTTL / 3,
		}
	}

	srv, err := server.NewNodeServer(srvCfg)
	if err != nil {
		panic(err)
	}

	if err := server.RunWithSignal(srv); err != nil {
		panic(err)
	}
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	v := envOr(key, "")
	if v == "" {
		return def
	}
	var x int
	_, err := fmt.Sscanf(v, "%d", &x)
	if err != nil {
		return def
	}
	return x
}

func envOrInt64(key string, def int64) int64 {
	v := envOr(key, "")
	if v == "" {
		return def
	}
	var x int64
	_, err := fmt.Sscanf(v, "%d", &x)
	if err != nil {
		return def
	}
	return x
}

func envOrBool(key string, def bool) bool {
	v := strings.ToLower(envOr(key, ""))
	if v == "" {
		return def
	}
	switch v {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return def
	}
}

func envOrFloat64(key string, def float64) float64 {
	v := envOr(key, "")
	if v == "" {
		return def
	}
	var x float64
	if _, err := fmt.Sscanf(v, "%f", &x); err != nil {
		return def
	}
	return x
}

func envOrDur(key string, def time.Duration) time.Duration {
	v := envOr(key, "")
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

func splitComma(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func hostnameOr(def string) string {
	if h, err := os.Hostname(); err == nil && h != "" {
		return h
	}
	return def
}
