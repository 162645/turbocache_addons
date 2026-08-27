package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"

	"turbocache/api/generated/cachepb"
	"turbocache/internal/cache/engine"
	"turbocache/internal/cluster/discovery"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/config"
	"turbocache/internal/transport/pool"
	"turbocache/internal/utils/logger"
)

// Config collects the minimal settings needed to boot a node.
type Config struct {
	NodeID        string
	ListenAddr    string // gRPC listen, e.g. ":7070"
	AdvertiseAddr string // peers connect to this, e.g. "turbocache-node-1:7070"
	MetricsAddr   string // HTTP listen, e.g. ":9090"
	NodeWeight    int

	HashReplicas     int
	MaxForwardDepth  int
	PromoteOnForward bool

	L1         *config.L1CacheConfig
	L2         *config.L2CacheConfig
	Protection *config.ProtectionConfig
	Discovery  *config.DiscoveryConfig

	// If true, do NOT register to etcd (useful for unit tests).
	DisableDiscovery bool

	// Optional: health checker (if nil, disabled)
	HealthCheck *discovery.HealthCheckConfig
}

// NodeServer is the running turbocache node.
type NodeServer struct {
	cfg *Config

	l1    *engine.L1Cache
	l2    *engine.L2Cache
	cache *engine.MultiLevelCache
	pool  *pool.ConnectionPool
	ring  *hash.ConsistentHash

	conns   *ConnManager
	metrics *Metrics

	grpcServer *grpc.Server
	healthSrv  *health.Server

	httpServer *http.Server

	discovery discovery.DiscoveryService
	hc        *discovery.HealthChecker

	nodesMu sync.RWMutex
	nodes   map[string]*discovery.NodeInfo // latest from discovery

	stopOnce sync.Once

	logger logger.Logger
}

func NewNodeServer(cfg *Config) (*NodeServer, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NodeID is required")
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":7070"
	}
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.ListenAddr
	}
	if cfg.MetricsAddr == "" {
		cfg.MetricsAddr = ":9090"
	}
	if cfg.HashReplicas <= 0 {
		cfg.HashReplicas = 128
	}
	if cfg.MaxForwardDepth <= 0 {
		cfg.MaxForwardDepth = 3
	}
	if cfg.NodeWeight <= 0 {
		cfg.NodeWeight = 1
	}
	if cfg.L1 == nil {
		cfg.L1 = &config.L1CacheConfig{
			MaxSize:    64 * 1024 * 1024,
			MaxEntries: 100000,
			ShardCount: 64,
		}
	}
	if cfg.L2 == nil {
		cfg.L2 = &config.L2CacheConfig{Enabled: true, ReplicationFactor: 2, ConsistencyLevel: "strong", SyncTimeout: 2 * time.Second}
	}

	return &NodeServer{
		cfg:     cfg,
		conns:   NewConnManager(3 * time.Second),
		ring:    hash.NewConsistentHash(cfg.HashReplicas, nil),
		metrics: NewMetrics(),
		nodes:   make(map[string]*discovery.NodeInfo),
		logger:  logger.WithField("component", "node_server"),
	}, nil
}

// Start boots:
//   - L1 cache
//   - optional etcd discovery + watch
//   - gRPC server (CacheService + health)
//   - HTTP server (/metrics, /stats)
func (s *NodeServer) Start(ctx context.Context) error {
	// 1) L1
	l1, err := engine.NewL1Cache(s.cfg.L1)
	if err != nil {
		return err
	}
	s.l1 = l1

	// 2) Discovery
	if !s.cfg.DisableDiscovery {
		if s.cfg.Discovery == nil {
			return fmt.Errorf("Discovery config is required unless DisableDiscovery=true")
		}

		disc, err := discovery.NewEtcdDiscovery(s.cfg.Discovery)
		if err != nil {
			return err
		}
		s.discovery = disc

		// Register self
		node := &discovery.NodeInfo{
			ID:      s.cfg.NodeID,
			Address: s.cfg.AdvertiseAddr,
			Weight:  s.cfg.NodeWeight,
			Status:  "healthy",
			Metadata: map[string]string{
				"metrics_addr": s.cfg.MetricsAddr,
			},
		}

		if err := s.discovery.Register(ctx, node); err != nil {
			return fmt.Errorf("register node: %w", err)
		}

		// Initial load
		if err := s.refreshNodes(ctx); err != nil {
			s.logger.Warnf("initial refreshNodes failed: %v", err)
		}

		// Watch
		evCh, err := s.discovery.Watch(ctx)
		if err != nil {
			return fmt.Errorf("watch nodes: %w", err)
		}
		go s.watchLoop(evCh)

		// Optional: health checker
		if s.cfg.HealthCheck != nil {
			s.hc = discovery.NewHealthChecker(s.cfg.HealthCheck)
			s.hc.SetHealthChangeCallback(func(nodeID string, healthy bool) {
				s.onHealthChange(nodeID, healthy)
			})
			s.hc.Start()
		}
	} else {
		// At minimum, add self to ring so forwarding logic can work.
		s.ring.AddNode(&hash.Node{ID: s.cfg.NodeID, Address: s.cfg.AdvertiseAddr, Weight: s.cfg.NodeWeight})
		s.metrics.ObserveRingNodes(len(s.ring.GetAllNodes()))
	}

	// 3) Compose the production data path. Every public operation now enters
	// MultiLevelCache; L2 owns routing, SingleFlight, replication and breakers.
	var l2 engine.L2CacheInterface
	if s.cfg.L2.Enabled {
		s.pool = pool.NewConnectionPool(&pool.Config{
			MaxSize: 32, MinSize: 0, MaxIdleTime: 5 * time.Minute,
			DialTimeout: 2 * time.Second, HealthCheckInterval: 15 * time.Second,
		})
		s.l2, err = engine.NewL2Cache(s.cfg.L2, s.cfg.Protection, s.ring, s.pool, s.cfg.NodeID)
		if err != nil {
			return fmt.Errorf("create L2 cache: %w", err)
		}
		l2 = s.l2
		s.l2.SetBloomKeysProvider(s.l1.Keys)
	}
	cacheCfg := &config.CacheConfig{L1: *s.cfg.L1, L2: *s.cfg.L2}
	if s.cfg.Protection != nil {
		cacheCfg.Protection = *s.cfg.Protection
	}
	s.cache = engine.NewMultiLevelCache(cacheCfg, s.l1, l2)

	// 4) gRPC server
	lis, err := net.Listen("tcp", s.cfg.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.cfg.ListenAddr, err)
	}

	s.healthSrv = health.NewServer()
	s.healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)

	// metrics interceptor
	grpcSrv := grpc.NewServer(
		grpc.UnaryInterceptor(s.metrics.UnaryServerInterceptor()),
	)
	s.grpcServer = grpcSrv

	// cache service
	cacheSvc, err := NewCacheService(CacheServiceOptions{
		Cache:            s.l1,
		MultiCache:       s.cache,
		L2:               s.l2,
		NodeID:           s.cfg.NodeID,
		Ring:             s.ring,
		Conns:            s.conns,
		MaxForwardDepth:  s.cfg.MaxForwardDepth,
		PromoteOnForward: s.cfg.PromoteOnForward,
		Metrics:          s.metrics,
	})
	if err != nil {
		return err
	}

	cachepb.RegisterCacheServiceServer(grpcSrv, cacheSvc)
	grpc_health_v1.RegisterHealthServer(grpcSrv, s.healthSrv)

	go func() {
		s.logger.Infof("gRPC listening on %s (advertise=%s)", s.cfg.ListenAddr, s.cfg.AdvertiseAddr)
		if serveErr := grpcSrv.Serve(lis); serveErr != nil {
			s.logger.Errorf("gRPC serve error: %v", serveErr)
		}
	}()

	// 5) HTTP metrics + stats
	mux := http.NewServeMux()
	mux.Handle("/metrics", s.metrics.Handler())
	mux.HandleFunc("/stats", s.handleStats)
	mux.HandleFunc("/ring", s.handleRing)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	s.httpServer = &http.Server{
		Addr:              s.cfg.MetricsAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		s.logger.Infof("HTTP listening on %s (/metrics,/stats,/ring)", s.cfg.MetricsAddr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Errorf("http serve error: %v", err)
		}
	}()

	// 6) periodic gauges
	go s.statsLoop()

	return nil
}

func (s *NodeServer) statsLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if s.l1 == nil {
			continue
		}
		st, err := s.l1.Stats()
		if err != nil {
			continue
		}
		s.metrics.ObserveL1(st.TotalKeys, st.TotalSize)
		s.metrics.ObserveRingNodes(len(s.ring.GetAllNodes()))
		if s.l2 != nil {
			if l2Stats, statErr := s.l2.Stats(); statErr == nil {
				s.metrics.ObserveBloom(l2Stats.BloomReady, l2Stats.BloomStale, l2Stats.BloomVersion,
					l2Stats.BloomFailOpen, l2Stats.BloomRejected, l2Stats.BloomSyncErrors,
					l2Stats.BloomRebuilds, l2Stats.BloomQueueDepth)
			}
		}
	}
}

func (s *NodeServer) watchLoop(evCh <-chan *discovery.NodeEvent) {
	for ev := range evCh {
		if ev == nil || ev.Node == nil {
			continue
		}
		switch ev.Type {
		case discovery.EventNodeAdded, discovery.EventNodeUpdated:
			s.upsertNode(ev.Node)
		case discovery.EventNodeRemoved:
			s.removeNode(ev.Node.ID)
		default:
		}
	}
}

func (s *NodeServer) upsertNode(n *discovery.NodeInfo) {
	s.nodesMu.Lock()
	s.nodes[n.ID] = n
	s.nodesMu.Unlock()

	// update ring (remove + add to apply changed addr/weight)
	s.ring.RemoveNode(n.ID)
	s.ring.AddNode(&hash.Node{ID: n.ID, Address: n.Address, Weight: n.Weight, Metadata: n.Metadata})

	// health checker tracking
	if s.hc != nil && n.ID != s.cfg.NodeID {
		_ = s.hc.AddNode(n.ID, n.Address)
	}

	s.metrics.ObserveRingNodes(len(s.ring.GetAllNodes()))
}

func (s *NodeServer) removeNode(nodeID string) {
	s.nodesMu.Lock()
	delete(s.nodes, nodeID)
	s.nodesMu.Unlock()

	s.ring.RemoveNode(nodeID)

	if s.hc != nil {
		s.hc.RemoveNode(nodeID)
	}

	s.metrics.ObserveRingNodes(len(s.ring.GetAllNodes()))
}

func (s *NodeServer) onHealthChange(nodeID string, healthy bool) {
	if nodeID == s.cfg.NodeID {
		return
	}

	// If unhealthy, remove from ring immediately.
	if !healthy {
		s.logger.Warnf("node unhealthy, removing from ring: %s", nodeID)
		s.ring.RemoveNode(nodeID)
		s.metrics.ObserveRingNodes(len(s.ring.GetAllNodes()))
		return
	}

	// If recovered, add back if discovery still knows it.
	s.nodesMu.RLock()
	n := s.nodes[nodeID]
	s.nodesMu.RUnlock()
	if n != nil {
		s.logger.Infof("node recovered, adding back to ring: %s", nodeID)
		s.ring.AddNode(&hash.Node{ID: n.ID, Address: n.Address, Weight: n.Weight, Metadata: n.Metadata})
		s.metrics.ObserveRingNodes(len(s.ring.GetAllNodes()))
	}
}

func (s *NodeServer) refreshNodes(ctx context.Context) error {
	if s.discovery == nil {
		return nil
	}

	nodes, err := s.discovery.GetNodes(ctx)
	if err != nil {
		return err
	}

	for _, n := range nodes {
		if n == nil {
			continue
		}
		s.upsertNode(n)
	}

	return nil
}

func (s *NodeServer) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	out := make(map[string]any)
	out["node_id"] = s.cfg.NodeID
	out["listen_addr"] = s.cfg.ListenAddr
	out["advertise_addr"] = s.cfg.AdvertiseAddr
	out["data_path"] = "grpc -> multi-level -> l1/l2 -> hash ring replicas"

	if s.l1 != nil {
		if st, err := s.l1.Stats(); err == nil {
			out["l1"] = st
		}
	}
	if s.cache != nil {
		if st, err := s.cache.Stats(); err == nil {
			out["multi_level"] = st
		}
	}

	if s.hc != nil {
		out["health"] = s.hc.GetStats()
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

func (s *NodeServer) handleRing(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	out := make(map[string]any)
	out["node_id"] = s.cfg.NodeID
	out["nodes"] = s.ring.GetAllNodes()

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(out)
}

// Stop tries to gracefully stop all components.
func (s *NodeServer) Stop(ctx context.Context) error {
	var retErr error

	s.stopOnce.Do(func() {
		// health: stop serving
		if s.healthSrv != nil {
			s.healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		}

		// discovery: unregister
		if s.discovery != nil {
			uctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			_ = s.discovery.Unregister(uctx, s.cfg.NodeID)
			cancel()
			_ = s.discovery.Close()
		}

		if s.hc != nil {
			s.hc.Stop()
		}

		// gRPC
		if s.grpcServer != nil {
			stopped := make(chan struct{})
			go func() {
				s.grpcServer.GracefulStop()
				close(stopped)
			}()

			select {
			case <-stopped:
			case <-time.After(5 * time.Second):
				s.grpcServer.Stop()
			}
		}

		// HTTP
		if s.httpServer != nil {
			hctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			_ = s.httpServer.Shutdown(hctx)
			cancel()
		}

		// connections
		if s.conns != nil {
			_ = s.conns.Close()
		}

		// cache (MultiLevelCache owns L1 and L2 lifecycle)
		if s.cache != nil {
			_ = s.cache.Close()
		} else if s.l1 != nil {
			_ = s.l1.Close()
		}
		if s.pool != nil {
			_ = s.pool.Close()
		}

		s.logger.Info("node stopped")
	})

	return retErr
}

// RunWithSignal starts the server and blocks until SIGINT/SIGTERM.
func RunWithSignal(srv *NodeServer) error {
	ctx := context.Background()
	if err := srv.Start(ctx); err != nil {
		return err
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigCh
	srv.logger.Infof("received signal: %v, shutting down", sig)

	stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Stop(stopCtx)
}
