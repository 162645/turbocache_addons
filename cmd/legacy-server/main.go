package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"turbocache/internal/cache/engine"
	"turbocache/internal/cache/value"
	"turbocache/internal/cluster/discovery"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/config"
	"turbocache/internal/metrics"
	"turbocache/internal/transport/grpc"
	httpHandler "turbocache/internal/transport/http"
	"turbocache/internal/transport/http/middleware"
	"turbocache/internal/transport/pool"
	"turbocache/internal/utils/logger"
)

// 构建信息，通过 ldflags 设置
var (
	Version   = "dev"
	BuildTime = "unknown"
	GoVersion = "unknown"
)

// TurboCacheServer TurboCache服务器
type TurboCacheServer struct {
	config *config.Config
	logger logger.Logger

	// 核心组件
	l1Cache    *engine.L1Cache
	l2Cache    *engine.L2Cache
	multiCache *engine.MultiLevelCache
	discovery  discovery.DiscoveryService
	hashRing   *hash.ConsistentHash
	connPool   *pool.ConnectionPool
	grpcServer *grpc.Server

	// HTTP 相关组件
	httpServer    *http.Server         // 标准库的 HTTP 服务器
	httpHandler   *httpHandler.Handler // 自定义的 HTTP 处理器
	metricsServer *http.Server         // 监控服务器

	// 监控收集器
	metricsCollector *metrics.Collector

	// 生命周期
	stopCh     chan struct{}
	shutdownWg sync.WaitGroup
}

func main() {
	// 解析命令行参数
	var (
		configPath = flag.String("config", "", "Path to config file")
		version    = flag.Bool("version", false, "Show version information")
		help       = flag.Bool("help", false, "Show help information")
	)
	flag.Parse()

	// 显示版本信息
	if *version {
		fmt.Printf("TurboCache Server\n")
		fmt.Printf("Version: %s\n", Version)
		fmt.Printf("Build Time: %s\n", BuildTime)
		fmt.Printf("Go Version: %s\n", GoVersion)
		os.Exit(0)
	}

	// 显示帮助信息
	if *help {
		fmt.Printf("TurboCache Server - Enterprise Distributed Cache System\n\n")
		fmt.Printf("Usage:\n")
		fmt.Printf("  %s [options]\n\n", os.Args[0])
		fmt.Printf("Options:\n")
		flag.PrintDefaults()
		fmt.Printf("\nEnvironment Variables:\n")
		fmt.Printf("  TURBOCACHE_CONFIG_PATH   Config file path\n")
		fmt.Printf("  TURBOCACHE_LOG_LEVEL     Log level (debug, info, warn, error)\n")
		fmt.Printf("  TURBOCACHE_NODE_ID       Node ID\n")
		fmt.Printf("  TURBOCACHE_CLUSTER_REGION Node region\n")
		fmt.Printf("  TURBOCACHE_CLUSTER_ZONE   Node zone\n")
		os.Exit(0)
	}

	// 从环境变量获取配置路径
	if *configPath == "" {
		if envConfig := os.Getenv("TURBOCACHE_CONFIG_PATH"); envConfig != "" {
			*configPath = envConfig
		}
	}

	// 加载配置
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 应用环境变量覆盖
	applyEnvOverrides(cfg)

	// 初始化日志系统
	logCfg := logger.Config{
		Level:      cfg.Logger.Level,
		Format:     cfg.Logger.Format,
		Output:     cfg.Logger.Output,
		File:       cfg.Logger.File,
		MaxSize:    cfg.Logger.MaxSize,
		MaxAge:     cfg.Logger.MaxAge,
		MaxBackups: cfg.Logger.MaxBackups,
		Compress:   cfg.Logger.Compress,
	}

	if err := logger.Init(logCfg); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	log := logger.WithField("component", "main")
	log.Infof("Starting TurboCache Server %s", Version)

	// 创建服务器实例
	server, err := NewTurboCacheServer(cfg)
	if err != nil {
		log.Fatalf("Failed to create server: %v", err)
	}

	// 启动服务器
	if err := server.Start(); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}

	// 等待信号
	server.WaitForShutdown()

	log.Info("TurboCache Server stopped")
}

// NewTurboCacheServer 创建服务器实例
func NewTurboCacheServer(cfg *config.Config) (*TurboCacheServer, error) {
	server := &TurboCacheServer{
		config: cfg,
		logger: logger.WithField("component", "server"),
		stopCh: make(chan struct{}),
	}

	// 🔧 修复1: 初始化监控收集器（传入配置）
	metricsConfig := &metrics.CollectorConfig{
		EnableDetailedMetrics: true,
		LatencyBuckets:        []float64{0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0},
		CollectionInterval:    15 * time.Second,
	}
	server.metricsCollector = metrics.NewCollector(metricsConfig)

	// 初始化L1缓存
	if cfg.Cache.L1.Enabled {
		l1Cache, err := engine.NewL1Cache(&cfg.Cache.L1)
		if err != nil {
			return nil, fmt.Errorf("failed to create L1 cache: %v", err)
		}
		server.l1Cache = l1Cache
		server.logger.Info("L1 cache initialized")
	}

	// 初始化一致性哈希
	hashFunc := hash.GetHashFunc(cfg.Cluster.Hash.HashFunction)
	server.hashRing = hash.NewConsistentHash(cfg.Cluster.Hash.VirtualNodes, hashFunc)
	server.logger.Info("Consistent hash ring initialized")

	// 初始化连接池
	poolCfg := &pool.Config{
		MaxSize:             cfg.Transport.Pool.MaxSize,
		MinSize:             cfg.Transport.Pool.MinSize,
		MaxIdleTime:         cfg.Transport.Pool.MaxIdleTime,
		DialTimeout:         cfg.Transport.Pool.DialTimeout,
		HealthCheckInterval: cfg.Transport.Pool.HealthCheckInterval,
	}
	server.connPool = pool.NewConnectionPool(poolCfg)
	server.logger.Info("Connection pool initialized")

	// 初始化服务发现
	if cfg.Cluster.Discovery.Type == "etcd" {
		discoveryService, err := discovery.NewEtcdDiscovery(&cfg.Cluster.Discovery)
		if err != nil {
			return nil, fmt.Errorf("failed to create discovery service: %v", err)
		}
		server.discovery = discoveryService
		server.logger.Info("Service discovery initialized")

		// 加载现有节点到 hashRing
		if err := server.loadInitialNodes(context.Background()); err != nil {
			server.logger.Warnf("Failed to load initial nodes: %v", err)
		}
	}

	// 初始化L2缓存
	if cfg.Cache.L2.Enabled {
		l2Cache, err := engine.NewL2Cache(
			&cfg.Cache.L2,
			&cfg.Cache.Protection,
			server.hashRing,
			server.connPool,
			cfg.Cluster.NodeID,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create L2 cache: %v", err)
		}
		server.l2Cache = l2Cache
		server.logger.Info("L2 cache initialized")
	}

	// 初始化多级缓存
	server.multiCache = engine.NewMultiLevelCache(
		&cfg.Cache,
		server.l1Cache,
		server.l2Cache,
	)
	server.logger.Info("Multi-level cache initialized")

	// 🔧 修复2: 使用适配器包装 multiCache
	cacheAdapter := &MultiLevelCacheAdapter{cache: server.multiCache}
	server.httpHandler = httpHandler.NewHandler(cacheAdapter, server.metricsCollector)
	server.logger.Info("HTTP handler initialized")

	return server, nil
}

// 🔧 修复2: MultiLevelCacheAdapter 适配器
// 将 MultiLevelCache 的 Stats() 方法适配为返回 interface{}
type MultiLevelCacheAdapter struct {
	cache *engine.MultiLevelCache
}

func (a *MultiLevelCacheAdapter) Get(ctx context.Context, key string) (*value.Entry, error) {
	return a.cache.Get(ctx, key)
}

func (a *MultiLevelCacheAdapter) Set(ctx context.Context, key string, entry *value.Entry) error {
	return a.cache.Set(ctx, key, entry)
}

func (a *MultiLevelCacheAdapter) Delete(ctx context.Context, key string) error {
	return a.cache.Delete(ctx, key)
}

func (a *MultiLevelCacheAdapter) Clear(ctx context.Context) error {
	return a.cache.Clear(ctx)
}

// 🔧 修复2: 适配 Stats 方法，将 *MultiLevelCacheStats 转为 interface{}
func (a *MultiLevelCacheAdapter) Stats() (interface{}, error) {
	return a.cache.Stats()
}

// loadInitialNodes 加载初始节点到 hashRing
func (s *TurboCacheServer) loadInitialNodes(ctx context.Context) error {
	if s.discovery == nil {
		return nil
	}

	nodes, err := s.discovery.GetNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to get initial nodes: %v", err)
	}

	for _, node := range nodes {
		hashNode := &hash.Node{
			ID:       node.ID,
			Address:  node.Address,
			Weight:   node.Weight,
			Metadata: node.Metadata,
		}
		s.hashRing.AddNode(hashNode)
		s.logger.Infof("Loaded initial node: %s (%s)", node.ID, node.Address)
	}

	return nil
}

// Start 启动服务器
func (s *TurboCacheServer) Start() error {
	ctx := context.Background()

	// 启动gRPC服务器
	if s.config.Transport.GRPC.Enabled {
		grpcServer, err := grpc.NewServer(
			s.config,
			s.multiCache,
			s.hashRing,
			s.connPool,
		)
		if err != nil {
			return fmt.Errorf("failed to create gRPC server: %v", err)
		}

		s.grpcServer = grpcServer
		s.shutdownWg.Add(1)
		go func() {
			defer s.shutdownWg.Done()
			if err := grpcServer.Start(); err != nil {
				s.logger.Errorf("gRPC server error: %v", err)
			}
		}()
		s.logger.Infof("gRPC server started on port %d", s.config.Transport.GRPC.Port)
	}

	// 启动HTTP服务器
	if s.config.Transport.HTTP.Enabled {
		s.httpServer = s.createHTTPServer()
		s.shutdownWg.Add(1)
		go func() {
			defer s.shutdownWg.Done()
			s.logger.Infof("HTTP server starting on port %d", s.config.Transport.HTTP.Port)
			if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Errorf("HTTP server error: %v", err)
			}
		}()
	}

	// 启动监控服务器
	if s.config.Metrics.Enabled {
		s.metricsServer = s.createMetricsServer()
		s.shutdownWg.Add(1)
		go func() {
			defer s.shutdownWg.Done()
			s.logger.Infof("Metrics server starting on port %d", s.config.Metrics.Port)
			if err := s.metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				s.logger.Errorf("Metrics server error: %v", err)
			}
		}()
	}

	// 注册到服务发现
	if s.discovery != nil {
		nodeInfo := &discovery.NodeInfo{
			ID:       s.config.Cluster.NodeID,
			Address:  fmt.Sprintf("%s:%d", s.getLocalIP(), s.config.Transport.GRPC.Port),
			Weight:   s.config.Cluster.Weight,
			Region:   s.config.Cluster.Region,
			Zone:     s.config.Cluster.Zone,
			Metadata: s.config.Cluster.Metadata,
			Status:   "healthy",
		}

		if err := s.discovery.Register(ctx, nodeInfo); err != nil {
			return fmt.Errorf("failed to register node: %v", err)
		}

		// 启动节点发现监听
		discoveryCtx, discoveryCancel := context.WithCancel(context.Background())
		s.shutdownWg.Add(1)
		go func() {
			defer s.shutdownWg.Done()
			defer discoveryCancel()
			s.startNodeDiscovery(discoveryCtx)
		}()

		s.logger.Infof("Node registered: %s", nodeInfo.ID)
	}

	s.logger.Info("TurboCache server started successfully")
	return nil
}

// WaitForShutdown 等待关闭信号
func (s *TurboCacheServer) WaitForShutdown() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigCh:
		s.logger.Infof("Received signal: %v", sig)
	case <-s.stopCh:
		s.logger.Info("Shutdown requested")
	}

	s.shutdown()
}

// shutdown 优雅关闭
func (s *TurboCacheServer) shutdown() {
	s.logger.Info("Shutting down TurboCache server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 从服务发现注销
	if s.discovery != nil {
		if err := s.discovery.Unregister(ctx, s.config.Cluster.NodeID); err != nil {
			s.logger.Warnf("Failed to unregister node: %v", err)
		}
		s.discovery.Close()
	}

	// 优雅关闭HTTP服务器
	if s.httpServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Warnf("HTTP server shutdown error: %v", err)
		}
		shutdownCancel()
	}

	// 优雅关闭Metrics服务器
	if s.metricsServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := s.metricsServer.Shutdown(shutdownCtx); err != nil {
			s.logger.Warnf("Metrics server shutdown error: %v", err)
		}
		shutdownCancel()
	}

	// 关闭gRPC服务器
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	// 关闭连接池
	if s.connPool != nil {
		s.connPool.Close()
	}

	// 关闭多级缓存
	if s.multiCache != nil {
		if err := s.multiCache.Close(); err != nil {
			s.logger.Warnf("Failed to close multi-level cache: %v", err)
		}
	}

	// 等待所有后台 goroutine 完成
	s.shutdownWg.Wait()

	close(s.stopCh)
	s.logger.Info("Server shutdown completed")
}

// createHTTPServer 创建HTTP服务器
func (s *TurboCacheServer) createHTTPServer() *http.Server {
	mux := http.NewServeMux()

	// 注册缓存操作路由
	s.httpHandler.RegisterRoutes(mux)

	// 应用中间件链
	handler := middleware.Chain(
		middleware.Recovery(s.logger),
		middleware.Logger(s.logger),
		middleware.Metrics(s.metricsCollector),
		middleware.CORS([]string{"*"}),
	)(mux)

	return &http.Server{
		Addr:         fmt.Sprintf(":%d", s.config.Transport.HTTP.Port),
		Handler:      handler,
		ReadTimeout:  s.config.Server.ReadTimeout,
		WriteTimeout: s.config.Server.WriteTimeout,
		IdleTimeout:  s.config.Server.IdleTimeout,
	}
}

// createMetricsServer 创建监控服务器
func (s *TurboCacheServer) createMetricsServer() *http.Server {
	mux := http.NewServeMux()

	// Prometheus metrics端点
	mux.HandleFunc(s.config.Metrics.Path, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		// 基础构建信息
		w.Write([]byte("# TurboCache metrics\n"))
		w.Write([]byte(fmt.Sprintf("# TYPE turbocache_build_info gauge\n")))
		w.Write([]byte(fmt.Sprintf("turbocache_build_info{version=\"%s\"} 1\n", Version)))

		// 🔧 修复3: 正确处理 MultiLevelCacheStats 类型
		if s.multiCache != nil {
			stats, err := s.multiCache.Stats()
			if err == nil && stats != nil {
				// 直接使用结构体字段访问
				w.Write([]byte(fmt.Sprintf("# TYPE turbocache_total_requests counter\n")))
				w.Write([]byte(fmt.Sprintf("turbocache_total_requests %d\n", stats.TotalRequests)))

				w.Write([]byte(fmt.Sprintf("# TYPE turbocache_l1_hits counter\n")))
				w.Write([]byte(fmt.Sprintf("turbocache_l1_hits %d\n", stats.L1Hits)))

				w.Write([]byte(fmt.Sprintf("# TYPE turbocache_l2_hits counter\n")))
				w.Write([]byte(fmt.Sprintf("turbocache_l2_hits %d\n", stats.L2Hits)))

				w.Write([]byte(fmt.Sprintf("# TYPE turbocache_misses counter\n")))
				w.Write([]byte(fmt.Sprintf("turbocache_misses %d\n", stats.Misses)))

				w.Write([]byte(fmt.Sprintf("# TYPE turbocache_hit_rate gauge\n")))
				w.Write([]byte(fmt.Sprintf("turbocache_hit_rate %.4f\n", stats.OverallHitRate)))

				// 🔧 修复: L1Stats 和 L2Stats 使用正确的字段名
				if stats.L1Stats != nil {
					w.Write([]byte(fmt.Sprintf("# TYPE turbocache_l1_keys gauge\n")))
					w.Write([]byte(fmt.Sprintf("turbocache_l1_keys %d\n", stats.L1Stats.TotalKeys)))

					w.Write([]byte(fmt.Sprintf("# TYPE turbocache_l1_size_bytes gauge\n")))
					w.Write([]byte(fmt.Sprintf("turbocache_l1_size_bytes %d\n", stats.L1Stats.TotalSize)))
				}

				if stats.L2Stats != nil {
					w.Write([]byte(fmt.Sprintf("# TYPE turbocache_l2_keys gauge\n")))
					w.Write([]byte(fmt.Sprintf("turbocache_l2_keys %d\n", stats.L2Stats.TotalKeys)))

					w.Write([]byte(fmt.Sprintf("# TYPE turbocache_l2_size_bytes gauge\n")))
					w.Write([]byte(fmt.Sprintf("turbocache_l2_size_bytes %d\n", stats.L2Stats.TotalSize)))
				}
			}
		}
	})

	// 健康检查端点
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "healthy",
			"version": Version,
		})
	})

	return &http.Server{
		Addr:    fmt.Sprintf(":%d", s.config.Metrics.Port),
		Handler: mux,
	}
}

// startNodeDiscovery 启动节点发现监听
func (s *TurboCacheServer) startNodeDiscovery(ctx context.Context) {
	eventCh, err := s.discovery.Watch(ctx)
	if err != nil {
		s.logger.Errorf("Failed to start node discovery: %v", err)
		return
	}

	s.logger.Info("Node discovery started")

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("Node discovery stopped")
			return
		case event, ok := <-eventCh:
			if !ok {
				s.logger.Warn("Node discovery event channel closed")
				return
			}
			s.handleNodeEvent(event)
		}
	}
}

// handleNodeEvent 处理节点事件
func (s *TurboCacheServer) handleNodeEvent(event *discovery.NodeEvent) {
	node := &hash.Node{
		ID:       event.Node.ID,
		Address:  event.Node.Address,
		Weight:   event.Node.Weight,
		Metadata: event.Node.Metadata,
	}

	switch event.Type {
	case discovery.EventNodeAdded:
		s.hashRing.AddNode(node)
		s.logger.Infof("Node added to hash ring: %s", node.ID)

	case discovery.EventNodeUpdated:
		s.hashRing.UpdateNode(node)
		s.logger.Debugf("Node updated in hash ring: %s", node.ID)

	case discovery.EventNodeRemoved:
		s.hashRing.RemoveNode(node.ID)
		s.connPool.RemoveTarget(node.Address)
		s.logger.Infof("Node removed from hash ring: %s", node.ID)
	}
}

// getLocalIP 获取本地IP地址
func (s *TurboCacheServer) getLocalIP() string {
	if host := os.Getenv("HOST"); host != "" {
		return host
	}
	return "127.0.0.1"
}

// applyEnvOverrides 应用环境变量覆盖配置
func applyEnvOverrides(cfg *config.Config) {
	if nodeID := os.Getenv("TURBOCACHE_NODE_ID"); nodeID != "" {
		cfg.Cluster.NodeID = nodeID
	}

	if region := os.Getenv("TURBOCACHE_CLUSTER_REGION"); region != "" {
		cfg.Cluster.Region = region
	}

	if zone := os.Getenv("TURBOCACHE_CLUSTER_ZONE"); zone != "" {
		cfg.Cluster.Zone = zone
	}

	if logLevel := os.Getenv("TURBOCACHE_LOG_LEVEL"); logLevel != "" {
		cfg.Logger.Level = logLevel
	}
}
