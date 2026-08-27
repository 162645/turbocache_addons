package grpc

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"turbocache/api/generated/cachepb"
	"turbocache/internal/cache/engine"
	"turbocache/internal/cache/value"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/config"
	"turbocache/internal/transport/pool"
	"turbocache/internal/utils/logger"
)

// Server gRPC服务器
type Server struct {
	cachepb.UnimplementedCacheServiceServer

	config     *config.Config
	logger     logger.Logger
	grpcServer *grpc.Server
	listener   net.Listener

	// 核心组件 - 使用多级缓存
	multiCache *engine.MultiLevelCache
	hashRing   *hash.ConsistentHash
	connPool   *pool.ConnectionPool

	// 🔧 新增：防链式查询统计
	stats *ServerStats

	// 生命周期
	started bool
	stopped bool
	mu      sync.RWMutex
}

// 🔧 新增：服务器统计信息
type ServerStats struct {
	ForwardedRequests  int64 // 转发的请求数
	ForcedLocalLookups int64 // 强制本地查询次数（异常指标）
	DepthViolations    int64 // 深度违规次数（depth > 1）
}

// NewServer 创建gRPC服务器
func NewServer(
	config *config.Config,
	multiCache *engine.MultiLevelCache,
	hashRing *hash.ConsistentHash,
	connPool *pool.ConnectionPool,
) (*Server, error) {
	server := &Server{
		config:     config,
		logger:     logger.WithField("component", "grpc_server"),
		multiCache: multiCache,
		hashRing:   hashRing,
		connPool:   connPool,
		stats:      &ServerStats{},
	}

	// 创建监听器
	addr := fmt.Sprintf(":%d", config.Transport.GRPC.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %v", addr, err)
	}
	server.listener = listener

	// 创建gRPC服务器
	opts := server.buildServerOptions()
	server.grpcServer = grpc.NewServer(opts...)

	// 注册服务
	cachepb.RegisterCacheServiceServer(server.grpcServer, server)

	// 启用反射（开发环境）
	if config.Logger.Level == "debug" {
		reflection.Register(server.grpcServer)
	}

	return server, nil
}

// buildServerOptions 构建服务器选项
func (s *Server) buildServerOptions() []grpc.ServerOption {
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    s.config.Transport.GRPC.KeepaliveTime,
			Timeout: s.config.Transport.GRPC.KeepaliveTimeout,
		}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
			MinTime:             10 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.MaxRecvMsgSize(int(s.config.Transport.GRPC.MaxRecvMsgSize)),
		grpc.MaxSendMsgSize(int(s.config.Transport.GRPC.MaxSendMsgSize)),
		grpc.ConnectionTimeout(s.config.Transport.GRPC.ConnectionTimeout),
	}

	// 添加拦截器
	opts = append(opts,
		grpc.UnaryInterceptor(s.unaryInterceptor),
		grpc.StreamInterceptor(s.streamInterceptor),
	)

	return opts
}

// Start 启动服务器
func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("server already started")
	}

	s.started = true
	s.logger.Infof("Starting gRPC server on %s", s.listener.Addr().String())

	// 启动服务器
	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			s.logger.Errorf("gRPC server error: %v", err)
		}
	}()

	return nil
}

// Stop 停止服务器
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}

	s.stopped = true
	s.logger.Info("Stopping gRPC server...")

	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}

	if s.listener != nil {
		s.listener.Close()
	}
}

// GracefulStop 优雅停止服务器
func (s *Server) GracefulStop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return
	}

	s.stopped = true
	s.logger.Info("Gracefully stopping gRPC server...")

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// ============================================================
// 🔧 核心修改：Get 方法 - 防止链式查询
// ============================================================

// Get 获取缓存值（修改版 - 防止链式查询）
func (s *Server) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// 🔑 关键步骤1：检查请求深度，防止链式转发
	depth := s.extractRequestDepth(req)

	if depth > 1 {
		// ⚠️ 深度违规：理论上不应该出现 depth > 1
		atomic.AddInt64(&s.stats.DepthViolations, 1)
		s.logger.Errorf("CRITICAL: Request depth > 1 detected: depth=%d, key=%s", depth, req.Key)
	}

	if depth > 0 {
		// ❌ 这是一个已经转发过的请求，只能在本地查询
		atomic.AddInt64(&s.stats.ForcedLocalLookups, 1)
		s.logger.Warnf("Received forwarded request (depth=%d) for key: %s, force local lookup", depth, req.Key)
		return s.getLocal(ctx, req)
	}

	// 🔑 关键步骤2：检查是否是本地键
	if s.isLocalKey(req.Key) {
		return s.getLocal(ctx, req)
	}

	// 🔑 关键步骤3：只转发一次（深度0→1）
	atomic.AddInt64(&s.stats.ForwardedRequests, 1)
	return s.getRemoteOnce(ctx, req)
}

// 🔧 新增：extractRequestDepth 提取请求深度
func (s *Server) extractRequestDepth(req *cachepb.GetRequest) int {
	if req.Metadata == nil || req.Metadata.Headers == nil {
		return 0
	}

	depthStr, ok := req.Metadata.Headers["X-TurboCache-Depth"]
	if !ok {
		return 0
	}

	depth, err := strconv.Atoi(depthStr)
	if err != nil {
		s.logger.Warnf("Invalid depth header: %s", depthStr)
		return 0
	}

	return depth
}

// 🔧 新增：getRemoteOnce 只转发一次到目标节点（深度0→1）
func (s *Server) getRemoteOnce(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	node := s.hashRing.GetNode(req.Key)
	if node == nil {
		return nil, status.Error(codes.Unavailable, "no available nodes")
	}

	var resp *cachepb.GetResponse
	err := s.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		// 🔑 克隆请求并增加深度标记
		forwardReq := &cachepb.GetRequest{
			Key:   req.Key,
			Group: req.Group,
			Metadata: &cachepb.RequestMetadata{
				RequestId: s.generateRequestID(),
				ClientId:  s.config.Cluster.NodeID,
				Headers: map[string]string{
					"X-TurboCache-Depth":  "1", // 标记为已转发一次
					"X-TurboCache-Source": s.config.Cluster.NodeID,
				},
			},
		}

		// 复制原有headers（如果存在）
		if req.Metadata != nil && req.Metadata.Headers != nil {
			for k, v := range req.Metadata.Headers {
				if k != "X-TurboCache-Depth" { // 不复制深度标记
					forwardReq.Metadata.Headers[k] = v
				}
			}
		}

		var err error
		resp, err = client.Get(ctx, forwardReq)
		return err
	})

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// ============================================================
// 🔧 核心修改：Set 方法 - 防止链式查询
// ============================================================

// Set 设置缓存值（修改版 - 防止链式查询）
func (s *Server) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	// 检查请求深度
	depth := s.extractSetRequestDepth(req)

	if depth > 1 {
		atomic.AddInt64(&s.stats.DepthViolations, 1)
		s.logger.Errorf("CRITICAL: SET request depth > 1: depth=%d, key=%s", depth, req.Key)
	}

	if depth > 0 {
		// 强制本地存储
		atomic.AddInt64(&s.stats.ForcedLocalLookups, 1)
		s.logger.Warnf("Received forwarded SET request (depth=%d) for key: %s, force local store", depth, req.Key)
		return s.setLocal(ctx, req)
	}

	if s.isLocalKey(req.Key) {
		return s.setLocal(ctx, req)
	}

	atomic.AddInt64(&s.stats.ForwardedRequests, 1)
	return s.setRemoteOnce(ctx, req)
}

// 🔧 新增：extractSetRequestDepth 提取Set请求深度
func (s *Server) extractSetRequestDepth(req *cachepb.SetRequest) int {
	if req.Metadata == nil || req.Metadata.Headers == nil {
		return 0
	}
	depthStr, ok := req.Metadata.Headers["X-TurboCache-Depth"]
	if !ok {
		return 0
	}
	depth, err := strconv.Atoi(depthStr)
	if err != nil {
		return 0
	}
	return depth
}

// 🔧 新增：setRemoteOnce 只转发一次
func (s *Server) setRemoteOnce(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
	node := s.hashRing.GetNode(req.Key)
	if node == nil {
		return nil, status.Error(codes.Unavailable, "no available nodes")
	}

	var resp *cachepb.SetResponse
	err := s.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		forwardReq := &cachepb.SetRequest{
			Key:     req.Key,
			Value:   req.Value,
			Group:   req.Group,
			Ttl:     req.Ttl,
			Options: req.Options,
			Metadata: &cachepb.RequestMetadata{
				RequestId: s.generateRequestID(),
				ClientId:  s.config.Cluster.NodeID,
				Headers: map[string]string{
					"X-TurboCache-Depth":  "1",
					"X-TurboCache-Source": s.config.Cluster.NodeID,
				},
			},
		}

		var err error
		resp, err = client.Set(ctx, forwardReq)
		return err
	})

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// ============================================================
// 🔧 核心修改：Delete 方法 - 防止链式查询
// ============================================================

// Delete 删除缓存值（修改版 - 防止链式查询）
func (s *Server) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	depth := s.extractDeleteRequestDepth(req)

	if depth > 1 {
		atomic.AddInt64(&s.stats.DepthViolations, 1)
		s.logger.Errorf("CRITICAL: DELETE request depth > 1: depth=%d, key=%s", depth, req.Key)
	}

	if depth > 0 {
		atomic.AddInt64(&s.stats.ForcedLocalLookups, 1)
		s.logger.Warnf("Received forwarded DELETE request (depth=%d) for key: %s, force local delete", depth, req.Key)
		return s.deleteLocal(ctx, req)
	}

	if s.isLocalKey(req.Key) {
		return s.deleteLocal(ctx, req)
	}

	atomic.AddInt64(&s.stats.ForwardedRequests, 1)
	return s.deleteRemoteOnce(ctx, req)
}

// 🔧 新增：extractDeleteRequestDepth 提取Delete请求深度
func (s *Server) extractDeleteRequestDepth(req *cachepb.DeleteRequest) int {
	if req.Metadata == nil || req.Metadata.Headers == nil {
		return 0
	}
	depthStr, ok := req.Metadata.Headers["X-TurboCache-Depth"]
	if !ok {
		return 0
	}
	depth, err := strconv.Atoi(depthStr)
	if err != nil {
		return 0
	}
	return depth
}

// 🔧 新增：deleteRemoteOnce 只转发一次
func (s *Server) deleteRemoteOnce(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	node := s.hashRing.GetNode(req.Key)
	if node == nil {
		return nil, status.Error(codes.Unavailable, "no available nodes")
	}

	var resp *cachepb.DeleteResponse
	err := s.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		forwardReq := &cachepb.DeleteRequest{
			Key:   req.Key,
			Group: req.Group,
			Metadata: &cachepb.RequestMetadata{
				RequestId: s.generateRequestID(),
				ClientId:  s.config.Cluster.NodeID,
				Headers: map[string]string{
					"X-TurboCache-Depth":  "1",
					"X-TurboCache-Source": s.config.Cluster.NodeID,
				},
			},
		}

		var err error
		resp, err = client.Delete(ctx, forwardReq)
		return err
	})

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// ============================================================
// 批量操作（保持原有逻辑）
// ============================================================

// BatchGet 批量获取
func (s *Server) BatchGet(ctx context.Context, req *cachepb.BatchGetRequest) (*cachepb.BatchGetResponse, error) {
	if len(req.Keys) == 0 {
		return &cachepb.BatchGetResponse{
			Results: []*cachepb.GetResult{},
			Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
		}, nil
	}

	// 按节点分组键
	keysByNode := s.groupKeysByNode(req.Keys)

	// 并发处理每个节点的请求
	results := make([]*cachepb.GetResult, 0, len(req.Keys))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for nodeAddr, keys := range keysByNode {
		wg.Add(1)
		go func(addr string, ks []string) {
			defer wg.Done()

			var nodeResults []*cachepb.GetResult
			if addr == "local" {
				nodeResults = s.batchGetLocal(ctx, ks)
			} else {
				nodeResults = s.batchGetRemote(ctx, addr, ks)
			}

			mu.Lock()
			results = append(results, nodeResults...)
			mu.Unlock()
		}(nodeAddr, keys)
	}

	wg.Wait()

	return &cachepb.BatchGetResponse{
		Results: results,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// BatchSet 批量设置
func (s *Server) BatchSet(ctx context.Context, req *cachepb.BatchSetRequest) (*cachepb.BatchSetResponse, error) {
	if len(req.Items) == 0 {
		return &cachepb.BatchSetResponse{
			Results: []*cachepb.SetResult{},
			Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
		}, nil
	}

	// 按节点分组
	itemsByNode := s.groupItemsByNode(req.Items)

	// 并发处理
	results := make([]*cachepb.SetResult, 0, len(req.Items))
	var mu sync.Mutex
	var wg sync.WaitGroup

	for nodeAddr, items := range itemsByNode {
		wg.Add(1)
		go func(addr string, its []*cachepb.SetItem) {
			defer wg.Done()

			var nodeResults []*cachepb.SetResult
			if addr == "local" {
				nodeResults = s.batchSetLocal(ctx, its)
			} else {
				nodeResults = s.batchSetRemote(ctx, addr, its)
			}

			mu.Lock()
			results = append(results, nodeResults...)
			mu.Unlock()
		}(nodeAddr, items)
	}

	wg.Wait()

	return &cachepb.BatchSetResponse{
		Results: results,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// BatchDelete 批量删除
func (s *Server) BatchDelete(ctx context.Context, req *cachepb.BatchDeleteRequest) (*cachepb.BatchDeleteResponse, error) {
	results := make([]*cachepb.DeleteResult, 0, len(req.Keys))

	for _, key := range req.Keys {
		result := &cachepb.DeleteResult{Key: key}

		resp, err := s.Delete(ctx, &cachepb.DeleteRequest{Key: key})
		if err != nil {
			result.Success = false
			result.Error = err.Error()
		} else {
			result.Success = resp.Success
			result.Found = resp.Found
		}

		results = append(results, result)
	}

	return &cachepb.BatchDeleteResponse{
		Results: results,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// Stats 获取统计信息
func (s *Server) Stats(ctx context.Context, req *cachepb.StatsRequest) (*cachepb.StatsResponse, error) {
	if s.multiCache == nil {
		return nil, status.Error(codes.Unavailable, "cache not available")
	}

	stats, err := s.multiCache.Stats()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	// 转换统计信息
	cacheStats := &cachepb.CacheStats{
		TotalKeys:      stats.TotalRequests,
		TotalSizeBytes: 0,
		HitCount:       stats.L1Hits + stats.L2Hits,
		MissCount:      stats.Misses,
		EvictionCount:  0,
		HitRate:        stats.OverallHitRate,
		LevelStats: map[string]int64{
			"L1":                  stats.L1Hits,
			"L2":                  stats.L2Hits,
			"forwarded_requests":  atomic.LoadInt64(&s.stats.ForwardedRequests),
			"forced_local_lookup": atomic.LoadInt64(&s.stats.ForcedLocalLookups),
			"depth_violations":    atomic.LoadInt64(&s.stats.DepthViolations),
		},
		LastUpdated: timestamppb.New(stats.LastUpdate),
	}

	if stats.L1Stats != nil {
		cacheStats.TotalSizeBytes = stats.L1Stats.TotalSize
		cacheStats.EvictionCount = stats.L1Stats.EvictionCount
		cacheStats.LevelStats["L1_keys"] = stats.L1Stats.TotalKeys
	}

	if stats.L2Stats != nil {
		cacheStats.LevelStats["L2_keys"] = stats.L2Stats.TotalKeys
		cacheStats.LevelStats["L2_network_calls"] = stats.L2Stats.NetworkCalls
		cacheStats.LevelStats["L2_network_errors"] = stats.L2Stats.NetworkErrors
	}

	return &cachepb.StatsResponse{
		Stats:  cacheStats,
		Status: &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// HealthCheck 健康检查
func (s *Server) HealthCheck(ctx context.Context, req *cachepb.HealthCheckRequest) (*cachepb.HealthCheckResponse, error) {
	return &cachepb.HealthCheckResponse{
		Status:  cachepb.HealthStatus_SERVING,
		Message: "healthy",
		Metadata: map[string]string{
			"version":             "1.0.0",
			"forwarded_requests":  fmt.Sprintf("%d", atomic.LoadInt64(&s.stats.ForwardedRequests)),
			"forced_local_lookup": fmt.Sprintf("%d", atomic.LoadInt64(&s.stats.ForcedLocalLookups)),
			"depth_violations":    fmt.Sprintf("%d", atomic.LoadInt64(&s.stats.DepthViolations)),
		},
	}, nil
}

// Clear 清空缓存
func (s *Server) Clear(ctx context.Context, req *cachepb.ClearRequest) (*cachepb.ClearResponse, error) {
	if s.multiCache == nil {
		return nil, status.Error(codes.Unavailable, "cache not available")
	}

	err := s.multiCache.Clear(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &cachepb.ClearResponse{
		ClearedCount: 0,
		Status:       &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// Expire 设置过期时间
func (s *Server) Expire(ctx context.Context, req *cachepb.ExpireRequest) (*cachepb.ExpireResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	if s.isLocalKey(req.Key) {
		return s.expireLocal(ctx, req)
	}

	return s.expireRemote(ctx, req)
}

// TTL 获取剩余生存时间
func (s *Server) TTL(ctx context.Context, req *cachepb.TTLRequest) (*cachepb.TTLResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	if s.isLocalKey(req.Key) {
		return s.ttlLocal(ctx, req)
	}

	return s.ttlRemote(ctx, req)
}

// ============================================================
// 本地操作实现
// ============================================================

// getLocal 本地获取
func (s *Server) getLocal(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	entry, err := s.multiCache.Get(ctx, req.Key)
	if err != nil {
		return &cachepb.GetResponse{
			Found:  false,
			Status: &cachepb.ResponseStatus{Code: 1, Message: err.Error()},
		}, nil
	}

	metadata := s.convertToProtoMetadata(entry.Metadata)

	return &cachepb.GetResponse{
		Value:    entry.Value.ByteSlice(),
		Found:    true,
		Metadata: metadata,
		Status:   &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// setLocal 本地设置
func (s *Server) setLocal(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
	ttl := time.Hour
	if req.Ttl != nil {
		ttl = req.Ttl.AsDuration()
	}

	entry := value.NewEntry(req.Key, value.NewByteView(req.Value), ttl)
	err := s.multiCache.Set(ctx, req.Key, entry)
	if err != nil {
		return &cachepb.SetResponse{
			Success: false,
			Status:  &cachepb.ResponseStatus{Code: 1, Message: err.Error()},
		}, nil
	}

	return &cachepb.SetResponse{
		Success: true,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// deleteLocal 本地删除
func (s *Server) deleteLocal(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	err := s.multiCache.Delete(ctx, req.Key)
	found := err == nil

	return &cachepb.DeleteResponse{
		Success: found,
		Found:   found,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// expireLocal 本地设置过期时间
func (s *Server) expireLocal(ctx context.Context, req *cachepb.ExpireRequest) (*cachepb.ExpireResponse, error) {
	ttl := req.Ttl.AsDuration()

	entry, err := s.multiCache.Get(ctx, req.Key)
	if err != nil {
		return &cachepb.ExpireResponse{
			Success: false,
			Status:  &cachepb.ResponseStatus{Code: 1, Message: "key not found"},
		}, nil
	}

	newEntry := value.NewEntry(req.Key, entry.Value, ttl)
	err = s.multiCache.Set(ctx, req.Key, newEntry)
	if err != nil {
		return &cachepb.ExpireResponse{
			Success: false,
			Status:  &cachepb.ResponseStatus{Code: 1, Message: err.Error()},
		}, nil
	}

	return &cachepb.ExpireResponse{
		Success: true,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// ttlLocal 本地获取TTL
func (s *Server) ttlLocal(ctx context.Context, req *cachepb.TTLRequest) (*cachepb.TTLResponse, error) {
	entry, err := s.multiCache.Get(ctx, req.Key)
	if err != nil {
		return &cachepb.TTLResponse{
			Found:  false,
			Status: &cachepb.ResponseStatus{Code: 1, Message: err.Error()},
		}, nil
	}

	var ttl time.Duration
	if entry.Metadata != nil {
		ttl = entry.Metadata.TTL()
	}

	return &cachepb.TTLResponse{
		Ttl:    durationpb.New(ttl),
		Found:  true,
		Status: &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

// ============================================================
// 远程操作实现（仅用于非Get/Set/Delete的辅助操作）
// ============================================================

// expireRemote 远程设置过期时间
func (s *Server) expireRemote(ctx context.Context, req *cachepb.ExpireRequest) (*cachepb.ExpireResponse, error) {
	node := s.hashRing.GetNode(req.Key)
	if node == nil {
		return nil, status.Error(codes.Unavailable, "no available nodes")
	}

	var resp *cachepb.ExpireResponse
	err := s.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)
		var err error
		resp, err = client.Expire(ctx, req)
		return err
	})

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// ttlRemote 远程获取TTL
func (s *Server) ttlRemote(ctx context.Context, req *cachepb.TTLRequest) (*cachepb.TTLResponse, error) {
	node := s.hashRing.GetNode(req.Key)
	if node == nil {
		return nil, status.Error(codes.Unavailable, "no available nodes")
	}

	var resp *cachepb.TTLResponse
	err := s.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)
		var err error
		resp, err = client.TTL(ctx, req)
		return err
	})

	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return resp, nil
}

// ============================================================
// 辅助方法
// ============================================================

// isLocalKey 判断键是否应该存储在本地
func (s *Server) isLocalKey(key string) bool {
	node := s.hashRing.GetNode(key)
	if node == nil {
		return true // 如果没有其他节点，存储在本地
	}

	// 比较节点ID（更准确的方式）
	return node.ID == s.config.Cluster.NodeID
}

// getLocalAddress 获取本地地址
func (s *Server) getLocalAddress() string {
	return fmt.Sprintf("127.0.0.1:%d", s.config.Transport.GRPC.Port)
}

// groupKeysByNode 按节点分组键
func (s *Server) groupKeysByNode(keys []string) map[string][]string {
	groups := make(map[string][]string)

	for _, key := range keys {
		var addr string
		if s.isLocalKey(key) {
			addr = "local"
		} else {
			node := s.hashRing.GetNode(key)
			if node != nil {
				addr = node.Address
			} else {
				addr = "local"
			}
		}

		groups[addr] = append(groups[addr], key)
	}

	return groups
}

// groupItemsByNode 按节点分组设置项
func (s *Server) groupItemsByNode(items []*cachepb.SetItem) map[string][]*cachepb.SetItem {
	groups := make(map[string][]*cachepb.SetItem)

	for _, item := range items {
		var addr string
		if s.isLocalKey(item.Key) {
			addr = "local"
		} else {
			node := s.hashRing.GetNode(item.Key)
			if node != nil {
				addr = node.Address
			} else {
				addr = "local"
			}
		}

		groups[addr] = append(groups[addr], item)
	}

	return groups
}

// batchGetLocal 本地批量获取
func (s *Server) batchGetLocal(ctx context.Context, keys []string) []*cachepb.GetResult {
	results := make([]*cachepb.GetResult, 0, len(keys))

	for _, key := range keys {
		result := &cachepb.GetResult{Key: key}

		resp, err := s.getLocal(ctx, &cachepb.GetRequest{Key: key})
		if err != nil {
			result.Found = false
		} else {
			result.Value = resp.Value
			result.Found = resp.Found
			result.Metadata = resp.Metadata
		}

		results = append(results, result)
	}

	return results
}

// batchGetRemote 远程批量获取
func (s *Server) batchGetRemote(ctx context.Context, addr string, keys []string) []*cachepb.GetResult {
	results := make([]*cachepb.GetResult, 0, len(keys))

	err := s.connPool.WithConnection(addr, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		for _, key := range keys {
			result := &cachepb.GetResult{Key: key}

			resp, err := client.Get(ctx, &cachepb.GetRequest{Key: key})
			if err != nil {
				result.Found = false
			} else {
				result.Value = resp.Value
				result.Found = resp.Found
				result.Metadata = resp.Metadata
			}

			results = append(results, result)
		}

		return nil
	})

	if err != nil {
		// 如果连接失败，返回全部失败的结果
		for _, key := range keys {
			results = append(results, &cachepb.GetResult{
				Key:   key,
				Found: false,
			})
		}
	}

	return results
}

// batchSetLocal 本地批量设置
func (s *Server) batchSetLocal(ctx context.Context, items []*cachepb.SetItem) []*cachepb.SetResult {
	results := make([]*cachepb.SetResult, 0, len(items))

	for _, item := range items {
		result := &cachepb.SetResult{Key: item.Key}

		req := &cachepb.SetRequest{
			Key:   item.Key,
			Value: item.Value,
			Ttl:   item.Ttl,
		}

		resp, err := s.setLocal(ctx, req)
		if err != nil {
			result.Success = false
			result.Error = err.Error()
		} else {
			result.Success = resp.Success
		}

		results = append(results, result)
	}

	return results
}

// batchSetRemote 远程批量设置
func (s *Server) batchSetRemote(ctx context.Context, addr string, items []*cachepb.SetItem) []*cachepb.SetResult {
	results := make([]*cachepb.SetResult, 0, len(items))

	err := s.connPool.WithConnection(addr, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		for _, item := range items {
			result := &cachepb.SetResult{Key: item.Key}

			req := &cachepb.SetRequest{
				Key:   item.Key,
				Value: item.Value,
				Ttl:   item.Ttl,
			}

			resp, err := client.Set(ctx, req)
			if err != nil {
				result.Success = false
				result.Error = err.Error()
			} else {
				result.Success = resp.Success
			}

			results = append(results, result)
		}

		return nil
	})

	if err != nil {
		// 如果连接失败，返回全部失败的结果
		for _, item := range items {
			results = append(results, &cachepb.SetResult{
				Key:     item.Key,
				Success: false,
				Error:   err.Error(),
			})
		}
	}

	return results
}

// convertToProtoMetadata 转换元数据到Proto格式
func (s *Server) convertToProtoMetadata(metadata *value.Metadata) *cachepb.CacheMetadata {
	if metadata == nil {
		return nil
	}

	return &cachepb.CacheMetadata{
		CreatedAt:   timestamppb.New(metadata.CreatedAt()),
		AccessedAt:  timestamppb.New(metadata.AccessedAt()),
		ExpiresAt:   timestamppb.New(metadata.ExpiresAt()),
		AccessCount: metadata.AccessCount(),
		SizeBytes:   metadata.TotalSize(),
		Level:       cachepb.CacheLevel_L1_LOCAL,
	}
}

// generateRequestID 生成请求ID
func (s *Server) generateRequestID() string {
	return fmt.Sprintf("%s-%d", s.config.Cluster.NodeID, time.Now().UnixNano())
}

// ============================================================
// 拦截器
// ============================================================

// unaryInterceptor 一元拦截器
func (s *Server) unaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	start := time.Now()

	// 记录请求日志
	s.logger.Debugf("Handling request: %s", info.FullMethod)

	// 执行请求
	resp, err := handler(ctx, req)

	// 记录响应日志
	duration := time.Since(start)
	if err != nil {
		s.logger.Warnf("Request %s failed in %v: %v", info.FullMethod, duration, err)
	} else {
		s.logger.Debugf("Request %s completed in %v", info.FullMethod, duration)
	}

	return resp, err
}

// streamInterceptor 流拦截器
func (s *Server) streamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()

	// 记录流开始
	s.logger.Debugf("Handling stream: %s", info.FullMethod)

	// 执行流处理
	err := handler(srv, ss)

	// 记录流结束
	duration := time.Since(start)
	if err != nil {
		s.logger.Warnf("Stream %s failed in %v: %v", info.FullMethod, duration, err)
	} else {
		s.logger.Debugf("Stream %s completed in %v", info.FullMethod, duration)
	}

	return err
}

// ============================================================
// 🔧 新增：获取服务器统计信息
// ============================================================

// GetStats 获取服务器统计信息（包含防链式查询指标）
func (s *Server) GetStats() *ServerStats {
	return &ServerStats{
		ForwardedRequests:  atomic.LoadInt64(&s.stats.ForwardedRequests),
		ForcedLocalLookups: atomic.LoadInt64(&s.stats.ForcedLocalLookups),
		DepthViolations:    atomic.LoadInt64(&s.stats.DepthViolations),
	}
}
