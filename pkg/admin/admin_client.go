package admin

import (
	"context"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"turbocache/api/generated/cachepb"
	"turbocache/api/generated/discoverypb"
	"turbocache/api/generated/metricspb"
)

// AdminClient 管理客户端
type AdminClient struct {
	// gRPC连接
	conn *grpc.ClientConn

	// 服务客户端（gRPC 线程安全）
	cacheClient     cachepb.CacheServiceClient
	discoveryClient discoverypb.DiscoveryServiceClient
	metricsClient   metricspb.MetricsServiceClient

	// 配置
	config *Config
}

// Config 管理客户端配置
type Config struct {
	// 服务器地址
	Address string
	// 连接超时
	DialTimeout time.Duration
	// 请求超时（用于在调用方未提供 deadline 时包裹）
	RequestTimeout time.Duration
	// TLS配置（如果需要 TLS，请提供 CAFile 或更复杂的 tls.Config 支持）
	TLS *TLSConfig
}

// TLSConfig TLS配置（简化：支持 CAFile；如果你需要 mTLS 请扩展）
type TLSConfig struct {
	Enabled  bool
	CertFile string // 客户端证书（未在当前实现中使用，保留供扩展）
	KeyFile  string // 客户端私钥（未在当前实现中使用，保留供扩展）
	CAFile   string // 用于验证服务端证书；若空则使用 insecure
}

// NewAdminClient 创建管理客户端
func NewAdminClient(cfg *Config) (*AdminClient, error) {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	// dial options 默认
	var opts []grpc.DialOption

	// 如果启用 TLS 且提供 CAFile，则使用 TLS cred；否则使用 insecure（可根据需求强制 TLS）
	if cfg.TLS != nil && cfg.TLS.Enabled && cfg.TLS.CAFile != "" {
		creds, err := credentials.NewClientTLSFromFile(cfg.TLS.CAFile, "")
		if err != nil {
			return nil, fmt.Errorf("failed to load TLS CA file: %w", err)
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	// 这里默认使用 WithBlock()，即 DialContext 会等待连接建立或超时（可按需改成异步）
	opts = append(opts, grpc.WithBlock())

	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, cfg.Address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server %s: %w", cfg.Address, err)
	}

	c := &AdminClient{
		conn:            conn,
		cacheClient:     cachepb.NewCacheServiceClient(conn),
		discoveryClient: discoverypb.NewDiscoveryServiceClient(conn),
		metricsClient:   metricspb.NewMetricsServiceClient(conn),
		config:          cfg,
	}
	return c, nil
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Address:        "localhost:9090",
		DialTimeout:    5 * time.Second,
		RequestTimeout: 10 * time.Second,
	}
}

// Close 关闭客户端
func (c *AdminClient) Close() error {
	if c == nil {
		return nil
	}
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// --- helper: 如果 ctx 没有 deadline，则用 requestTimeout 包裹；始终返回 cancel 可安全调用 ---
func (c *AdminClient) withRequestContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || c == nil || c.config == nil || c.config.RequestTimeout <= 0 {
		// ctx 已有 deadline 或者没有配置，就返回原 ctx，并且返回 no-op cancel（便于统一 defer cancel())
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, c.config.RequestTimeout)
}

// ============ 缓存管理 ============

// GetCacheStats 获取缓存统计信息
func (c *AdminClient) GetCacheStats(ctx context.Context, group string) (*cachepb.CacheStats, error) {
	if c == nil || c.cacheClient == nil {
		return nil, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.cacheClient.Stats(ctx2, &cachepb.StatsRequest{Group: group})
	if err != nil {
		return nil, fmt.Errorf("failed to get cache stats: %w", err)
	}
	return resp.Stats, nil
}

// ClearCache 清空缓存
func (c *AdminClient) ClearCache(ctx context.Context, group string) (int64, error) {
	if c == nil || c.cacheClient == nil {
		return 0, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.cacheClient.Clear(ctx2, &cachepb.ClearRequest{Group: group})
	if err != nil {
		return 0, fmt.Errorf("failed to clear cache: %w", err)
	}
	return resp.ClearedCount, nil
}

// GetKey 获取键值
func (c *AdminClient) GetKey(ctx context.Context, group, key string) ([]byte, bool, error) {
	if c == nil || c.cacheClient == nil {
		return nil, false, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.cacheClient.Get(ctx2, &cachepb.GetRequest{Group: group, Key: key})
	if err != nil {
		return nil, false, fmt.Errorf("failed to get key: %w", err)
	}
	return resp.Value, resp.Found, nil
}

// DeleteKey 删除键
func (c *AdminClient) DeleteKey(ctx context.Context, group, key string) (bool, error) {
	if c == nil || c.cacheClient == nil {
		return false, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.cacheClient.Delete(ctx2, &cachepb.DeleteRequest{Group: group, Key: key})
	if err != nil {
		return false, fmt.Errorf("failed to delete key: %w", err)
	}
	return resp.Found, nil
}

// GetTTL 获取键的TTL
func (c *AdminClient) GetTTL(ctx context.Context, group, key string) (time.Duration, bool, error) {
	if c == nil || c.cacheClient == nil {
		return 0, false, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.cacheClient.TTL(ctx2, &cachepb.TTLRequest{Group: group, Key: key})
	if err != nil {
		return 0, false, fmt.Errorf("failed to get TTL: %w", err)
	}
	return resp.Ttl.AsDuration(), resp.Found, nil
}

// ============ 节点管理 ============

// ListNodes 列出所有节点
func (c *AdminClient) ListNodes(ctx context.Context, region, zone string) ([]*discoverypb.NodeInfo, error) {
	if c == nil || c.discoveryClient == nil {
		return nil, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.discoveryClient.ListNodes(ctx2, &discoverypb.ListNodesRequest{Region: region, Zone: zone})
	if err != nil {
		return nil, fmt.Errorf("failed to list nodes: %w", err)
	}
	return resp.Nodes, nil
}

// GetNode 获取节点信息
func (c *AdminClient) GetNode(ctx context.Context, nodeID string) (*discoverypb.NodeInfo, error) {
	if c == nil || c.discoveryClient == nil {
		return nil, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.discoveryClient.GetNode(ctx2, &discoverypb.GetNodeRequest{NodeId: nodeID})
	if err != nil {
		return nil, fmt.Errorf("failed to get node: %w", err)
	}
	if !resp.Found {
		// 保持语义明确（可改为返回 grpc/status 的 codes.NotFound）
		return nil, fmt.Errorf("node not found")
	}
	return resp.Node, nil
}

// HealthCheckNode 检查节点健康状态
func (c *AdminClient) HealthCheckNode(ctx context.Context, nodeID string) (*discoverypb.NodeHealthCheckResponse, error) {
	if c == nil || c.discoveryClient == nil {
		return nil, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.discoveryClient.HealthCheck(ctx2, &discoverypb.NodeHealthCheckRequest{NodeId: nodeID})
	if err != nil {
		return nil, fmt.Errorf("failed to health check node: %w", err)
	}
	return resp, nil
}

// ============ 指标管理 ============

// GetMetrics 获取指标快照
func (c *AdminClient) GetMetrics(ctx context.Context, nodeID string, metricNames []string) (*metricspb.MetricsSnapshot, error) {
	if c == nil || c.metricsClient == nil {
		return nil, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.metricsClient.GetMetrics(ctx2, &metricspb.GetMetricsRequest{NodeId: nodeID, MetricNames: metricNames})
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics: %w", err)
	}
	return resp.Snapshot, nil
}

// GetHistoricalMetrics 获取历史指标
func (c *AdminClient) GetHistoricalMetrics(ctx context.Context, nodeID string, metricNames []string, startTime, endTime time.Time) ([]*metricspb.MetricsSnapshot, error) {
	if c == nil || c.metricsClient == nil {
		return nil, fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	resp, err := c.metricsClient.GetHistoricalMetrics(ctx2, &metricspb.GetHistoricalMetricsRequest{
		NodeId:      nodeID,
		MetricNames: metricNames,
		StartTime:   timestamppb.New(startTime),
		EndTime:     timestamppb.New(endTime),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get historical metrics: %w", err)
	}
	return resp.Snapshots, nil
}

// StreamMetrics 流式获取指标（handler 在每个快照上执行，返回 error 将终止流）
func (c *AdminClient) StreamMetrics(ctx context.Context, nodeID string, interval time.Duration, handler func(*metricspb.MetricsSnapshot) error) error {
	if c == nil || c.metricsClient == nil {
		return fmt.Errorf("admin client not initialized")
	}
	// 不对外覆盖调用者 ctx 的 deadline；仅作为流的父 ctx
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	stream, err := c.metricsClient.StreamMetrics(ctx2, &metricspb.StreamMetricsRequest{
		NodeId:   nodeID,
		Interval: durationpb.New(interval),
	})
	if err != nil {
		return fmt.Errorf("failed to create metrics stream: %w", err)
	}

	for {
		// 如果上层取消，及早退出
		if ctx2.Err() != nil {
			return ctx2.Err()
		}

		snapshot, err := stream.Recv()
		if err != nil {
			// 正常的流结束
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to receive metrics: %w", err)
		}
		if snapshot == nil {
			continue
		}
		if err := handler(snapshot); err != nil {
			return err
		}
	}
}

// ============ 集群管理 ============

func (c *AdminClient) GetClusterInfo(ctx context.Context) (*ClusterInfo, error) {
	nodes, err := c.ListNodes(ctx, "", "")
	if err != nil {
		return nil, err
	}

	info := &ClusterInfo{
		TotalNodes:     len(nodes),
		HealthyNodes:   0,
		UnhealthyNodes: 0,
		Nodes:          make([]*NodeSummary, 0, len(nodes)),
	}

	for _, node := range nodes {
		summary := &NodeSummary{
			NodeID:  node.NodeId,
			Address: node.Address,
			Status:  node.Status.String(),
			Region:  node.Region,
			Zone:    node.Zone,
			Version: node.Version,
		}
		if node.Status == discoverypb.NodeStatus_HEALTHY {
			info.HealthyNodes++
		} else {
			info.UnhealthyNodes++
		}
		info.Nodes = append(info.Nodes, summary)
	}

	return info, nil
}

// ClusterInfo / NodeSummary 定义（同你原来的一样）
type ClusterInfo struct {
	TotalNodes     int            `json:"total_nodes"`
	HealthyNodes   int            `json:"healthy_nodes"`
	UnhealthyNodes int            `json:"unhealthy_nodes"`
	Nodes          []*NodeSummary `json:"nodes"`
}

type NodeSummary struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"`
	Status  string `json:"status"`
	Region  string `json:"region"`
	Zone    string `json:"zone"`
	Version string `json:"version"`
}

// ============ 便捷方法 ============

// Ping 测试连接（HealthCheck RPC）
func (c *AdminClient) Ping(ctx context.Context) error {
	if c == nil || c.cacheClient == nil {
		return fmt.Errorf("admin client not initialized")
	}
	ctx2, cancel := c.withRequestContext(ctx)
	defer cancel()

	_, err := c.cacheClient.HealthCheck(ctx2, &cachepb.HealthCheckRequest{})
	return err
}

// GetVersion 获取服务器版本（通过 ListNodes 取第一台机器的 version）
func (c *AdminClient) GetVersion(ctx context.Context) (string, error) {
	nodes, err := c.ListNodes(ctx, "", "")
	if err != nil {
		return "", err
	}
	if len(nodes) == 0 {
		return "", fmt.Errorf("no nodes found")
	}
	return nodes[0].Version, nil
}
