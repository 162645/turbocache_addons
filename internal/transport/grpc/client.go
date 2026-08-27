package grpc

import (
	"context"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"

	"turbocache/api/generated/cachepb"
	"turbocache/internal/transport/pool"
	"turbocache/internal/utils/logger"
)

// Client gRPC客户端
type Client struct {
	mu     sync.RWMutex
	pool   *pool.ConnectionPool
	config *ClientConfig
	logger logger.Logger
	closed bool
}

// ClientConfig 客户端配置
type ClientConfig struct {
	MaxRecvMsgSize    int           `yaml:"max_recv_msg_size"`
	MaxSendMsgSize    int           `yaml:"max_send_msg_size"`
	ConnectionTimeout time.Duration `yaml:"connection_timeout"`
	KeepaliveTime     time.Duration `yaml:"keepalive_time"`
	KeepaliveTimeout  time.Duration `yaml:"keepalive_timeout"`
	MaxConnectionIdle time.Duration `yaml:"max_connection_idle"`

	// 重试配置
	MaxRetries    int           `yaml:"max_retries"`
	RetryInterval time.Duration `yaml:"retry_interval"`
	RetryBackoff  float64       `yaml:"retry_backoff"`

	// TLS配置
	TLS TLSConfig `yaml:"tls"`
}

// TLSConfig TLS 配置
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
	CAFile   string `yaml:"ca_file"`
}

// DefaultClientConfig 默认客户端配置
func DefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		MaxRecvMsgSize:    4 * 1024 * 1024, // 4MB
		MaxSendMsgSize:    4 * 1024 * 1024, // 4MB
		ConnectionTimeout: 5 * time.Second,
		KeepaliveTime:     30 * time.Second,
		KeepaliveTimeout:  5 * time.Second,
		MaxConnectionIdle: 5 * time.Minute,
		MaxRetries:        3,
		RetryInterval:     100 * time.Millisecond,
		RetryBackoff:      2.0,
		TLS: TLSConfig{
			Enabled: false,
		},
	}
}

// NewClient 创建gRPC客户端
func NewClient(config *ClientConfig) *Client {
	if config == nil {
		config = DefaultClientConfig()
	}

	poolConfig := &pool.Config{
		MaxSize:             100,
		MinSize:             10,
		MaxIdleTime:         config.MaxConnectionIdle,
		DialTimeout:         config.ConnectionTimeout,
		HealthCheckInterval: 30 * time.Second,
	}

	return &Client{
		pool:   pool.NewConnectionPool(poolConfig),
		config: config,
		logger: logger.WithField("component", "grpc_client"),
	}
}

// Connect 连接到指定地址
func (c *Client) Connect(address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return fmt.Errorf("client is closed")
	}

	opts := c.buildDialOptions()

	conn, err := grpc.Dial(address, opts...)
	if err != nil {
		c.logger.Errorf("Failed to connect to %s: %v", address, err)
		return fmt.Errorf("failed to connect: %w", err)
	}

	if err := c.pool.Add(address, conn); err != nil {
		conn.Close()
		return fmt.Errorf("failed to add connection to pool: %w", err)
	}

	c.logger.Infof("Connected to %s", address)
	return nil
}

// buildDialOptions 构建拨号选项
func (c *Client) buildDialOptions() []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.config.MaxRecvMsgSize),
			grpc.MaxCallSendMsgSize(c.config.MaxSendMsgSize),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    c.config.KeepaliveTime,
			Timeout: c.config.KeepaliveTimeout,
		}),
	}

	// TLS配置
	if c.config.TLS.Enabled {
		// TODO: 实现TLS配置
		c.logger.Warn("TLS not implemented yet, using insecure connection")
	}

	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	return opts
}

// Get 获取缓存值
func (c *Client) Get(ctx context.Context, address, key, group string) (*cachepb.GetResponse, error) {
	req := &cachepb.GetRequest{
		Key:   key,
		Group: group,
	}

	var resp *cachepb.GetResponse
	err := c.executeWithRetry(ctx, address, func(client cachepb.CacheServiceClient) error {
		var err error
		resp, err = client.Get(ctx, req)
		return err
	})

	return resp, err
}

// Set 设置缓存值
func (c *Client) Set(ctx context.Context, address, key, group string, value []byte, ttl time.Duration) (*cachepb.SetResponse, error) {
	req := &cachepb.SetRequest{
		Key:   key,
		Value: value,
		Group: group,
	}

	if ttl > 0 {
		req.Ttl = durationpb.New(ttl)
	}

	var resp *cachepb.SetResponse
	err := c.executeWithRetry(ctx, address, func(client cachepb.CacheServiceClient) error {
		var err error
		resp, err = client.Set(ctx, req)
		return err
	})

	return resp, err
}

// Delete 删除缓存值
func (c *Client) Delete(ctx context.Context, address, key, group string) (*cachepb.DeleteResponse, error) {
	req := &cachepb.DeleteRequest{
		Key:   key,
		Group: group,
	}

	var resp *cachepb.DeleteResponse
	err := c.executeWithRetry(ctx, address, func(client cachepb.CacheServiceClient) error {
		var err error
		resp, err = client.Delete(ctx, req)
		return err
	})

	return resp, err
}

// BatchGet 批量获取
func (c *Client) BatchGet(ctx context.Context, address string, keys []string, group string) (*cachepb.BatchGetResponse, error) {
	req := &cachepb.BatchGetRequest{
		Keys:  keys,
		Group: group,
	}

	var resp *cachepb.BatchGetResponse
	err := c.executeWithRetry(ctx, address, func(client cachepb.CacheServiceClient) error {
		var err error
		resp, err = client.BatchGet(ctx, req)
		return err
	})

	return resp, err
}

// Stats 获取统计信息
func (c *Client) Stats(ctx context.Context, address, group string) (*cachepb.StatsResponse, error) {
	req := &cachepb.StatsRequest{
		Group: group,
	}

	var resp *cachepb.StatsResponse
	err := c.executeWithRetry(ctx, address, func(client cachepb.CacheServiceClient) error {
		var err error
		resp, err = client.Stats(ctx, req)
		return err
	})

	return resp, err
}

// HealthCheck 健康检查
func (c *Client) HealthCheck(ctx context.Context, address string) (*cachepb.HealthCheckResponse, error) {
	req := &cachepb.HealthCheckRequest{
		Service: "cache",
	}

	var resp *cachepb.HealthCheckResponse
	err := c.executeWithRetry(ctx, address, func(client cachepb.CacheServiceClient) error {
		var err error
		resp, err = client.HealthCheck(ctx, req)
		return err
	})

	return resp, err
}

// executeWithRetry 执行带重试的操作（不使用泛型）
func (c *Client) executeWithRetry(ctx context.Context, address string, operation func(cachepb.CacheServiceClient) error) error {
	if c.closed {
		return fmt.Errorf("client is closed")
	}

	var lastErr error
	backoff := c.config.RetryInterval

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				backoff = time.Duration(float64(backoff) * c.config.RetryBackoff)
			}
		}

		err := c.executeOnce(ctx, address, operation)
		if err == nil {
			return nil
		}

		lastErr = err

		// 检查是否应该重试
		if !c.shouldRetry(err) {
			break
		}

		c.logger.Warnf("Attempt %d failed for %s: %v", attempt+1, address, err)
	}

	return fmt.Errorf("operation failed after %d attempts: %w", c.config.MaxRetries+1, lastErr)
}

// executeOnce 执行一次操作
func (c *Client) executeOnce(ctx context.Context, address string, operation func(cachepb.CacheServiceClient) error) error {
	conn, err := c.pool.Get(address)
	if err != nil {
		return fmt.Errorf("failed to get connection: %w", err)
	}
	defer c.pool.Put(address, conn)

	client := cachepb.NewCacheServiceClient(conn)
	return operation(client)
}

// shouldRetry 判断是否应该重试
func (c *Client) shouldRetry(err error) bool {
	if err == nil {
		return false
	}

	// gRPC 状态码检查
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return true
		case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied:
			return false
		}
	}

	return true
}

// Close 关闭客户端
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	if err := c.pool.Close(); err != nil {
		c.logger.Errorf("Failed to close connection pool: %v", err)
		return err
	}

	c.logger.Info("gRPC client closed")
	return nil
}

// GetPoolStats 获取连接池状态
func (c *Client) GetPoolStats() map[string]pool.Stats {
	return c.pool.GetStats()
}

// RemoveConnection 移除连接
func (c *Client) RemoveConnection(address string) {
	c.pool.Remove(address)
	c.logger.Infof("Removed connection to %s", address)
}
