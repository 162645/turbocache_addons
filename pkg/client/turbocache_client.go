package client // TurboCache 客户端库包，封装对服务端的 RPC 调用与重试/负载均衡逻辑

import (
	"context" // 用于上下文传递（超时、取消）
	"fmt"     // 格式化错误/输出
	"sync"    // 并发保护（Mutex / RWMutex）
	"sync/atomic"
	"time" // 时间相关（超时、重试间隔等）

	"google.golang.org/grpc"                             // gRPC 主库
	"google.golang.org/grpc/credentials/insecure"        // 不使用 TLS 的凭据（适合内部网络/示例）
	"google.golang.org/grpc/keepalive"                   // keepalive 参数
	"google.golang.org/protobuf/types/known/durationpb"  // protobuf duration 类型包装
	"google.golang.org/protobuf/types/known/timestamppb" // protobuf timestamp 类型包装

	"turbocache/api/generated/cachepb" // 生成的 protobuf/gRPC 客户端代码（CacheService）
)

// Client TurboCache客户端
type Client struct {
	servers     []string                     // 服务地址列表（可配置多地址用于容错）
	connections []*grpc.ClientConn           // 对应的 gRPC 连接池（简单数组实现）
	clients     []cachepb.CacheServiceClient // 对应的 RPC 客户端接口
	mu          sync.RWMutex                 // 保护 connections/clients 等并发访问

	// 配置
	config *Config // 客户端配置对象

	// 负载均衡
	roundRobin int64 // 🔧 修复：改为 int64 用于原子操作
	// UnixNano deadlines. A transport-failing endpoint is temporarily skipped so
	// every third request does not repeatedly pay the same timeout.
	unavailableUntil []int64

	// 生命周期
	closed bool // 客户端是否已关闭（简单标志）
}

// Config 客户端配置
type Config struct {
	Servers           []string      `yaml:"servers" json:"servers"`                       // 可选：服务地址列表
	ConnectionTimeout time.Duration `yaml:"connection_timeout" json:"connection_timeout"` // 拨号超时时间
	RequestTimeout    time.Duration `yaml:"request_timeout" json:"request_timeout"`       // 请求超时时间（未在此文件强制使用）
	MaxRetries        int           `yaml:"max_retries" json:"max_retries"`               // 操作重试次数
	RetryDelay        time.Duration `yaml:"retry_delay" json:"retry_delay"`               // 重试间隔
	KeepaliveTime     time.Duration `yaml:"keepalive_time" json:"keepalive_time"`         // keepalive 心跳间隔
	KeepaliveTimeout  time.Duration `yaml:"keepalive_timeout" json:"keepalive_timeout"`   // keepalive 超时
	FailureCooldown   time.Duration `yaml:"failure_cooldown" json:"failure_cooldown"`

	// 客户端标识
	ClientID string `yaml:"client_id" json:"client_id"` // 客户端标识，用于请求元数据
}

// DefaultConfig 默认配置（在 NewClient 时若无配置则使用）
var DefaultConfig = Config{
	ConnectionTimeout: 5 * time.Second,        // 默认拨号 5s
	RequestTimeout:    30 * time.Second,       // 默认请求 30s（可在上层 context 控制）
	MaxRetries:        3,                      // 默认重试 3 次
	RetryDelay:        100 * time.Millisecond, // 重试间隔 100ms
	KeepaliveTime:     30 * time.Second,       // gRPC keepalive 默认 30s
	KeepaliveTimeout:  5 * time.Second,        // keepalive 响应超时 5s
	FailureCooldown:   2 * time.Second,
	ClientID:          "turbocache-go-client", // 默认客户端 ID
}

// NewClient 创建客户端
func NewClient(servers []string, config *Config) (*Client, error) {
	if len(servers) == 0 { // 至少要有一个 server
		return nil, fmt.Errorf("at least one server address is required")
	}

	// 🔧 修复：深拷贝 DefaultConfig 避免修改全局变量
	if config == nil {
		configCopy := DefaultConfig // 值拷贝
		config = &configCopy
	}
	config.Servers = servers // 将 servers 写回配置，便于后续读取

	client := &Client{
		servers: servers,
		config:  config,
	}

	// 建立连接（会创建到每个 server 的 gRPC 连接并构建 client 接口）
	if err := client.connect(); err != nil {
		return nil, err
	}

	return client, nil
}

// connect 建立到所有服务器的连接
func (c *Client) connect() error {
	c.connections = make([]*grpc.ClientConn, len(c.servers))       // 为连接切片分配空间
	c.clients = make([]cachepb.CacheServiceClient, len(c.servers)) // 为 client 接口分配空间
	c.unavailableUntil = make([]int64, len(c.servers))

	for i, server := range c.servers {
		conn, err := c.createConnection(server) // 与 server 建立 gRPC 连接
		if err != nil {
			// 若中途失败，清理已建立的连接以避免泄露
			for j := 0; j < i; j++ {
				if c.connections[j] != nil {
					c.connections[j].Close()
				}
			}
			return fmt.Errorf("failed to connect to server %s: %v", server, err)
		}

		c.connections[i] = conn                            // 保存连接
		c.clients[i] = cachepb.NewCacheServiceClient(conn) // 使用连接创建 protobuf client
	}

	return nil
}

// createConnection 创建gRPC连接
func (c *Client) createConnection(server string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.ConnectionTimeout) // 使用配置的拨号超时
	defer cancel()

	// 配置 gRPC 选项：不使用 TLS、keepalive、阻塞拨号（直到建立或超时）
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()), // 内网示例用 insecure，生产建议使用 TLS
		grpc.WithKeepaliveParams(keepalive.ClientParameters{ // keepalive 参数用于探测连接健康
			Time:                c.config.KeepaliveTime,
			Timeout:             c.config.KeepaliveTimeout,
			PermitWithoutStream: true,
		}),
		grpc.WithBlock(), // 阻塞直到 Dial 成功或超时（配合上面的 context）
	}

	return grpc.DialContext(ctx, server, opts...) // 返回建立的连接或错误
}

// Get 获取缓存值
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	return c.GetWithGroup(ctx, "", key) // 简单代理到带 group 的方法，默认空 group
}

// GetWithGroup 从指定组获取缓存值
func (c *Client) GetWithGroup(ctx context.Context, group, key string) ([]byte, error) {
	if c.closed { // 客户端已关闭，不应再发起请求
		return nil, ErrClientClosed
	}

	// 构造 protobuf 请求体，包含元数据（client id、timestamp 等）
	req := &cachepb.GetRequest{
		Key:      key,
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.GetResponse
	var err error

	// 使用带重试的执行函数去实际调用 RPC（executeWithRetry 会选择节点并重试）
	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.Get(ctx, req) // 把 rpc 调用写在闭包中
		return err
	})

	if err != nil {
		return nil, err // 上层判断错误类型（网络/超时/未找到等）
	}

	if !resp.Found { // RPC 返回但服务端表明未找到
		return nil, ErrKeyNotFound
	}

	return resp.Value, nil // 返回数据字节切片
}

// Set 设置缓存值
func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	return c.SetWithGroup(ctx, "", key, value, ttl) // 代理到带 group 的实现
}

// SetWithGroup 设置缓存值到指定组
func (c *Client) SetWithGroup(ctx context.Context, group, key string, value []byte, ttl time.Duration) error {
	if c.closed {
		return ErrClientClosed
	}

	// 构造 SetRequest，TTL 使用 protobuf 的 durationpb 包装
	req := &cachepb.SetRequest{
		Key:      key,
		Value:    value,
		Group:    group,
		Ttl:      durationpb.New(ttl),
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.SetResponse
	var err error

	// 使用带重试的执行
	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.Set(ctx, req)
		return err
	})

	if err != nil {
		return err
	}

	// 服务端返回结果里有 Success 字段用于判断操作是否真正成功
	if !resp.Success {
		return fmt.Errorf("set operation failed: %s", resp.Status.Message)
	}

	return nil
}

// Delete 删除缓存值
func (c *Client) Delete(ctx context.Context, key string) error {
	return c.DeleteWithGroup(ctx, "", key)
}

// DeleteWithGroup 从指定组删除缓存值
func (c *Client) DeleteWithGroup(ctx context.Context, group, key string) error {
	if c.closed {
		return ErrClientClosed
	}

	req := &cachepb.DeleteRequest{
		Key:      key,
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.DeleteResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.Delete(ctx, req)
		return err
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("delete operation failed: %s", resp.Status.Message)
	}

	return nil
}

// BatchGet 批量获取
func (c *Client) BatchGet(ctx context.Context, keys []string) (map[string][]byte, error) {
	return c.BatchGetWithGroup(ctx, "", keys)
}

// BatchGetWithGroup 从指定组批量获取
func (c *Client) BatchGetWithGroup(ctx context.Context, group string, keys []string) (map[string][]byte, error) {
	if c.closed {
		return nil, ErrClientClosed
	}

	if len(keys) == 0 { // 空请求直接返回空 map，避免 RPC
		return make(map[string][]byte), nil
	}

	req := &cachepb.BatchGetRequest{
		Keys:     keys,
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.BatchGetResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.BatchGet(ctx, req)
		return err
	})

	if err != nil {
		return nil, err
	}

	// 将返回结果转换为 map（只包含 Found 的项）
	result := make(map[string][]byte)
	for _, item := range resp.Results {
		if item.Found {
			result[item.Key] = item.Value
		}
	}

	return result, nil
}

// BatchSet 批量设置
func (c *Client) BatchSet(ctx context.Context, items map[string][]byte, ttl time.Duration) error {
	return c.BatchSetWithGroup(ctx, "", items, ttl)
}

// BatchSetWithGroup 批量设置到指定组
func (c *Client) BatchSetWithGroup(ctx context.Context, group string, items map[string][]byte, ttl time.Duration) error {
	if c.closed {
		return ErrClientClosed
	}

	if len(items) == 0 { // 空操作直接返回
		return nil
	}

	// 把 map 转换为 protobuf 的 SetItem 列表
	setItems := make([]*cachepb.SetItem, 0, len(items))
	for key, value := range items {
		setItems = append(setItems, &cachepb.SetItem{
			Key:   key,
			Value: value,
			Ttl:   durationpb.New(ttl),
		})
	}

	req := &cachepb.BatchSetRequest{
		Items:    setItems,
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.BatchSetResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.BatchSet(ctx, req)
		return err
	})

	if err != nil {
		return err
	}

	// 检查返回结果中是否有单条设置失败
	for _, result := range resp.Results {
		if !result.Success {
			return fmt.Errorf("failed to set key %s: %s", result.Key, result.Error)
		}
	}

	return nil
}

// BatchDelete 批量删除
func (c *Client) BatchDelete(ctx context.Context, keys []string) error {
	return c.BatchDeleteWithGroup(ctx, "", keys)
}

// BatchDeleteWithGroup 从指定组批量删除
func (c *Client) BatchDeleteWithGroup(ctx context.Context, group string, keys []string) error {
	if c.closed {
		return ErrClientClosed
	}

	if len(keys) == 0 {
		return nil
	}

	req := &cachepb.BatchDeleteRequest{
		Keys:     keys,
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.BatchDeleteResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.BatchDelete(ctx, req)
		return err
	})

	if err != nil {
		return err
	}

	// 检查批量删除的返回结果，若某项失败且包含错误信息则返回错误
	for _, result := range resp.Results {
		if !result.Success && result.Error != "" {
			return fmt.Errorf("failed to delete key %s: %s", result.Key, result.Error)
		}
	}

	return nil
}

// Exists 检查键是否存在
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	return c.ExistsWithGroup(ctx, "", key)
}

// ExistsWithGroup 检查指定组中的键是否存在
func (c *Client) ExistsWithGroup(ctx context.Context, group, key string) (bool, error) {
	_, err := c.GetWithGroup(ctx, group, key) // 复用 Get 逻辑判断存在性
	if err != nil {
		if err == ErrKeyNotFound {
			return false, nil // 明确的未找到，返回 false
		}
		return false, err // 其他错误上抛
	}
	return true, nil
}

// TTL 获取键的剩余生存时间
func (c *Client) TTL(ctx context.Context, key string) (time.Duration, error) {
	return c.TTLWithGroup(ctx, "", key)
}

// TTLWithGroup 获取指定组中键的剩余生存时间
func (c *Client) TTLWithGroup(ctx context.Context, group, key string) (time.Duration, error) {
	if c.closed {
		return 0, ErrClientClosed
	}

	req := &cachepb.TTLRequest{
		Key:      key,
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.TTLResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.TTL(ctx, req)
		return err
	})

	if err != nil {
		return 0, err
	}

	if !resp.Found {
		return 0, ErrKeyNotFound
	}

	return resp.Ttl.AsDuration(), nil
}

// Expire 设置键的过期时间
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.ExpireWithGroup(ctx, "", key, ttl)
}

// ExpireWithGroup 设置指定组中键的过期时间
func (c *Client) ExpireWithGroup(ctx context.Context, group, key string, ttl time.Duration) error {
	if c.closed {
		return ErrClientClosed
	}

	req := &cachepb.ExpireRequest{
		Key:      key,
		Group:    group,
		Ttl:      durationpb.New(ttl),
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.ExpireResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.Expire(ctx, req)
		return err
	})

	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("expire operation failed: %s", resp.Status.Message)
	}

	return nil
}

// Stats 获取缓存统计信息
func (c *Client) Stats(ctx context.Context) (*CacheStats, error) {
	return c.StatsWithGroup(ctx, "")
}

// StatsWithGroup 获取指定组的缓存统计信息
func (c *Client) StatsWithGroup(ctx context.Context, group string) (*CacheStats, error) {
	if c.closed {
		return nil, ErrClientClosed
	}

	req := &cachepb.StatsRequest{
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.StatsResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.Stats(ctx, req)
		return err
	})

	if err != nil {
		return nil, err
	}

	// 将 RPC 返回的统计数据转换为本地 CacheStats 结构
	return &CacheStats{
		TotalKeys:     resp.Stats.TotalKeys,
		TotalSize:     resp.Stats.TotalSizeBytes,
		HitCount:      resp.Stats.HitCount,
		MissCount:     resp.Stats.MissCount,
		EvictionCount: resp.Stats.EvictionCount,
		HitRate:       resp.Stats.HitRate,
	}, nil
}

// Clear 清空缓存
func (c *Client) Clear(ctx context.Context) error {
	return c.ClearWithGroup(ctx, "")
}

// ClearWithGroup 清空指定组的缓存
func (c *Client) ClearWithGroup(ctx context.Context, group string) error {
	if c.closed {
		return ErrClientClosed
	}

	req := &cachepb.ClearRequest{
		Group:    group,
		Metadata: c.createRequestMetadata(),
	}

	var resp *cachepb.ClearResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.Clear(ctx, req)
		return err
	})

	if err != nil {
		return err
	}

	if resp.Status.Code != 0 {
		return fmt.Errorf("clear operation failed: %s", resp.Status.Message)
	}

	return nil
}

// Ping 检查连接状态
func (c *Client) Ping(ctx context.Context) error {
	if c.closed {
		return ErrClientClosed
	}

	req := &cachepb.HealthCheckRequest{
		Service: "cache",
	}

	var resp *cachepb.HealthCheckResponse
	var err error

	err = c.executeWithRetry(ctx, func(client cachepb.CacheServiceClient) error {
		resp, err = client.HealthCheck(ctx, req)
		return err
	})

	if err != nil {
		return err
	}

	if resp.Status != cachepb.HealthStatus_SERVING {
		return fmt.Errorf("service not healthy: %s", resp.Message)
	}

	return nil
}

// Close 关闭客户端
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// 关闭所有 gRPC 连接以释放资源
	for _, conn := range c.connections {
		if conn != nil {
			conn.Close()
		}
	}

	return nil
}

// 辅助方法

// executeWithRetry 执行带重试的操作
func (c *Client) executeWithRetry(ctx context.Context, fn func(cachepb.CacheServiceClient) error) error {
	var lastErr error

	// 尝试 0..MaxRetries 次（共 MaxRetries+1 次尝试）
	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		client, index := c.getClient() // 获取一个可用的客户端实例（负载均衡策略：轮询）
		if client == nil {
			return fmt.Errorf("no available clients")
		}

		// 执行具体 RPC 操作，错误由 fn 返回
		err := fn(client)

		if err == nil {
			atomic.StoreInt64(&c.unavailableUntil[index], 0)
			return nil // 成功则返回 nil
		}
		cooldown := c.config.FailureCooldown
		if cooldown <= 0 {
			cooldown = 2 * time.Second
		}
		atomic.StoreInt64(&c.unavailableUntil[index], time.Now().Add(cooldown).UnixNano())

		lastErr = err // 保存最近一次错误用于最终返回

		// 如果到达最大尝试次数则跳出循环（不再等待/重试）
		if attempt == c.config.MaxRetries {
			break
		}

		// 等待重试延迟，但尊重 ctx 的取消/超时
		select {
		case <-ctx.Done():
			return ctx.Err() // 上层 context 已取消或超时，立即返回
		case <-time.After(c.config.RetryDelay):
			// 等待后继续下一次尝试
		}
	}

	return lastErr // 返回最后一次错误
}

// getClient 使用轮询方式获取客户端
// 🔧 修复：使用原子操作避免并发竞态
func (c *Client) getClient() (cachepb.CacheServiceClient, int) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.closed || len(c.clients) == 0 {
		return nil, -1
	}

	now := time.Now().UnixNano()
	for range c.clients {
		counter := atomic.AddInt64(&c.roundRobin, 1)
		index := int(counter-1) % len(c.clients)
		if atomic.LoadInt64(&c.unavailableUntil[index]) <= now {
			return c.clients[index], index
		}
	}
	// All endpoints are cooling down: allow one probe rather than failing closed.
	counter := atomic.AddInt64(&c.roundRobin, 1)
	index := int(counter-1) % len(c.clients)
	return c.clients[index], index
}

// createRequestMetadata 创建请求元数据
func (c *Client) createRequestMetadata() *cachepb.RequestMetadata {
	return &cachepb.RequestMetadata{
		ClientId:  c.config.ClientID,
		Timestamp: timestamppb.Now(),
		Headers: map[string]string{
			"user-agent": "turbocache-go-client/1.0.0",
		},
	}
}

// CacheStats 缓存统计信息（本地表示层）
type CacheStats struct {
	TotalKeys     int64   `json:"total_keys"`
	TotalSize     int64   `json:"total_size"`
	HitCount      int64   `json:"hit_count"`
	MissCount     int64   `json:"miss_count"`
	EvictionCount int64   `json:"eviction_count"`
	HitRate       float64 `json:"hit_rate"`
}

// 错误定义
var (
	ErrClientClosed = fmt.Errorf("client is closed")
	ErrKeyNotFound  = fmt.Errorf("key not found")
)
