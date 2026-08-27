package pool

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"turbocache/internal/utils/logger"
)

// Connection 连接包装器
type Connection struct {
	conn      *grpc.ClientConn
	target    string
	createdAt time.Time
	lastUsed  int64 // Unix纳秒时间戳
	useCount  int64
	closed    int32
}

// Config 连接池配置
type Config struct {
	MaxSize             int           `yaml:"max_size"`
	MinSize             int           `yaml:"min_size"`
	MaxIdleTime         time.Duration `yaml:"max_idle_time"`
	DialTimeout         time.Duration `yaml:"dial_timeout"`
	HealthCheckInterval time.Duration `yaml:"health_check_interval"`
}

// Stats 连接池统计信息
type Stats struct {
	Target            string    `json:"target"`
	TotalConnections  int       `json:"total_connections"`
	ActiveConnections int       `json:"active_connections"`
	TotalUseCount     int64     `json:"total_use_count"`
	OldestCreatedAt   time.Time `json:"oldest_created_at"`
	NewestCreatedAt   time.Time `json:"newest_created_at"`
}

// NewConnection 创建连接
func NewConnection(target string, opts ...grpc.DialOption) (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, target, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %v", target, err)
	}

	return &Connection{
		conn:      conn,
		target:    target,
		createdAt: time.Now(),
		lastUsed:  time.Now().UnixNano(),
	}, nil
}

// Conn 获取底层连接
func (c *Connection) Conn() *grpc.ClientConn {
	atomic.StoreInt64(&c.lastUsed, time.Now().UnixNano())
	atomic.AddInt64(&c.useCount, 1)
	return c.conn
}

// Target 获取连接目标
func (c *Connection) Target() string {
	return c.target
}

// CreatedAt 获取创建时间
func (c *Connection) CreatedAt() time.Time {
	return c.createdAt
}

// LastUsed 获取最后使用时间
func (c *Connection) LastUsed() time.Time {
	return time.Unix(0, atomic.LoadInt64(&c.lastUsed))
}

// UseCount 获取使用次数
func (c *Connection) UseCount() int64 {
	return atomic.LoadInt64(&c.useCount)
}

// IsHealthy 检查连接是否健康
func (c *Connection) IsHealthy() bool {
	if c.isClosed() {
		return false
	}

	state := c.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Idle
}

// Close 关闭连接
func (c *Connection) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return nil // 已经关闭
	}

	return c.conn.Close()
}

// isClosed 检查是否已关闭
func (c *Connection) isClosed() bool {
	return atomic.LoadInt32(&c.closed) != 0
}

// ConnectionPool gRPC连接池
type ConnectionPool struct {
	mu     sync.RWMutex
	pools  map[string]*targetPool
	config *Config
	logger logger.Logger
	closed int32

	// 健康检查
	healthTicker *time.Ticker
	healthDone   chan struct{}
}

// targetPool 单个目标的连接池
type targetPool struct {
	target      string
	connections []*Connection
	roundRobin  int64
	dialOpts    []grpc.DialOption
	mu          sync.RWMutex
}

// NewConnectionPool 创建连接池
func NewConnectionPool(config *Config) *ConnectionPool {
	if config == nil {
		config = &Config{
			MaxSize:             100,
			MinSize:             10,
			MaxIdleTime:         5 * time.Minute,
			DialTimeout:         5 * time.Second,
			HealthCheckInterval: 30 * time.Second,
		}
	}

	pool := &ConnectionPool{
		pools:      make(map[string]*targetPool),
		config:     config,
		logger:     logger.WithField("component", "connection_pool"),
		healthDone: make(chan struct{}),
	}

	// 启动健康检查
	if config.HealthCheckInterval > 0 {
		pool.startHealthCheck()
	}

	return pool
}

// Add 添加连接到池中
func (cp *ConnectionPool) Add(target string, conn *grpc.ClientConn) error {
	if cp.isClosed() {
		return fmt.Errorf("connection pool is closed")
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	pool, exists := cp.pools[target]
	if !exists {
		pool = &targetPool{
			target:      target,
			connections: make([]*Connection, 0, cp.config.MaxSize),
			dialOpts:    cp.buildDialOptions(),
		}
		cp.pools[target] = pool
	}

	connection := &Connection{
		conn:      conn,
		target:    target,
		createdAt: time.Now(),
		lastUsed:  time.Now().UnixNano(),
	}

	pool.mu.Lock()
	pool.connections = append(pool.connections, connection)
	pool.mu.Unlock()

	return nil
}

// Get 获取连接
func (cp *ConnectionPool) Get(target string) (*grpc.ClientConn, error) {
	conn, err := cp.GetConnection(target)
	if err != nil {
		return nil, err
	}
	return conn.Conn(), nil
}

// Put 归还连接
func (cp *ConnectionPool) Put(target string, conn *grpc.ClientConn) {
	// gRPC连接池中的连接通常不需要显式归还
	// 这里保留接口以保持兼容性
}

// Remove 移除目标的所有连接
func (cp *ConnectionPool) Remove(target string) {
	cp.RemoveTarget(target)
}

// GetConnection 获取连接
func (cp *ConnectionPool) GetConnection(target string, opts ...grpc.DialOption) (*Connection, error) {
	if cp.isClosed() {
		return nil, fmt.Errorf("connection pool is closed")
	}

	cp.mu.RLock()
	pool, exists := cp.pools[target]
	cp.mu.RUnlock()

	if !exists {
		cp.mu.Lock()
		// 双重检查
		if pool, exists = cp.pools[target]; !exists {
			pool = &targetPool{
				target:      target,
				connections: make([]*Connection, 0, cp.config.MaxSize),
				dialOpts:    cp.buildDialOptions(opts...),
			}
			cp.pools[target] = pool
		}
		cp.mu.Unlock()
	}

	return pool.getConnection()
}

// PutConnection 归还连接
func (cp *ConnectionPool) PutConnection(conn *Connection) {
	if cp.isClosed() || conn == nil {
		if conn != nil {
			conn.Close()
		}
		return
	}

	cp.mu.RLock()
	pool, exists := cp.pools[conn.Target()]
	cp.mu.RUnlock()

	if exists {
		pool.putConnection(conn)
	} else {
		conn.Close()
	}
}

// RemoveTarget 移除目标的所有连接
func (cp *ConnectionPool) RemoveTarget(target string) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if pool, exists := cp.pools[target]; exists {
		pool.closeAll()
		delete(cp.pools, target)
		cp.logger.Infof("Removed connection pool for target: %s", target)
	}
}

// GetStats 获取连接池统计信息
func (cp *ConnectionPool) GetStats() map[string]Stats {
	cp.mu.RLock()
	defer cp.mu.RUnlock()

	stats := make(map[string]Stats)
	for target, pool := range cp.pools {
		poolStats := pool.stats()
		stats[target] = Stats{
			Target:            poolStats["target"].(string),
			TotalConnections:  poolStats["total_connections"].(int),
			ActiveConnections: poolStats["active_connections"].(int),
			TotalUseCount:     poolStats["total_use_count"].(int64),
			OldestCreatedAt:   poolStats["oldest_created_at"].(time.Time),
			NewestCreatedAt:   poolStats["newest_created_at"].(time.Time),
		}
	}

	return stats
}

// Close 关闭连接池
func (cp *ConnectionPool) Close() error {
	if !atomic.CompareAndSwapInt32(&cp.closed, 0, 1) {
		return nil
	}

	// 停止健康检查
	if cp.healthTicker != nil {
		cp.healthTicker.Stop()
	}
	close(cp.healthDone)

	cp.mu.Lock()
	defer cp.mu.Unlock()

	// 关闭所有连接池
	for target, pool := range cp.pools {
		pool.closeAll()
		cp.logger.Infof("Closed connection pool for target: %s", target)
	}

	cp.pools = make(map[string]*targetPool)
	cp.logger.Info("Connection pool closed")

	return nil
}

// isClosed 检查连接池是否已关闭
func (cp *ConnectionPool) isClosed() bool {
	return atomic.LoadInt32(&cp.closed) != 0
}

// buildDialOptions 构建拨号选项
func (cp *ConnectionPool) buildDialOptions(extraOpts ...grpc.DialOption) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		}),
	}

	if cp.config.DialTimeout > 0 {
		opts = append(opts, grpc.WithBlock())
	}

	// 添加额外选项
	opts = append(opts, extraOpts...)

	return opts
}

// startHealthCheck 启动健康检查
func (cp *ConnectionPool) startHealthCheck() {
	cp.healthTicker = time.NewTicker(cp.config.HealthCheckInterval)

	go func() {
		defer cp.healthTicker.Stop()

		for {
			select {
			case <-cp.healthTicker.C:
				cp.healthCheck()
			case <-cp.healthDone:
				return
			}
		}
	}()
}

// healthCheck 执行健康检查
func (cp *ConnectionPool) healthCheck() {
	cp.mu.RLock()
	pools := make([]*targetPool, 0, len(cp.pools))
	for _, pool := range cp.pools {
		pools = append(pools, pool)
	}
	cp.mu.RUnlock()

	for _, pool := range pools {
		pool.healthCheck(cp.config.MaxIdleTime)
	}
}

// targetPool 方法实现

// getConnection 获取连接
func (tp *targetPool) getConnection() (*Connection, error) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	// 尝试获取可用连接
	for _, conn := range tp.connections {
		if conn.IsHealthy() {
			// 使用轮询方式选择连接
			idx := int(atomic.AddInt64(&tp.roundRobin, 1)) % len(tp.connections)
			if idx < len(tp.connections) && tp.connections[idx].IsHealthy() {
				return tp.connections[idx], nil
			}
			// 如果轮询选择的连接不健康,使用第一个健康的
			return conn, nil
		}
	}

	// 没有可用连接,创建新连接
	return tp.createConnection()
}

// putConnection 归还连接
func (tp *targetPool) putConnection(conn *Connection) {
	// gRPC连接是长连接,通常不需要归还操作
	// 这里可以更新连接的使用统计信息
	atomic.StoreInt64(&conn.lastUsed, time.Now().UnixNano())
}

// createConnection 创建新连接
func (tp *targetPool) createConnection() (*Connection, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, tp.target, tp.dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %v", tp.target, err)
	}

	connection := &Connection{
		conn:      conn,
		target:    tp.target,
		createdAt: time.Now(),
		lastUsed:  time.Now().UnixNano(),
	}

	tp.connections = append(tp.connections, connection)
	return connection, nil
}

// healthCheck 健康检查
func (tp *targetPool) healthCheck(maxIdleTime time.Duration) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	now := time.Now()
	activeConnections := make([]*Connection, 0, len(tp.connections))

	for _, conn := range tp.connections {
		// 检查连接是否过期或不健康
		if !conn.IsHealthy() || now.Sub(conn.LastUsed()) > maxIdleTime {
			conn.Close()
			continue
		}

		activeConnections = append(activeConnections, conn)
	}

	tp.connections = activeConnections
}

// stats 获取池统计信息
func (tp *targetPool) stats() map[string]interface{} {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	activeCount := 0
	totalUseCount := int64(0)
	oldestCreatedAt := time.Now()
	newestCreatedAt := time.Time{}

	for _, conn := range tp.connections {
		if conn.IsHealthy() {
			activeCount++
		}
		totalUseCount += conn.UseCount()

		if conn.CreatedAt().Before(oldestCreatedAt) {
			oldestCreatedAt = conn.CreatedAt()
		}
		if conn.CreatedAt().After(newestCreatedAt) {
			newestCreatedAt = conn.CreatedAt()
		}
	}

	return map[string]interface{}{
		"target":             tp.target,
		"total_connections":  len(tp.connections),
		"active_connections": activeCount,
		"total_use_count":    totalUseCount,
		"oldest_created_at":  oldestCreatedAt,
		"newest_created_at":  newestCreatedAt,
	}
}

// closeAll 关闭所有连接
func (tp *targetPool) closeAll() {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	for _, conn := range tp.connections {
		conn.Close()
	}

	tp.connections = tp.connections[:0]
}

// WithConnection 使用连接执行操作
func (cp *ConnectionPool) WithConnection(target string, fn func(*grpc.ClientConn) error) error {
	conn, err := cp.GetConnection(target)
	if err != nil {
		return err
	}

	defer cp.PutConnection(conn)
	return fn(conn.Conn())
}

// Warmup 预热连接池
func (cp *ConnectionPool) Warmup(target string, count int) error {
	if count <= 0 {
		return nil
	}

	cp.mu.Lock()
	pool, exists := cp.pools[target]
	if !exists {
		pool = &targetPool{
			target:      target,
			connections: make([]*Connection, 0, cp.config.MaxSize),
			dialOpts:    cp.buildDialOptions(),
		}
		cp.pools[target] = pool
	}
	cp.mu.Unlock()

	// 创建指定数量的连接
	pool.mu.Lock()
	defer pool.mu.Unlock()

	for i := 0; i < count && len(pool.connections) < cp.config.MaxSize; i++ {
		_, err := pool.createConnection()
		if err != nil {
			cp.logger.Warnf("Failed to create warmup connection for %s: %v", target, err)
		}
	}

	cp.logger.Infof("Warmed up %d connections for target: %s",
		len(pool.connections), target)

	return nil
}
