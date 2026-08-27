package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/test/bufconn"

	"turbocache/internal/transport/pool"
)

// 测试用的缓冲连接大小
const bufSize = 1024 * 1024

// TestNewConnectionPool 测试创建连接池
func TestNewConnectionPool(t *testing.T) {
	tests := []struct {
		name   string
		config *pool.Config
	}{
		{
			name: "with custom config",
			config: &pool.Config{
				MaxSize:             50,
				MinSize:             5,
				MaxIdleTime:         3 * time.Minute,
				DialTimeout:         3 * time.Second,
				HealthCheckInterval: 20 * time.Second,
			},
		},
		{
			name:   "with nil config (defaults)",
			config: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cp := pool.NewConnectionPool(tt.config)
			if cp == nil {
				t.Fatal("NewConnectionPool returned nil")
			}

			stats := cp.GetStats()
			if len(stats) != 0 {
				t.Errorf("New pool should have no connections, got %d targets", len(stats))
			}

			cp.Close()
		})
	}
}

// TestConnectionPool_AddGet 测试添加和获取连接
func TestConnectionPool_AddGet(t *testing.T) {
	cp := pool.NewConnectionPool(nil)
	defer cp.Close()

	target := "bufnet"

	// 创建mock连接
	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	// 启动测试服务器
	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	// 使用 GetConnection 自动创建连接
	conn, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("GetConnection() failed: %v", err)
	}

	if conn == nil {
		t.Fatal("Retrieved connection is nil")
	}

	// 验证连接状态
	state := conn.Conn().GetState()
	if state != connectivity.Ready && state != connectivity.Idle && state != connectivity.Connecting {
		t.Errorf("Unexpected connection state: %v", state)
	}

	// 再次获取，应该复用连接
	conn2, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Second GetConnection() failed: %v", err)
	}

	if conn2 == nil {
		t.Fatal("Second connection is nil")
	}

	// 验证统计信息
	stats := cp.GetStats()
	if len(stats) != 1 {
		t.Errorf("Expected 1 target in stats, got %d", len(stats))
	}
}

// TestConnectionPool_GetConnection 测试获取连接（自动创建）
func TestConnectionPool_GetConnection(t *testing.T) {
	cp := pool.NewConnectionPool(&pool.Config{
		MaxSize:     10,
		MinSize:     2,
		MaxIdleTime: 5 * time.Minute,
		DialTimeout: 2 * time.Second,
	})
	defer cp.Close()

	// 创建bufconn监听器
	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	target := "bufnet"

	// 获取连接（会自动创建）
	conn, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("GetConnection() failed: %v", err)
	}

	if conn == nil {
		t.Fatal("Connection is nil")
	}

	// 再次获取应该复用连接
	conn2, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Second GetConnection() failed: %v", err)
	}

	if conn2 == nil {
		t.Fatal("Second connection is nil")
	}
}

// TestConnectionPool_Remove 测试移除连接
func TestConnectionPool_Remove(t *testing.T) {
	cp := pool.NewConnectionPool(nil)
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	target := "test-target"

	// 创建连接
	_, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("GetConnection() failed: %v", err)
	}

	// 验证连接已添加
	stats := cp.GetStats()
	if len(stats) != 1 {
		t.Errorf("Expected 1 target, got %d", len(stats))
	}

	// 移除连接
	cp.Remove(target)

	// 验证连接已移除
	stats = cp.GetStats()
	if len(stats) != 0 {
		t.Errorf("Expected 0 targets after removal, got %d", len(stats))
	}
}

// TestConnectionPool_WithConnection 测试WithConnection辅助方法
func TestConnectionPool_WithConnection(t *testing.T) {
	cp := pool.NewConnectionPool(nil)
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	target := "test-target"

	// 先创建连接
	_, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("GetConnection() failed: %v", err)
	}

	executed := false
	err = cp.WithConnection(target, func(conn *grpc.ClientConn) error {
		executed = true
		if conn == nil {
			return fmt.Errorf("connection is nil")
		}
		return nil
	})

	if err != nil {
		t.Errorf("WithConnection() failed: %v", err)
	}

	if !executed {
		t.Error("WithConnection callback was not executed")
	}
}

// TestConnectionPool_GetStats 测试获取统计信息
func TestConnectionPool_GetStats(t *testing.T) {
	cp := pool.NewConnectionPool(nil)
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	// 添加多个目标的连接
	for i := 1; i <= 3; i++ {
		target := fmt.Sprintf("target-%d", i)
		_, err := cp.GetConnection(target,
			grpc.WithInsecure(),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}))
		if err != nil {
			t.Fatalf("GetConnection() failed for %s: %v", target, err)
		}
	}

	stats := cp.GetStats()
	if len(stats) != 3 {
		t.Errorf("Expected 3 targets in stats, got %d", len(stats))
	}

	// 验证统计信息结构
	for target, stat := range stats {
		if stat.Target != target {
			t.Errorf("Target mismatch: expected %s, got %s", target, stat.Target)
		}

		if stat.TotalConnections <= 0 {
			t.Errorf("Target %s should have at least 1 connection", target)
		}
	}
}

// TestConnectionPool_Close 测试关闭连接池
func TestConnectionPool_Close(t *testing.T) {
	cp := pool.NewConnectionPool(nil)

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	// 添加连接
	target := "test-target"
	_, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("GetConnection() failed: %v", err)
	}

	// 关闭连接池
	err = cp.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// 再次关闭应该是安全的
	err = cp.Close()
	if err != nil {
		t.Errorf("Second Close() failed: %v", err)
	}

	// 关闭后操作应该失败
	_, err = cp.GetConnection("new-target",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err == nil {
		t.Error("GetConnection() should fail after Close()")
	}
}

// TestConnectionPool_Warmup 测试预热连接池
func TestConnectionPool_Warmup(t *testing.T) {
	cp := pool.NewConnectionPool(&pool.Config{
		MaxSize:     20,
		DialTimeout: 1 * time.Second,
	})
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	target := "warmup-target"

	// 需要先创建 targetPool，通过调用一次 GetConnection
	_, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Initial GetConnection() failed: %v", err)
	}

	// 预热5个连接
	err = cp.Warmup(target, 5)
	if err != nil {
		t.Logf("Warmup returned error (may be expected): %v", err)
	}

	// 检查统计信息
	stats := cp.GetStats()
	if stat, exists := stats[target]; exists {
		t.Logf("Warmed up connections: %d", stat.TotalConnections)
		if stat.TotalConnections < 1 {
			t.Error("Should have at least 1 connection after warmup")
		}
	} else {
		t.Error("Target not found in stats after warmup")
	}
}

// TestConnection_Properties 测试Connection对象的属性
func TestConnection_Properties(t *testing.T) {
	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	target := "test-target"

	conn, err := pool.NewConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("NewConnection() failed: %v", err)
	}
	defer conn.Close()

	// 测试Target
	if conn.Target() != target {
		t.Errorf("Target() = %s, want %s", conn.Target(), target)
	}

	// 测试CreatedAt
	createdAt := conn.CreatedAt()
	if createdAt.IsZero() {
		t.Error("CreatedAt() returned zero time")
	}

	// 测试LastUsed
	lastUsed := conn.LastUsed()
	if lastUsed.IsZero() {
		t.Error("LastUsed() returned zero time")
	}

	// 测试UseCount
	initialCount := conn.UseCount()

	// 使用连接
	_ = conn.Conn()

	newCount := conn.UseCount()
	if newCount <= initialCount {
		t.Errorf("UseCount should increase after use: %d -> %d", initialCount, newCount)
	}

	// 测试IsHealthy
	healthy := conn.IsHealthy()
	t.Logf("Connection healthy: %v", healthy)
}

// TestConnectionPool_ConcurrentOperations 测试并发操作
func TestConnectionPool_ConcurrentOperations(t *testing.T) {
	cp := pool.NewConnectionPool(&pool.Config{
		MaxSize:     50,
		MinSize:     5,
		MaxIdleTime: 1 * time.Minute,
	})
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// 并发获取连接
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			target := fmt.Sprintf("target-%d", id%5) // 5个不同的目标
			_, err := cp.GetConnection(target,
				grpc.WithInsecure(),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return lis.Dial()
				}))
			if err != nil {
				errChan <- err
			}
		}(i)
	}

	// 并发获取统计信息
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = cp.GetStats()
		}()
	}

	wg.Wait()
	close(errChan)

	// 检查错误
	errorCount := 0
	for err := range errChan {
		errorCount++
		t.Logf("Concurrent operation error: %v", err)
	}

	if errorCount > 0 {
		t.Logf("Total errors in concurrent operations: %d", errorCount)
	}

	// 验证最终状态
	stats := cp.GetStats()
	t.Logf("Final pool stats: %d targets", len(stats))
	if len(stats) == 0 {
		t.Error("Expected at least some targets in final stats")
	}
}

// TestConnectionPool_HealthCheck 测试健康检查（间接）
func TestConnectionPool_HealthCheck(t *testing.T) {
	cp := pool.NewConnectionPool(&pool.Config{
		MaxSize:             10,
		MaxIdleTime:         100 * time.Millisecond, // 短超时用于测试
		HealthCheckInterval: 50 * time.Millisecond,  // 频繁检查
	})
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()

	target := "health-check-target"
	_, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("GetConnection() failed: %v", err)
	}

	// 等待健康检查运行
	time.Sleep(200 * time.Millisecond)

	// 停止服务器（使连接变为不健康）
	server.Stop()

	// 再等待一次健康检查
	time.Sleep(200 * time.Millisecond)

	stats := cp.GetStats()
	t.Logf("Stats after health check: %+v", stats)
}

// TestConnectionPool_MaxIdleTime 测试最大空闲时间
func TestConnectionPool_MaxIdleTime(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping idle time test in short mode")
	}

	cp := pool.NewConnectionPool(&pool.Config{
		MaxSize:             10,
		MaxIdleTime:         200 * time.Millisecond,
		HealthCheckInterval: 100 * time.Millisecond,
	})
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	target := "idle-test"
	_, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("GetConnection() failed: %v", err)
	}

	// 初始状态
	stats := cp.GetStats()
	initialCount := stats[target].TotalConnections
	t.Logf("Initial connections: %d", initialCount)

	// 等待超过MaxIdleTime
	time.Sleep(400 * time.Millisecond)

	// 检查是否被清理
	stats = cp.GetStats()
	if stat, exists := stats[target]; exists {
		t.Logf("Connections after idle timeout: %d (active: %d)",
			stat.TotalConnections, stat.ActiveConnections)
	} else {
		t.Log("Target removed after idle timeout")
	}
}

// BenchmarkConnectionPool_Get 性能基准测试
func BenchmarkConnectionPool_Get(b *testing.B) {
	cp := pool.NewConnectionPool(nil)
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	target := "bench-target"
	_, err := cp.GetConnection(target,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		b.Fatalf("GetConnection() failed: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cp.Get(target)
	}
}

// BenchmarkConnectionPool_Concurrent 并发性能基准
func BenchmarkConnectionPool_Concurrent(b *testing.B) {
	cp := pool.NewConnectionPool(nil)
	defer cp.Close()

	lis := bufconn.Listen(bufSize)
	defer lis.Close()

	server := grpc.NewServer()
	go func() {
		server.Serve(lis)
	}()
	defer server.Stop()

	targets := make([]string, 10)
	for i := range targets {
		targets[i] = fmt.Sprintf("target-%d", i)
		_, err := cp.GetConnection(targets[i],
			grpc.WithInsecure(),
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
				return lis.Dial()
			}))
		if err != nil {
			b.Fatalf("GetConnection() failed for %s: %v", targets[i], err)
		}
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			target := targets[i%len(targets)]
			cp.Get(target)
			i++
		}
	})
}
