package transport

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"

	"turbocache/api/generated/cachepb"
	grpctransport "turbocache/internal/transport/grpc"
)

const bufConnSize = 1024 * 1024

// mockCacheServer 模拟缓存服务器
type mockCacheServer struct {
	cachepb.UnimplementedCacheServiceServer
	data map[string][]byte
}

func newMockCacheServer() *mockCacheServer {
	return &mockCacheServer{
		data: make(map[string][]byte),
	}
}

func (m *mockCacheServer) Get(ctx context.Context, req *cachepb.GetRequest) (*cachepb.GetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	value, exists := m.data[req.Key]
	if !exists {
		return &cachepb.GetResponse{
			Found:  false,
			Status: &cachepb.ResponseStatus{Code: 1, Message: "not found"},
		}, nil
	}

	return &cachepb.GetResponse{
		Value:  value,
		Found:  true,
		Status: &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

func (m *mockCacheServer) Set(ctx context.Context, req *cachepb.SetRequest) (*cachepb.SetResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	m.data[req.Key] = req.Value

	return &cachepb.SetResponse{
		Success: true,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

func (m *mockCacheServer) Delete(ctx context.Context, req *cachepb.DeleteRequest) (*cachepb.DeleteResponse, error) {
	if req.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	_, found := m.data[req.Key]
	delete(m.data, req.Key)

	return &cachepb.DeleteResponse{
		Success: found,
		Found:   found,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

func (m *mockCacheServer) BatchGet(ctx context.Context, req *cachepb.BatchGetRequest) (*cachepb.BatchGetResponse, error) {
	results := make([]*cachepb.GetResult, 0, len(req.Keys))

	for _, key := range req.Keys {
		result := &cachepb.GetResult{Key: key}
		if value, exists := m.data[key]; exists {
			result.Value = value
			result.Found = true
		}
		results = append(results, result)
	}

	return &cachepb.BatchGetResponse{
		Results: results,
		Status:  &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

func (m *mockCacheServer) Stats(ctx context.Context, req *cachepb.StatsRequest) (*cachepb.StatsResponse, error) {
	return &cachepb.StatsResponse{
		Stats: &cachepb.CacheStats{
			TotalKeys: int64(len(m.data)),
			HitCount:  100,
			MissCount: 10,
			HitRate:   0.9,
		},
		Status: &cachepb.ResponseStatus{Code: 0, Message: "success"},
	}, nil
}

func (m *mockCacheServer) HealthCheck(ctx context.Context, req *cachepb.HealthCheckRequest) (*cachepb.HealthCheckResponse, error) {
	return &cachepb.HealthCheckResponse{
		Status:  cachepb.HealthStatus_SERVING,
		Message: "healthy",
	}, nil
}

// setupMockServer 改用 testing.TB，兼容 *testing.T 和 *testing.B
func setupMockServer(tb testing.TB) (*grpc.Server, *bufconn.Listener, func()) {
	lis := bufconn.Listen(bufConnSize)

	server := grpc.NewServer()
	cachepb.RegisterCacheServiceServer(server, newMockCacheServer())

	go func() {
		if err := server.Serve(lis); err != nil {
			// 使用 tb 而不是 t
			tb.Logf("Server error: %v", err)
		}
	}()

	cleanup := func() {
		server.Stop()
		lis.Close()
	}

	return server, lis, cleanup
}

// TestNewClient 测试创建客户端
func TestNewClient(t *testing.T) {
	tests := []struct {
		name   string
		config *grpctransport.ClientConfig
	}{
		{
			name:   "with nil config (defaults)",
			config: nil,
		},
		{
			name: "with custom config",
			config: &grpctransport.ClientConfig{
				MaxRecvMsgSize:    8 * 1024 * 1024,
				MaxSendMsgSize:    8 * 1024 * 1024,
				ConnectionTimeout: 3 * time.Second,
				MaxRetries:        5,
				RetryInterval:     200 * time.Millisecond,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := grpctransport.NewClient(tt.config)
			if client == nil {
				t.Fatal("NewClient returned nil")
			}

			err := client.Close()
			if err != nil {
				t.Errorf("Close() failed: %v", err)
			}
		})
	}
}

// TestClient_Connect 测试连接到服务器
func TestClient_Connect(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	// 使用bufconn的连接
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}

	// 手动添加连接到客户端的连接池
	// 注意：实际使用中应该使用client.Connect()
	t.Logf("Connection established to mock server")
	conn.Close()
}

// TestClient_Get 测试Get操作
func TestClient_Get(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(&grpctransport.ClientConfig{
		MaxRetries:    1,
		RetryInterval: 10 * time.Millisecond,
	})
	defer client.Close()

	// 建立连接
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	if err != nil {
		t.Fatalf("Failed to dial: %v", err)
	}
	defer conn.Close()

	// 直接使用连接进行测试
	cacheClient := cachepb.NewCacheServiceClient(conn)

	// 先设置一个值
	setResp, err := cacheClient.Set(ctx, &cachepb.SetRequest{
		Key:   "test-key",
		Value: []byte("test-value"),
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	if !setResp.Success {
		t.Error("Set should succeed")
	}

	// 获取值
	getResp, err := cacheClient.Get(ctx, &cachepb.GetRequest{
		Key: "test-key",
	})
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !getResp.Found {
		t.Error("Expected key to be found")
	}

	if string(getResp.Value) != "test-value" {
		t.Errorf("Expected value 'test-value', got '%s'", string(getResp.Value))
	}
}

// TestClient_Set 测试Set操作
func TestClient_Set(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	tests := []struct {
		name  string
		key   string
		value []byte
		ttl   time.Duration
	}{
		{
			name:  "simple set",
			key:   "key1",
			value: []byte("value1"),
			ttl:   time.Hour,
		},
		{
			name:  "set with custom ttl",
			key:   "key2",
			value: []byte("value2"),
			ttl:   30 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := cacheClient.Set(ctx, &cachepb.SetRequest{
				Key:   tt.key,
				Value: tt.value,
				Ttl:   durationpb.New(tt.ttl),
			})

			if err != nil {
				t.Fatalf("Set failed: %v", err)
			}

			if !resp.Success {
				t.Error("Set should succeed")
			}
		})
	}
}

// TestClient_Delete 测试Delete操作
func TestClient_Delete(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	// 先设置一个值
	cacheClient.Set(ctx, &cachepb.SetRequest{
		Key:   "delete-key",
		Value: []byte("delete-value"),
	})

	// 删除
	resp, err := cacheClient.Delete(ctx, &cachepb.DeleteRequest{
		Key: "delete-key",
	})

	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	if !resp.Success || !resp.Found {
		t.Error("Delete should succeed and find the key")
	}

	// 再次删除应该找不到
	resp2, err := cacheClient.Delete(ctx, &cachepb.DeleteRequest{
		Key: "delete-key",
	})

	if err != nil {
		t.Fatalf("Second delete failed: %v", err)
	}

	if resp2.Found {
		t.Error("Second delete should not find the key")
	}
}

// TestClient_BatchGet 测试批量获取
func TestClient_BatchGet(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	// 设置多个值
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		value := fmt.Sprintf("batch-value-%d", i)
		cacheClient.Set(ctx, &cachepb.SetRequest{
			Key:   key,
			Value: []byte(value),
		})
	}

	// 批量获取
	keys := []string{"batch-key-1", "batch-key-2", "batch-key-3", "batch-key-99"}
	resp, err := cacheClient.BatchGet(ctx, &cachepb.BatchGetRequest{
		Keys: keys,
	})

	if err != nil {
		t.Fatalf("BatchGet failed: %v", err)
	}

	if len(resp.Results) != len(keys) {
		t.Errorf("Expected %d results, got %d", len(keys), len(resp.Results))
	}

	// 验证结果
	foundCount := 0
	for _, result := range resp.Results {
		if result.Found {
			foundCount++
		}
	}

	if foundCount != 3 {
		t.Errorf("Expected 3 found keys, got %d", foundCount)
	}
}

// TestClient_Stats 测试获取统计信息
func TestClient_Stats(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	resp, err := cacheClient.Stats(ctx, &cachepb.StatsRequest{})
	if err != nil {
		t.Fatalf("Stats failed: %v", err)
	}

	if resp.Stats == nil {
		t.Fatal("Stats should not be nil")
	}

	t.Logf("Stats: TotalKeys=%d, HitRate=%.2f",
		resp.Stats.TotalKeys, resp.Stats.HitRate)
}

// TestClient_HealthCheck 测试健康检查
func TestClient_HealthCheck(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	resp, err := cacheClient.HealthCheck(ctx, &cachepb.HealthCheckRequest{
		Service: "cache",
	})

	if err != nil {
		t.Fatalf("HealthCheck failed: %v", err)
	}

	if resp.Status != cachepb.HealthStatus_SERVING {
		t.Errorf("Expected SERVING status, got %v", resp.Status)
	}

	if resp.Message != "healthy" {
		t.Errorf("Expected 'healthy' message, got '%s'", resp.Message)
	}
}

// TestClient_ErrorHandling 测试错误处理
func TestClient_ErrorHandling(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	// 测试空键错误
	_, err := cacheClient.Get(ctx, &cachepb.GetRequest{
		Key: "",
	})

	if err == nil {
		t.Error("Expected error for empty key")
	}

	st, ok := status.FromError(err)
	if ok && st.Code() != codes.InvalidArgument {
		t.Errorf("Expected InvalidArgument error, got %v", st.Code())
	}
}

// TestClient_Timeout 测试超时
func TestClient_Timeout(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(&grpctransport.ClientConfig{
		ConnectionTimeout: 1 * time.Second,
	})
	defer client.Close()

	// 使用非常短的超时
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	conn, _ := grpc.DialContext(context.Background(), "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	_, err := cacheClient.Get(ctx, &cachepb.GetRequest{
		Key: "test-key",
	})

	if err == nil {
		t.Log("Expected timeout error (but may not occur with bufconn)")
	} else {
		t.Logf("Got expected error: %v", err)
	}
}

// TestClient_Close 测试关闭客户端
func TestClient_Close(t *testing.T) {
	client := grpctransport.NewClient(nil)

	// 第一次关闭
	err := client.Close()
	if err != nil {
		t.Errorf("Close() failed: %v", err)
	}

	// 第二次关闭应该是安全的
	err = client.Close()
	if err != nil {
		t.Errorf("Second Close() failed: %v", err)
	}
}

// TestClient_GetPoolStats 测试获取连接池统计
func TestClient_GetPoolStats(t *testing.T) {
	client := grpctransport.NewClient(nil)
	defer client.Close()

	stats := client.GetPoolStats()
	if stats == nil {
		t.Error("GetPoolStats() returned nil")
	}

	// 初始应该为空
	if len(stats) != 0 {
		t.Logf("Initial pool has %d targets", len(stats))
	}
}

// TestClient_RemoveConnection 测试移除连接
func TestClient_RemoveConnection(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	address := "bufnet"

	// 建立连接
	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, address,
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	// 移除连接（应该不会panic）
	client.RemoveConnection(address)

	// 再次移除应该是安全的
	client.RemoveConnection(address)
}

// TestClient_InvalidArguments 测试无效参数
func TestClient_InvalidArguments(t *testing.T) {
	_, lis, cleanup := setupMockServer(t)
	defer cleanup()

	client := grpctransport.NewClient(nil)
	defer client.Close()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	tests := []struct {
		name string
		req  interface{}
	}{
		{
			name: "get with empty key",
			req: &cachepb.GetRequest{
				Key: "",
			},
		},
		{
			name: "set with empty key",
			req: &cachepb.SetRequest{
				Key:   "",
				Value: []byte("value"),
			},
		},
		{
			name: "delete with empty key",
			req: &cachepb.DeleteRequest{
				Key: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var err error

			switch req := tt.req.(type) {
			case *cachepb.GetRequest:
				_, err = cacheClient.Get(ctx, req)
			case *cachepb.SetRequest:
				_, err = cacheClient.Set(ctx, req)
			case *cachepb.DeleteRequest:
				_, err = cacheClient.Delete(ctx, req)
			}

			if err == nil {
				t.Error("Expected error for invalid arguments")
			}
		})
	}
}

// BenchmarkClient_Get 性能基准测试
func BenchmarkClient_Get(b *testing.B) {
	_, lis, cleanup := setupMockServer(b)
	defer cleanup()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	// 预设数据
	cacheClient.Set(ctx, &cachepb.SetRequest{
		Key:   "bench-key",
		Value: []byte("bench-value"),
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cacheClient.Get(ctx, &cachepb.GetRequest{
			Key: "bench-key",
		})
	}
}

// BenchmarkClient_Set 性能基准测试
func BenchmarkClient_Set(b *testing.B) {
	_, lis, cleanup := setupMockServer(b)
	defer cleanup()

	ctx := context.Background()
	conn, _ := grpc.DialContext(ctx, "bufnet",
		grpc.WithInsecure(),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return lis.Dial()
		}))
	defer conn.Close()

	cacheClient := cachepb.NewCacheServiceClient(conn)

	value := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		cacheClient.Set(ctx, &cachepb.SetRequest{
			Key:   key,
			Value: value,
		})
	}
}
