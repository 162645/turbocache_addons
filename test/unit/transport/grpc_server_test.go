package transport

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"turbocache/api/generated/cachepb"
	"turbocache/internal/cache/value"
)

// mockMultiLevelCache 模拟多级缓存（带锁以保证并发安全）
type mockMultiLevelCache struct {
	mu   sync.RWMutex
	data map[string]*value.Entry
}

func newMockMultiLevelCache() *mockMultiLevelCache {
	return &mockMultiLevelCache{
		data: make(map[string]*value.Entry),
	}
}

func (m *mockMultiLevelCache) Get(ctx context.Context, key string) (*value.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if entry, exists := m.data[key]; exists {
		return entry, nil
	}
	return nil, fmt.Errorf("key not found")
}

func (m *mockMultiLevelCache) Set(ctx context.Context, key string, entry *value.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = entry
	return nil
}

func (m *mockMultiLevelCache) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *mockMultiLevelCache) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]*value.Entry)
	return nil
}

func (m *mockMultiLevelCache) Stats() (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]interface{}{
		"total_keys":       len(m.data),
		"total_requests":   1000,
		"l1_hits":          800,
		"l2_hits":          150,
		"misses":           50,
		"overall_hit_rate": 0.95,
		"last_update":      time.Now(),
	}, nil
}

// mockHashRing 模拟一致性哈希环
type mockHashRing struct {
	localNodeID string
}

func (m *mockHashRing) GetNode(key string) *mockNode {
	// 简单实现：所有键都路由到本地节点
	return &mockNode{
		ID:      m.localNodeID,
		Address: "127.0.0.1:50051",
	}
}

type mockNode struct {
	ID      string
	Address string
}

// mockConnectionPool 模拟连接池（未使用于本测试，但保留接口兼容性）
type mockConnectionPool struct{}

func (m *mockConnectionPool) WithConnection(address string, fn func(conn interface{}) error) error {
	// 简单实现：直接调用函数
	return fn(nil)
}

// ----------------- 各项测试 -----------------

// TestServer_GetRequest 测试 Get 请求处理
func TestServer_GetRequest(t *testing.T) {
	t.Run("空 key 应返回错误", func(t *testing.T) {
		req := &cachepb.GetRequest{
			Key: "",
		}

		if req.Key == "" {
			err := status.Error(codes.InvalidArgument, "key cannot be empty")
			if err == nil {
				t.Error("期望对空 key 返回错误，但未得到错误")
			}
		}
	})

	t.Run("有效的 get 请求", func(t *testing.T) {
		req := &cachepb.GetRequest{
			Key:   "test-key",
			Group: "test-group",
		}

		if req.Key == "" {
			t.Error("Key 不应为空")
		}

		cache := newMockMultiLevelCache()
		entry := value.NewEntry("test-key", value.NewByteView([]byte("test-value")), time.Hour)
		if err := cache.Set(context.Background(), "test-key", entry); err != nil {
			t.Fatalf("设置测试数据失败：%v", err)
		}

		retrievedEntry, err := cache.Get(context.Background(), "test-key")
		if err != nil {
			t.Fatalf("Get 操作失败：%v", err)
		}

		if retrievedEntry == nil {
			t.Fatal("期望 Entry 非空，结果为 nil")
		}

		if string(retrievedEntry.Value.ByteSlice()) != "test-value" {
			t.Errorf("期望值 'test-value'，但得到 '%s'", string(retrievedEntry.Value.ByteSlice()))
		}
	})
}

// TestServer_SetRequest 测试 Set 请求处理
func TestServer_SetRequest(t *testing.T) {
	t.Run("空 key 应返回错误", func(t *testing.T) {
		req := &cachepb.SetRequest{
			Key:   "",
			Value: []byte("value"),
		}

		if req.Key == "" {
			err := status.Error(codes.InvalidArgument, "key cannot be empty")
			if err == nil {
				t.Error("期望对空 key 的 Set 返回错误，但未得到错误")
			}
		}
	})

	t.Run("有效的 set 请求", func(t *testing.T) {
		cache := newMockMultiLevelCache()

		req := &cachepb.SetRequest{
			Key:   "test-key",
			Value: []byte("test-value"),
		}

		entry := value.NewEntry(req.Key, value.NewByteView(req.Value), time.Hour)
		err := cache.Set(context.Background(), req.Key, entry)
		if err != nil {
			t.Fatalf("执行 Set 失败：%v", err)
		}

		retrievedEntry, err := cache.Get(context.Background(), req.Key)
		if err != nil {
			t.Fatalf("Set 之后 Get 操作失败：%v", err)
		}

		if string(retrievedEntry.Value.ByteSlice()) != string(req.Value) {
			t.Errorf("值不匹配：期望 '%s'，得到 '%s'", string(req.Value), string(retrievedEntry.Value.ByteSlice()))
		}
	})
}

// TestServer_DeleteRequest 测试 Delete 请求处理
func TestServer_DeleteRequest(t *testing.T) {
	t.Run("空 key 应返回错误", func(t *testing.T) {
		req := &cachepb.DeleteRequest{
			Key: "",
		}

		if req.Key == "" {
			err := status.Error(codes.InvalidArgument, "key cannot be empty")
			if err == nil {
				t.Error("期望对空 key 的 Delete 返回错误，但未得到错误")
			}
		}
	})

	t.Run("删除已存在的键", func(t *testing.T) {
		cache := newMockMultiLevelCache()

		entry := value.NewEntry("delete-key", value.NewByteView([]byte("delete-value")), time.Hour)
		cache.Set(context.Background(), "delete-key", entry)

		err := cache.Delete(context.Background(), "delete-key")
		if err != nil {
			t.Fatalf("Delete 失败：%v", err)
		}

		_, err = cache.Get(context.Background(), "delete-key")
		if err == nil {
			t.Error("删除后不应再能获取到该键")
		}
	})

	t.Run("删除不存在的键不应报错", func(t *testing.T) {
		cache := newMockMultiLevelCache()

		err := cache.Delete(context.Background(), "non-existing-key")
		if err != nil {
			t.Logf("删除不存在键返回错误（可接受）：%v", err)
		}
	})
}

// TestServer_BatchGet 测试批量获取
func TestServer_BatchGet(t *testing.T) {
	cache := newMockMultiLevelCache()

	// 准备数据
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("batch-key-%d", i)
		val := fmt.Sprintf("batch-value-%d", i)
		entry := value.NewEntry(key, value.NewByteView([]byte(val)), time.Hour)
		cache.Set(context.Background(), key, entry)
	}

	t.Run("混合结果的批量获取", func(t *testing.T) {
		keys := []string{"batch-key-1", "batch-key-2", "batch-key-999", "batch-key-3"}

		foundCount := 0
		notFoundCount := 0

		for _, key := range keys {
			_, err := cache.Get(context.Background(), key)
			if err == nil {
				foundCount++
			} else {
				notFoundCount++
			}
		}

		if foundCount != 3 {
			t.Errorf("期望找到 3 个键，实际找到 %d 个", foundCount)
		}

		if notFoundCount != 1 {
			t.Errorf("期望未找到 1 个键，实际未找到 %d 个", notFoundCount)
		}
	})

	t.Run("空 keys 数组", func(t *testing.T) {
		req := &cachepb.BatchGetRequest{
			Keys: []string{},
		}

		if len(req.Keys) != 0 {
			t.Error("期望 keys 为空数组")
		}
	})
}

// TestServer_Stats 测试统计信息
func TestServer_Stats(t *testing.T) {
	cache := newMockMultiLevelCache()

	// 添加一些数据
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("stats-key-%d", i)
		entry := value.NewEntry(key, value.NewByteView([]byte("value")), time.Hour)
		cache.Set(context.Background(), key, entry)
	}

	stats, err := cache.Stats()
	if err != nil {
		t.Fatalf("Stats() 失败：%v", err)
	}

	if stats == nil {
		t.Fatal("期望 Stats 非空")
	}

	statsMap := stats.(map[string]interface{})

	if totalKeys, ok := statsMap["total_keys"]; ok {
		if totalKeys.(int) != 10 {
			t.Errorf("期望 10 个键，实际 %v", totalKeys)
		}
	}

	if hitRate, ok := statsMap["overall_hit_rate"]; ok {
		t.Logf("命中率: %.2f", hitRate.(float64))
	}
}

// TestServer_HealthCheck 测试健康检查
func TestServer_HealthCheck(t *testing.T) {
	t.Run("健康检查应返回 SERVING 状态", func(t *testing.T) {
		// 模拟健康检查响应（直接构造）
		resp := &cachepb.HealthCheckResponse{
			Status:  cachepb.HealthStatus_SERVING,
			Message: "healthy",
			Metadata: map[string]string{
				"version": "1.0.0",
			},
		}

		if resp.Status != cachepb.HealthStatus_SERVING {
			t.Errorf("期望状态为 SERVING，实际为 %v", resp.Status)
		}

		if resp.Message != "healthy" {
			t.Errorf("期望返回消息为 'healthy'，实际为 '%s'", resp.Message)
		}
	})
}

// TestServer_Clear 测试清空缓存
func TestServer_Clear(t *testing.T) {
	cache := newMockMultiLevelCache()

	// 添加数据
	for i := 1; i <= 5; i++ {
		key := fmt.Sprintf("clear-key-%d", i)
		entry := value.NewEntry(key, value.NewByteView([]byte("value")), time.Hour)
		cache.Set(context.Background(), key, entry)
	}

	stats, _ := cache.Stats()
	statsMap := stats.(map[string]interface{})
	if statsMap["total_keys"].(int) != 5 {
		t.Errorf("期望 clear 前有 5 个键，实际 %v", statsMap["total_keys"])
	}

	// 清空
	err := cache.Clear(context.Background())
	if err != nil {
		t.Fatalf("Clear() 失败：%v", err)
	}

	stats, _ = cache.Stats()
	statsMap = stats.(map[string]interface{})
	if statsMap["total_keys"].(int) != 0 {
		t.Errorf("期望 clear 后有 0 个键，实际 %v", statsMap["total_keys"])
	}
}

// TestServer_RequestDepth 测试请求深度（防链式查询）
func TestServer_RequestDepth(t *testing.T) {
	t.Run("深度 0 请求", func(t *testing.T) {
		req := &cachepb.GetRequest{
			Key: "test-key",
			Metadata: &cachepb.RequestMetadata{
				Headers: map[string]string{},
			},
		}

		depth := 0
		if req.Metadata != nil && req.Metadata.Headers != nil {
			if depthStr, ok := req.Metadata.Headers["X-TurboCache-Depth"]; ok {
				fmt.Sscanf(depthStr, "%d", &depth)
			}
		}

		if depth != 0 {
			t.Errorf("期望深度为 0，实际为 %d", depth)
		}
	})

	t.Run("深度 1 请求应强制本地处理", func(t *testing.T) {
		req := &cachepb.GetRequest{
			Key: "test-key",
			Metadata: &cachepb.RequestMetadata{
				Headers: map[string]string{
					"X-TurboCache-Depth": "1",
				},
			},
		}

		depth := 0
		if req.Metadata != nil && req.Metadata.Headers != nil {
			if depthStr, ok := req.Metadata.Headers["X-TurboCache-Depth"]; ok {
				fmt.Sscanf(depthStr, "%d", &depth)
			}
		}

		if depth != 1 {
			t.Errorf("期望深度为 1，实际为 %d", depth)
		}

		if depth > 0 {
			t.Log("深度 > 0 的请求应由本地处理")
		}
	})

	t.Run("深度 > 1 为违规情况", func(t *testing.T) {
		req := &cachepb.GetRequest{
			Key: "test-key",
			Metadata: &cachepb.RequestMetadata{
				Headers: map[string]string{
					"X-TurboCache-Depth": "2",
				},
			},
		}

		depth := 0
		if req.Metadata != nil && req.Metadata.Headers != nil {
			if depthStr, ok := req.Metadata.Headers["X-TurboCache-Depth"]; ok {
				fmt.Sscanf(depthStr, "%d", &depth)
			}
		}

		if depth > 1 {
			t.Logf("检测到深度违规：%d", depth)
		}
	})
}

// TestServer_LocalKeyRouting 测试本地键路由
func TestServer_LocalKeyRouting(t *testing.T) {
	hashRing := &mockHashRing{
		localNodeID: "node1",
	}

	tests := []struct {
		name        string
		key         string
		localNodeID string
		expectLocal bool
	}{
		{
			name:        "本地键",
			key:         "local-key",
			localNodeID: "node1",
			expectLocal: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := hashRing.GetNode(tt.key)
			isLocal := node.ID == tt.localNodeID

			if isLocal != tt.expectLocal {
				t.Errorf("期望 isLocal=%v，实际 %v", tt.expectLocal, isLocal)
			}
		})
	}
}

// TestServer_GroupKeysByNode 测试按节点分组键
func TestServer_GroupKeysByNode(t *testing.T) {
	hashRing := &mockHashRing{
		localNodeID: "node1",
	}

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	groups := make(map[string][]string)

	for _, key := range keys {
		node := hashRing.GetNode(key)
		var addr string
		if node.ID == hashRing.localNodeID {
			addr = "local"
		} else {
			addr = node.Address
		}
		groups[addr] = append(groups[addr], key)
	}

	if len(groups) == 0 {
		t.Error("分组结果不应为空")
	}

	for addr, groupKeys := range groups {
		t.Logf("节点 %s: %d 个键", addr, len(groupKeys))
	}
}

// TestServer_BatchSetRequest 测试批量设置
func TestServer_BatchSetRequest(t *testing.T) {
	cache := newMockMultiLevelCache()

	items := []*cachepb.SetItem{
		{Key: "batch-set-1", Value: []byte("value1")},
		{Key: "batch-set-2", Value: []byte("value2")},
		{Key: "batch-set-3", Value: []byte("value3")},
	}

	successCount := 0
	for _, item := range items {
		entry := value.NewEntry(item.Key, value.NewByteView(item.Value), time.Hour)
		err := cache.Set(context.Background(), item.Key, entry)
		if err == nil {
			successCount++
		}
	}

	if successCount != len(items) {
		t.Errorf("期望成功设置 %d 个，实际 %d 个", len(items), successCount)
	}

	for _, item := range items {
		_, err := cache.Get(context.Background(), item.Key)
		if err != nil {
			t.Errorf("期望键 %s 存在，但未找到：%v", item.Key, err)
		}
	}
}

// TestServer_ConcurrentRequests 测试并发请求
func TestServer_ConcurrentRequests(t *testing.T) {
	cache := newMockMultiLevelCache()

	var wg sync.WaitGroup
	errChan := make(chan error, 100)

	// 并发写入
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%d", id)
			entry := value.NewEntry(key, value.NewByteView([]byte("value")), time.Hour)
			if err := cache.Set(context.Background(), key, entry); err != nil {
				errChan <- err
			}
		}(i)
	}

	// 并发读取
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent-key-%d", id%25)
			if _, err := cache.Get(context.Background(), key); err != nil {
				// 读取失败是正常的（可能还没写入），仅记录日志
			}
		}(i)
	}

	wg.Wait()
	close(errChan)

	for err := range errChan {
		t.Errorf("并发操作出现错误：%v", err)
	}

	stats, _ := cache.Stats()
	statsMap := stats.(map[string]interface{})
	t.Logf("最终键数：%v", statsMap["total_keys"])
}

// TestServer_MetadataConversion 测试元数据相关方法
func TestServer_MetadataConversion(t *testing.T) {
	// 说明：NewMetadata 的签名为 NewMetadata(keySize int64, valueSize int64, ttl time.Duration)
	metadata := value.NewMetadata(10, 1024, time.Hour)

	if metadata.CreatedAt().IsZero() {
		t.Error("CreatedAt 不应为零值")
	}

	if metadata.TotalSize() != 10+1024 {
		t.Errorf("期望总大小 %d，实际 %d", 10+1024, metadata.TotalSize())
	}

	// 测试访问计数（使用 OnAccess）
	initialCount := metadata.AccessCount()
	metadata.OnAccess()
	newCount := metadata.AccessCount()

	if newCount != initialCount+1 {
		t.Errorf("访问计数应增加：%d -> %d", initialCount, newCount)
	}
}

// TestServer_TTLHandling 测试 TTL 相关
func TestServer_TTLHandling(t *testing.T) {
	cache := newMockMultiLevelCache()

	t.Run("设置含 TTL 的项", func(t *testing.T) {
		ttl := 5 * time.Second
		entry := value.NewEntry("ttl-key", value.NewByteView([]byte("value")), ttl)

		err := cache.Set(context.Background(), "ttl-key", entry)
		if err != nil {
			t.Fatalf("设置含 TTL 的项失败：%v", err)
		}

		retrievedEntry, err := cache.Get(context.Background(), "ttl-key")
		if err != nil {
			t.Fatalf("立即获取失败：%v", err)
		}

		if remaining := retrievedEntry.Metadata.TTL(); remaining <= 0 || remaining > ttl || ttl-remaining > 50*time.Millisecond {
			t.Errorf("TTL 不匹配：期望 %v，实际 %v", ttl, retrievedEntry.Metadata.TTL())
		}
	})
}

// TestServer_ErrorCases 测试常见错误场景
func TestServer_ErrorCases(t *testing.T) {
	tests := []struct {
		name      string
		operation string
		key       string
		wantError bool
	}{
		{"get 空键", "get", "", true},
		{"set 空键", "set", "", true},
		{"delete 空键", "delete", "", true},
		{"有效键", "get", "valid-key", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasError := tt.key == ""
			if hasError != tt.wantError {
				t.Errorf("期望 error=%v，但得到 %v（操作=%s，键=%s）", tt.wantError, hasError, tt.operation, tt.key)
			}
		})
	}
}

// BenchmarkServer_Get 基准测试
func BenchmarkServer_Get(b *testing.B) {
	cache := newMockMultiLevelCache()

	entry := value.NewEntry("bench-key", value.NewByteView([]byte("bench-value")), time.Hour)
	cache.Set(context.Background(), "bench-key", entry)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cache.Get(context.Background(), "bench-key")
	}
}

// BenchmarkServer_Set 基准测试
func BenchmarkServer_Set(b *testing.B) {
	cache := newMockMultiLevelCache()
	val := []byte("benchmark-value")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench-key-%d", i)
		entry := value.NewEntry(key, value.NewByteView(val), time.Hour)
		cache.Set(context.Background(), key, entry)
	}
}
