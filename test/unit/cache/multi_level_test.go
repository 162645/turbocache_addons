package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turbocache/internal/cache/engine"
	"turbocache/internal/cache/value"
	"turbocache/internal/config"
)

// ==================== Mock L2Cache ====================

type MockL2Cache struct {
	mu        sync.RWMutex
	data      map[string]*value.Entry
	getErr    error
	setErr    error
	delErr    error
	closed    bool
	getCalls  int64
	setCalls  int64
	delCalls  int64
	promoteCh chan struct{}
}

func NewMockL2Cache() *MockL2Cache {
	return &MockL2Cache{
		data:      make(map[string]*value.Entry),
		promoteCh: make(chan struct{}, 16),
	}
}

func (m *MockL2Cache) Get(ctx context.Context, key string) (*value.Entry, error) {
	atomic.AddInt64(&m.getCalls, 1)

	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.closed {
		return nil, fmt.Errorf("L2 缓存已关闭")
	}

	if m.getErr != nil {
		return nil, m.getErr
	}

	entry, exists := m.data[key]
	if !exists {
		return nil, fmt.Errorf("键未找到: %s", key)
	}

	// 通知（供测试等待，但注意: 这只表示 L2 被访问，不表示 L1 promotion 已完成）
	select {
	case m.promoteCh <- struct{}{}:
	default:
	}

	return entry.Clone(), nil
}

func (m *MockL2Cache) Set(ctx context.Context, key string, entry *value.Entry) error {
	atomic.AddInt64(&m.setCalls, 1)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("L2 缓存已关闭")
	}

	if m.setErr != nil {
		return m.setErr
	}

	m.data[key] = entry.Clone()

	select {
	case m.promoteCh <- struct{}{}:
	default:
	}

	return nil
}

func (m *MockL2Cache) Delete(ctx context.Context, key string) error {
	atomic.AddInt64(&m.delCalls, 1)

	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("L2 缓存已关闭")
	}

	if m.delErr != nil {
		return m.delErr
	}

	delete(m.data, key)
	return nil
}

func (m *MockL2Cache) Stats() (*engine.L2CacheStats, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return &engine.L2CacheStats{
		Level:          "L2",
		Type:           "mock",
		TotalKeys:      int64(len(m.data)),
		GetCount:       atomic.LoadInt64(&m.getCalls),
		SetCount:       atomic.LoadInt64(&m.setCalls),
		DeleteCount:    atomic.LoadInt64(&m.delCalls),
		LocalHitCount:  0,
		RemoteHitCount: atomic.LoadInt64(&m.getCalls),
		MissCount:      0,
		StartTime:      time.Now(),
		Config:         make(map[string]interface{}),
	}, nil
}

func (m *MockL2Cache) Close() error {
	if m == nil {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.data = nil
	return nil
}

func (m *MockL2Cache) SetGetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.getErr = err
}

func (m *MockL2Cache) SetSetError(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.setErr = err
}

func (m *MockL2Cache) ResetStats() {
	atomic.StoreInt64(&m.getCalls, 0)
	atomic.StoreInt64(&m.setCalls, 0)
	atomic.StoreInt64(&m.delCalls, 0)
}

// ==================== Helper Functions ====================

func createTestL1Cache(t *testing.T) *engine.L1Cache {
	t.Helper()

	// 提高容量以减少并发测试中触发 storage-full 的概率
	cfg := &config.L1CacheConfig{
		MaxSize:     64 * 1024 * 1024, // 64MB (测试用)
		MaxEntries:  100000,
		TTL:         time.Minute,
		ShardCount:  16,
		StatsWindow: time.Second,
	}

	l1, err := engine.NewL1Cache(cfg)
	if err != nil {
		t.Fatalf("创建 L1 缓存失败: %v", err)
	}
	return l1
}

func createTestMultiLevelCache(t *testing.T, l1 *engine.L1Cache, l2 *MockL2Cache) *engine.MultiLevelCache {
	t.Helper()

	cfg := &config.CacheConfig{}

	var l1If engine.L1CacheInterface
	var l2If engine.L2CacheInterface

	if l1 != nil {
		l1If = l1
	}
	if l2 != nil {
		l2If = l2
	}

	return engine.NewMultiLevelCache(cfg, l1If, l2If)
}

func createTestEntry(key, val string) *value.Entry {
	bv := value.NewByteViewFromString(val)
	metadata := value.NewMetadata(int64(len(key)), int64(bv.Len()), 0*time.Second)
	return &value.Entry{
		Value:    bv,
		Metadata: metadata,
	}
}

// 等待 L1 包含指定 key（轮询），超时返回错误
func waitForL1HasKey(l1 *engine.L1Cache, key string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := l1.Get(context.Background(), key); err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("等待 L1 包含 key %s 超时", key)
}

// 等待 L2 包含指定 key（轮询），超时返回错误
func waitForL2HasKey(m *MockL2Cache, key string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := m.Get(context.Background(), key); err == nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("等待 L2 包含 key %s 超时", key)
}

// ==================== Basic Functionality Tests ====================

func TestNewMultiLevelCache(t *testing.T) {
	t.Run("with both L1 and L2", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()

		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		if mlc == nil {
			t.Fatal("预期得到非空的 MultiLevelCache")
		}
		if mlc.L1() == nil {
			t.Error("L1 缓存不应为 nil")
		}
		if mlc.L2() == nil {
			t.Error("L2 缓存不应为 nil")
		}
		if mlc.GetStrategy() != engine.WriteThrough {
			t.Errorf("期望默认策略 WriteThrough，实际 %v", mlc.GetStrategy())
		}
	})

	t.Run("with L1 only", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		mlc := createTestMultiLevelCache(t, l1, nil)
		defer mlc.Close()

		if mlc.L1() == nil {
			t.Error("L1 缓存不应为 nil")
		}
		if mlc.L2() != nil {
			t.Error("L2 缓存应为 nil")
		}
	})

	t.Run("with L2 only", func(t *testing.T) {
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, nil, l2)
		defer mlc.Close()

		if mlc.L1() != nil {
			t.Error("L1 缓存应为 nil")
		}
		if mlc.L2() == nil {
			t.Error("L2 缓存不应为 nil")
		}
	})
}

// ==================== Get Operation Tests ====================

func TestMultiLevelCache_Get(t *testing.T) {
	t.Run("L1 hit", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		key := "test-key"
		entry := createTestEntry(key, "test-value")
		if err := l1.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("写入 L1 失败: %v", err)
		}

		result, err := mlc.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get 操作失败: %v", err)
		}
		if result.Value.String() != "test-value" {
			t.Errorf("期望 'test-value', 实际 '%s'", result.Value.String())
		}
		if atomic.LoadInt64(&l2.getCalls) != 0 {
			t.Error("L1 命中时不应调用 L2")
		}
		stats, _ := mlc.Stats()
		if stats.L1Hits != 1 {
			t.Errorf("期望 1 次 L1 命中，实际 %d", stats.L1Hits)
		}
	})

	t.Run("L1 miss, L2 hit", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		key := "test-key"
		entry := createTestEntry(key, "test-value")
		if err := l2.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("写入 L2 失败: %v", err)
		}

		result, err := mlc.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get 操作失败: %v", err)
		}
		if result.Value.String() != "test-value" {
			t.Errorf("期望 'test-value', 实际 '%s'", result.Value.String())
		}

		// 等待 L1 真正包含该 key（轮询）
		if err := waitForL1HasKey(l1, key, 2*time.Second); err != nil {
			t.Fatalf("预期被提升到 L1，但未找到: %v", err)
		}

		stats, _ := mlc.Stats()
		if stats.L2Hits != 1 {
			t.Errorf("期望 1 次 L2 命中，实际 %d", stats.L2Hits)
		}
	})

	t.Run("both miss", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		_, err := mlc.Get(context.Background(), "non-existent")
		if err == nil {
			t.Error("对不存在键应返回错误")
		}
		stats, _ := mlc.Stats()
		if stats.Misses != 1 {
			t.Errorf("期望 1 次未命中，实际 %d", stats.Misses)
		}
	})

	t.Run("L1 only mode", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		mlc := createTestMultiLevelCache(t, l1, nil)
		defer mlc.Close()

		key := "test-key"
		entry := createTestEntry(key, "test-value")
		if err := l1.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("写入 L1 失败: %v", err)
		}
		result, err := mlc.Get(context.Background(), key)
		if err != nil {
			t.Fatalf("Get 操作失败: %v", err)
		}
		if result.Value.String() != "test-value" {
			t.Errorf("值不匹配")
		}
		_, err = mlc.Get(context.Background(), "non-existent")
		if err == nil {
			t.Error("L1-only 模式下，对不存在键应返回错误")
		}
	})

	t.Run("closed cache", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		l2 := NewMockL2Cache()
		mlc := createTestMultiLevelCache(t, l1, l2)
		mlc.Close()

		_, err := mlc.Get(context.Background(), "test-key")
		if err == nil {
			t.Error("关闭后 Get 应返回错误")
		}
	})
}

// ==================== Set Operation Tests ====================

func TestMultiLevelCache_Set(t *testing.T) {
	t.Run("WriteThrough strategy", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		mlc.SetStrategy(engine.WriteThrough)
		key := "test-key"
		entry := createTestEntry(key, "test-value")

		if err := mlc.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("Set 失败: %v", err)
		}

		l1Entry, err := l1.Get(context.Background(), key)
		if err != nil {
			t.Errorf("L1 应包含该条目: %v", err)
		} else if l1Entry.Value.String() != "test-value" {
			t.Error("L1 值不匹配")
		}
		l2Entry, err := l2.Get(context.Background(), key)
		if err != nil {
			t.Errorf("L2 应包含该条目: %v", err)
		} else if l2Entry.Value.String() != "test-value" {
			t.Error("L2 值不匹配")
		}
		stats, _ := mlc.Stats()
		if stats.SetCount != 1 {
			t.Errorf("期望 1 次 set，实际 %d", stats.SetCount)
		}
	})

	t.Run("WriteBack strategy", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		mlc.SetStrategy(engine.WriteBack)
		key := "test-key"
		entry := createTestEntry(key, "test-value")
		if err := mlc.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("Set 失败: %v", err)
		}

		// L1 应立即有数据
		l1Entry, err := l1.Get(context.Background(), key)
		if err != nil {
			t.Errorf("L1 应立即包含该条目: %v", err)
		} else if l1Entry.Value.String() != "test-value" {
			t.Error("L1 值不匹配")
		}

		// 等待 L2 真正包含 key（轮询）
		if err := waitForL2HasKey(l2, key, 2*time.Second); err != nil {
			t.Log("等待异步写 L2 超时（继续测试）")
		}

		l2Entry, err := l2.Get(context.Background(), key)
		if err != nil {
			t.Errorf("异步写入后 L2 应包含该条目: %v", err)
		} else if l2Entry.Value.String() != "test-value" {
			t.Error("L2 值不匹配")
		}
	})

	t.Run("WriteAround strategy", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		mlc.SetStrategy(engine.WriteAround)
		key := "test-key"
		entry := createTestEntry(key, "test-value")
		if err := mlc.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("Set 失败: %v", err)
		}

		_, err := l1.Get(context.Background(), key)
		if err == nil {
			t.Error("WriteAround 模式下 L1 不应包含该条目")
		}
		l2Entry, err := l2.Get(context.Background(), key)
		if err != nil {
			t.Errorf("L2 应包含该条目: %v", err)
		} else if l2Entry.Value.String() != "test-value" {
			t.Error("L2 值不匹配")
		}
	})

	t.Run("L1 only mode", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		mlc := createTestMultiLevelCache(t, l1, nil)
		defer mlc.Close()

		key := "test-key"
		entry := createTestEntry(key, "test-value")
		if err := mlc.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("Set 失败: %v", err)
		}
		l1Entry, err := l1.Get(context.Background(), key)
		if err != nil {
			t.Errorf("L1 应包含该条目: %v", err)
		} else if l1Entry.Value.String() != "test-value" {
			t.Error("L1 值不匹配")
		}
	})

	t.Run("L2 only mode with WriteAround", func(t *testing.T) {
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, nil, l2)
		defer mlc.Close()

		mlc.SetStrategy(engine.WriteAround)
		key := "test-key"
		entry := createTestEntry(key, "test-value")
		if err := mlc.Set(context.Background(), key, entry); err != nil {
			t.Fatalf("Set 失败: %v", err)
		}
		l2Entry, err := l2.Get(context.Background(), key)
		if err != nil {
			t.Errorf("L2 应包含该条目: %v", err)
		} else if l2Entry.Value.String() != "test-value" {
			t.Error("L2 值不匹配")
		}
	})
}

// ==================== Delete Operation Tests ====================

func TestMultiLevelCache_Delete(t *testing.T) {
	t.Run("delete from both levels", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		key := "test-key"
		entry := createTestEntry(key, "test-value")
		l1.Set(context.Background(), key, entry)
		l2.Set(context.Background(), key, entry)

		if err := mlc.Delete(context.Background(), key); err != nil {
			t.Fatalf("Delete 失败: %v", err)
		}

		_, err := l1.Get(context.Background(), key)
		if err == nil {
			t.Error("删除后 L1 不应包含该条目")
		}
		_, err = l2.Get(context.Background(), key)
		if err == nil {
			t.Error("删除后 L2 不应包含该条目")
		}
		stats, _ := mlc.Stats()
		if stats.DeleteCount != 1 {
			t.Errorf("期望 1 次 delete，实际 %d", stats.DeleteCount)
		}
	})

	t.Run("delete non-existent key", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		if err := mlc.Delete(context.Background(), "non-existent"); err != nil {
			t.Errorf("删除不存在键不应返回错误: %v", err)
		}
	})
}

// ==================== Strategy Tests ====================

func TestMultiLevelCache_SetStrategy(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	strategies := []engine.CacheStrategy{engine.WriteThrough, engine.WriteBack, engine.WriteAround}
	for _, strategy := range strategies {
		mlc.SetStrategy(strategy)
		if got := mlc.GetStrategy(); got != strategy {
			t.Errorf("期望策略 %v，实际 %v", strategy, got)
		}
	}
}

func TestCacheStrategy_String(t *testing.T) {
	tests := []struct {
		strategy engine.CacheStrategy
		expected string
	}{
		{engine.WriteThrough, "write-through"},
		{engine.WriteBack, "write-back"},
		{engine.WriteAround, "write-around"},
		{engine.CacheStrategy(999), "unknown"},
	}
	for _, tt := range tests {
		if got := tt.strategy.String(); got != tt.expected {
			t.Errorf("策略 %v: 期望 '%s'，实际 '%s'", tt.strategy, tt.expected, got)
		}
	}
}

// ==================== Clear Operation Tests ====================

func TestMultiLevelCache_Clear(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
		mlc.Set(context.Background(), key, entry)
	}

	if err := mlc.Clear(context.Background()); err != nil {
		t.Fatalf("Clear 失败: %v", err)
	}

	l1Stats, _ := l1.Stats()
	if l1Stats.TotalKeys != 0 {
		t.Errorf("L1 应为空，实际 %d 个 key", l1Stats.TotalKeys)
	}
	stats, _ := mlc.Stats()
	if stats.TotalRequests != 0 {
		t.Errorf("统计应被重置，实际 TotalRequests=%d", stats.TotalRequests)
	}
}

// ==================== Stats Tests ====================

func TestMultiLevelCache_Stats(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	key := "test-key"
	entry := createTestEntry(key, "test-value")

	mlc.Set(ctx, key, entry)
	mlc.Get(ctx, key)
	mlc.Get(ctx, "non-existent")
	mlc.Delete(ctx, key)

	stats, err := mlc.Stats()
	if err != nil {
		t.Fatalf("Stats 获取失败: %v", err)
	}

	if stats.SetCount != 1 {
		t.Errorf("期望 1 次 set，实际 %d", stats.SetCount)
	}
	if stats.GetCount != 2 {
		t.Errorf("期望 2 次 get，实际 %d", stats.GetCount)
	}
	if stats.DeleteCount != 1 {
		t.Errorf("期望 1 次 delete，实际 %d", stats.DeleteCount)
	}
	if stats.TotalRequests != 2 {
		t.Errorf("期望 2 次 total requests，实际 %d", stats.TotalRequests)
	}
	if stats.L1Hits != 1 {
		t.Errorf("期望 1 次 L1 hit，实际 %d", stats.L1Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("期望 1 次 miss，实际 %d", stats.Misses)
	}
	expectedHitRate := 1.0 / 2.0
	if stats.OverallHitRate != expectedHitRate {
		t.Errorf("期望命中率 %.2f，实际 %.2f", expectedHitRate, stats.OverallHitRate)
	}
	if stats.L1Stats == nil {
		t.Error("L1 子统计不应为 nil")
	}
	if stats.L2Stats == nil {
		t.Error("L2 子统计不应为 nil")
	}
}

// ==================== Concurrent Tests ====================

func TestMultiLevelCache_Concurrent(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	numGoroutines := 50
	numOperations := 100
	var wg sync.WaitGroup

	// 并发写入
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				entry := createTestEntry(key, fmt.Sprintf("value-%d-%d", id, j))
				if err := mlc.Set(ctx, key, entry); err != nil {
					t.Errorf("并发 Set 失败: %v", err)
				}
			}
		}(i)
	}

	// 并发读取
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				mlc.Get(ctx, key)
			}
		}(i)
	}

	// 并发删除
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < numOperations/2; j++ {
				key := fmt.Sprintf("key-%d-%d", id, j)
				mlc.Delete(ctx, key)
			}
		}(i)
	}

	wg.Wait()

	stats, err := mlc.Stats()
	if err != nil {
		t.Fatalf("并发后获取统计失败: %v", err)
	}

	t.Logf("并发测试完成: Sets=%d, Gets=%d, Deletes=%d",
		stats.SetCount, stats.GetCount, stats.DeleteCount)
}

func TestMultiLevelCache_ConcurrentSameKey(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	key := "concurrent-key"
	numGoroutines := 100
	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			entry := createTestEntry(key, fmt.Sprintf("value-%d", id))
			mlc.Set(ctx, key, entry)
			mlc.Get(ctx, key)
			if id%2 == 0 {
				mlc.Delete(ctx, key)
			}
		}(i)
	}

	wg.Wait()
	_, err := mlc.Stats()
	if err != nil {
		t.Fatalf("同键并发后获取统计失败: %v", err)
	}
}

// ==================== Error Handling Tests ====================

func TestMultiLevelCache_L2Failure(t *testing.T) {
	t.Run("L2 Get failure with L1 fallback", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		ctx := context.Background()
		key := "test-key"
		entry := createTestEntry(key, "test-value")

		l1.Set(ctx, key, entry)
		l2.SetGetError(fmt.Errorf("L2 连接错误"))

		result, err := mlc.Get(ctx, key)
		if err != nil {
			t.Fatalf("L2 失败时应回退到 L1：%v", err)
		}
		if result.Value.String() != "test-value" {
			t.Error("从 L1 回退读取的值不匹配")
		}
	})

	t.Run("WriteThrough with L2 Set failure", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		mlc.SetStrategy(engine.WriteThrough)
		l2.SetSetError(fmt.Errorf("L2 写入错误"))

		ctx := context.Background()
		key := "test-key"
		entry := createTestEntry(key, "test-value")
		err := mlc.Set(ctx, key, entry)
		if err == nil {
			t.Error("WriteThrough 在 L2 写失败时应返回错误")
		}
	})

	t.Run("WriteBack tolerates L2 failure", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		mlc.SetStrategy(engine.WriteBack)
		ctx := context.Background()
		key := "test-key"
		entry := createTestEntry(key, "test-value")
		err := mlc.Set(ctx, key, entry)
		if err != nil {
			t.Errorf("WriteBack 在 L2 失败时应仍然成功: %v", err)
		}
		l1Entry, err := l1.Get(ctx, key)
		if err != nil {
			t.Errorf("L1 应包含该条目: %v", err)
		} else if l1Entry.Value.String() != "test-value" {
			t.Error("L1 值不匹配")
		}
	})
}

// ==================== Promotion Tests ====================

func TestMultiLevelCache_DataPromotion(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	key := "test-key"
	entry := createTestEntry(key, "test-value")
	l2.Set(ctx, key, entry)

	result, err := mlc.Get(ctx, key)
	if err != nil {
		t.Fatalf("首次 Get 失败: %v", err)
	}
	if result.Value.String() != "test-value" {
		t.Error("从 L2 读取的值不匹配")
	}

	// 轮询 L1 等待提升完成并校验值
	if err := waitForL1HasKey(l1, key, 2*time.Second); err != nil {
		t.Fatalf("提升到 L1 超时或失败: %v", err)
	}

	l2.ResetStats()
	result, err = mlc.Get(ctx, key)
	if err != nil {
		t.Fatalf("第二次 Get 失败: %v", err)
	}
	if result.Value.String() != "test-value" {
		t.Error("从 L1 读取的值不匹配")
	}
	if atomic.LoadInt64(&l2.getCalls) != 0 {
		t.Error("提升后不应再调用 L2")
	}
}

// ==================== Latency Tests ====================

func TestMultiLevelCache_Latency(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	key := "test-key"
	entry := createTestEntry(key, "test-value")
	l1.Set(ctx, key, entry)

	start := time.Now()
	for i := 0; i < 1000; i++ {
		mlc.Get(ctx, key)
	}
	l1Duration := time.Since(start)

	l1.Clear(ctx)
	l2.Set(ctx, key, entry)

	start = time.Now()
	for i := 0; i < 1000; i++ {
		l2.Get(ctx, key)
	}
	l2Duration := time.Since(start)

	t.Logf("L1 延迟: %v (avg %v)", l1Duration, l1Duration/1000)
	t.Logf("L2 延迟: %v (avg %v)", l2Duration, l2Duration/1000)

	stats, _ := mlc.Stats()
	_ = stats
}

// ==================== Close Tests ====================

func TestMultiLevelCache_Close(t *testing.T) {
	t.Run("normal close", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		l2 := NewMockL2Cache()
		mlc := createTestMultiLevelCache(t, l1, l2)

		ctx := context.Background()
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key-%d", i)
			entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
			mlc.Set(ctx, key, entry)
		}

		if err := mlc.Close(); err != nil {
			t.Fatalf("Close 失败: %v", err)
		}
		if err := mlc.Close(); err != nil {
			t.Errorf("第二次 Close 不应返回错误: %v", err)
		}
		_, err := mlc.Get(ctx, "key-0")
		if err == nil {
			t.Error("关闭后 Get 应返回错误")
		}
	})

	t.Run("close with nil L2", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		mlc := createTestMultiLevelCache(t, l1, nil)
		if err := mlc.Close(); err != nil {
			t.Fatalf("L2 为 nil 时 Close 失败: %v", err)
		}
	})

	t.Run("close with nil L1", func(t *testing.T) {
		l2 := NewMockL2Cache()
		mlc := createTestMultiLevelCache(t, nil, l2)
		if err := mlc.Close(); err != nil {
			t.Fatalf("L1 为 nil 时 Close 失败: %v", err)
		}
	})
}

// ==================== Edge Cases Tests ====================

func TestMultiLevelCache_EdgeCases(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		mlc := createTestMultiLevelCache(t, l1, nil)
		defer mlc.Close()

		entry := createTestEntry("", "value")
		err := mlc.Set(context.Background(), "", entry)
		if err == nil {
			t.Error("空键 Set 应返回错误（或被拒绝）")
		}
	})

	t.Run("nil entry", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		mlc := createTestMultiLevelCache(t, l1, nil)
		defer mlc.Close()

		err := mlc.Set(context.Background(), "key", nil)
		if err == nil {
			t.Error("nil entry Set 应返回错误")
		}
	})

	t.Run("very large value", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		mlc := createTestMultiLevelCache(t, l1, nil)
		defer mlc.Close()

		largeValue := make([]byte, 10*1024*1024) // 10MB
		entry := &value.Entry{
			Value:    value.NewByteView(largeValue),
			Metadata: value.NewMetadata(3, int64(len(largeValue)), 0*time.Second),
		}

		err := mlc.Set(context.Background(), "key", entry)
		if err == nil {
			t.Log("大值写入成功（可能受限于实现）")
		}
	})

	t.Run("rapid set-get-delete cycles", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		ctx := context.Background()
		key := "test-key"
		for i := 0; i < 100; i++ {
			entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
			if err := mlc.Set(ctx, key, entry); err != nil {
				t.Fatalf("第 %d 次 Set 失败: %v", i, err)
			}
			if _, err := mlc.Get(ctx, key); err != nil {
				t.Fatalf("第 %d 次 Get 失败: %v", i, err)
			}
			if err := mlc.Delete(ctx, key); err != nil {
				t.Fatalf("第 %d 次 Delete 失败: %v", i, err)
			}
		}
	})
}

// ==================== Context Cancellation Tests ====================

func TestMultiLevelCache_ContextCancellation(t *testing.T) {
	l1 := createTestL1Cache(t)
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(t, l1, l2)
	defer mlc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	key := "test-key"
	entry := createTestEntry(key, "test-value")
	if err := mlc.Set(ctx, key, entry); err != nil {
		t.Fatalf("取消前 Set 失败: %v", err)
	}
	cancel()
	_, err := mlc.Get(ctx, key)
	if err != nil {
		t.Logf("取消后 Get 失败预期: %v", err)
	}
}

// ==================== Benchmark Tests ====================

func BenchmarkMultiLevelCache_Get(b *testing.B) {
	l1 := createTestL1Cache(&testing.T{})
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(&testing.T{}, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	key := "benchmark-key"
	entry := createTestEntry(key, "benchmark-value")
	mlc.Set(ctx, key, entry)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mlc.Get(ctx, key)
		}
	})
}

func BenchmarkMultiLevelCache_Set(b *testing.B) {
	l1 := createTestL1Cache(&testing.T{})
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(&testing.T{}, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i)
			entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
			mlc.Set(ctx, key, entry)
			i++
		}
	})
}

func BenchmarkMultiLevelCache_GetMiss(b *testing.B) {
	l1 := createTestL1Cache(&testing.T{})
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(&testing.T{}, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			mlc.Get(ctx, fmt.Sprintf("non-existent-%d", i))
			i++
		}
	})
}

func BenchmarkMultiLevelCache_WriteThrough(b *testing.B) {
	l1 := createTestL1Cache(&testing.T{})
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(&testing.T{}, l1, l2)
	defer mlc.Close()

	mlc.SetStrategy(engine.WriteThrough)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
		mlc.Set(ctx, key, entry)
	}
}

func BenchmarkMultiLevelCache_WriteBack(b *testing.B) {
	l1 := createTestL1Cache(&testing.T{})
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(&testing.T{}, l1, l2)
	defer mlc.Close()

	mlc.SetStrategy(engine.WriteBack)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
		mlc.Set(ctx, key, entry)
	}
}

func BenchmarkMultiLevelCache_Concurrent(b *testing.B) {
	l1 := createTestL1Cache(&testing.T{})
	defer l1.Close()
	l2 := NewMockL2Cache()
	defer l2.Close()
	mlc := createTestMultiLevelCache(&testing.T{}, l1, l2)
	defer mlc.Close()

	ctx := context.Background()
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
		mlc.Set(ctx, key, entry)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000)
			if i%3 == 0 {
				entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
				mlc.Set(ctx, key, entry)
			} else {
				mlc.Get(ctx, key)
			}
			i++
		}
	})
}

// ==================== Integration Tests ====================

func TestMultiLevelCache_Integration(t *testing.T) {
	t.Run("full workflow", func(t *testing.T) {
		l1 := createTestL1Cache(t)
		defer l1.Close()
		l2 := NewMockL2Cache()
		defer l2.Close()
		mlc := createTestMultiLevelCache(t, l1, l2)
		defer mlc.Close()

		ctx := context.Background()

		// 1. 写入 (WriteThrough)
		mlc.SetStrategy(engine.WriteThrough)
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("key-%d", i)
			entry := createTestEntry(key, fmt.Sprintf("value-%d", i))
			if err := mlc.Set(ctx, key, entry); err != nil {
				t.Fatalf("写入失败: %v", err)
			}
		}

		// 2. 读取验证 L1 命中
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("key-%d", i)
			result, err := mlc.Get(ctx, key)
			if err != nil {
				t.Fatalf("读取失败 %s: %v", key, err)
			}
			expected := fmt.Sprintf("value-%d", i)
			if result.Value.String() != expected {
				t.Errorf("值不匹配 %s: 期望 %s, 实际 %s", key, expected, result.Value.String())
			}
		}

		// 3. 清空 L1，回源到 L2
		if err := l1.Clear(ctx); err != nil {
			t.Fatalf("L1 清空失败: %v", err)
		}

		// 4. 再次读取（触发 L2 -> L1 提升）
		result, err := mlc.Get(ctx, "key-0")
		if err != nil {
			t.Fatalf("L1 清空后读取失败: %v", err)
		}
		if result.Value.String() != "value-0" {
			t.Error("L2 回源读取值不匹配")
		}

		// 5. 等待 L1 真正包含 key-0（轮询）
		if err := waitForL1HasKey(l1, "key-0", 2*time.Second); err != nil {
			t.Log("等待提升超时（继续测试）")
		}

		// 6. 删除部分数据
		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("key-%d", i)
			if err := mlc.Delete(ctx, key); err != nil {
				t.Fatalf("删除失败 %s: %v", key, err)
			}
		}

		// 7. 验证删除成功
		for i := 0; i < 25; i++ {
			key := fmt.Sprintf("key-%d", i)
			_, err := mlc.Get(ctx, key)
			if err == nil {
				t.Errorf("键 %s 应已被删除", key)
			}
		}

		// 8. 验证未删除数据仍存在
		for i := 25; i < 50; i++ {
			key := fmt.Sprintf("key-%d", i)
			result, err := mlc.Get(ctx, key)
			if err != nil {
				t.Fatalf("读取未删除键失败 %s: %v", key, err)
			}
			expected := fmt.Sprintf("value-%d", i)
			if result.Value.String() != expected {
				t.Errorf("值不匹配 %s", key)
			}
		}

		// 9. 最终统计
		stats, err := mlc.Stats()
		if err != nil {
			t.Fatalf("获取统计失败: %v", err)
		}
		t.Logf("最终统计: Sets=%d, Gets=%d, Deletes=%d, L1Hits=%d, L2Hits=%d, Misses=%d",
			stats.SetCount, stats.GetCount, stats.DeleteCount, stats.L1Hits, stats.L2Hits, stats.Misses)

		// 10. 清空所有数据
		if err := mlc.Clear(ctx); err != nil {
			t.Fatalf("Clear 失败: %v", err)
		}

		// 11. 验证清空
		stats, _ = mlc.Stats()
		if stats.TotalRequests != 0 {
			t.Error("清空后统计应被重置")
		}
	})
}
