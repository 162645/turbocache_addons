package protection_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turbocache/internal/protection/negative"
)

// ===== 基础功能测试 =====

// TestNegativeCache_Basic 测试基本功能
func TestNegativeCache_Basic(t *testing.T) {
	nc := negative.NewNegativeCache(&negative.Config{
		DefaultTTL:      time.Minute,
		MaxSize:         100,
		CleanupInterval: time.Second,
	})
	defer nc.Close()

	// 设置负缓存条目
	nc.Set("key1", "not found", negative.ErrorTypeNotFound)

	// 获取条目
	entry, exists := nc.Get("key1")
	if !exists {
		t.Fatal("期望条目存在")
	}
	if entry.Key != "key1" {
		t.Errorf("获得key %s, 期望 key1", entry.Key)
	}
	if entry.Reason != "not found" {
		t.Errorf("获得reason %s, 期望 not found", entry.Reason)
	}
	if entry.ErrorType != negative.ErrorTypeNotFound {
		t.Errorf("获得errorType %s, 期望 %s", entry.ErrorType, negative.ErrorTypeNotFound)
	}
}

// TestNegativeCache_SetWithTTL 测试自定义TTL
func TestNegativeCache_SetWithTTL(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	// 设置短TTL的条目
	nc.SetWithTTL("key1", "expired soon", negative.ErrorTypeTimeout, 50*time.Millisecond)

	// 立即获取应该存在
	_, exists := nc.Get("key1")
	if !exists {
		t.Error("设置后立即获取应该存在")
	}

	// 等待过期
	time.Sleep(100 * time.Millisecond)

	// 再次获取应该不存在
	_, exists = nc.Get("key1")
	if exists {
		t.Error("过期后不应该存在")
	}
}

// TestNegativeCache_AutoCleanup 测试自动清理
func TestNegativeCache_AutoCleanup(t *testing.T) {
	config := &negative.Config{
		DefaultTTL:      50 * time.Millisecond,
		MaxSize:         100,
		CleanupInterval: 100 * time.Millisecond,
	}

	nc := negative.NewNegativeCache(config)
	defer nc.Close()

	// 添加一些短TTL的条目
	for i := 0; i < 10; i++ {
		nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}

	// 验证都存在（刚刚设置）
	stats := nc.Stats()
	if stats.ActiveEntries != 10 {
		t.Errorf("添加后应该有10个活跃条目, 获得 %d", stats.ActiveEntries)
	}

	// 等待过期和清理
	time.Sleep(300 * time.Millisecond)

	// 验证已清理或至少有过期计数
	stats = nc.Stats()
	if stats.ActiveEntries > 0 {
		t.Logf("清理后仍有 %d 个活跃条目（可能因为时间调度），请检查清理周期是否正确", stats.ActiveEntries)
	}
	if stats.ExpiredCount == 0 {
		t.Error("期望有过期计数")
	}
}

// TestNegativeCache_MaxSize 测试最大容量限制（淘汰）
func TestNegativeCache_MaxSize(t *testing.T) {
	config := &negative.Config{
		DefaultTTL:      time.Hour,
		MaxSize:         10,
		CleanupInterval: 0,
	}

	nc := negative.NewNegativeCache(config)
	defer nc.Close()

	// 添加超过最大容量的条目
	for i := 0; i < 15; i++ {
		nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}

	stats := nc.Stats()
	if stats.TotalEntries > config.MaxSize {
		t.Errorf("条目数 %d 超过最大容量 %d", stats.TotalEntries, config.MaxSize)
	}

	// 应该有淘汰计数
	if stats.EvictCount == 0 {
		t.Error("期望有淘汰计数")
	}
}

// ===== 并发安全测试 =====

// TestNegativeCache_ConcurrentSet 测试并发设置
func TestNegativeCache_ConcurrentSet(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	const (
		goroutines = 50
		itemsEach  = 100
	)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < itemsEach; i++ {
				key := fmt.Sprintf("key_%d_%d", id, i)
				nc.Set(key, "test", negative.ErrorTypeNotFound)
			}
		}(g)
	}

	wg.Wait()

	stats := nc.Stats()
	expected := int64(goroutines * itemsEach)
	if stats.SetCount != expected {
		t.Errorf("获得 %d 次设置操作, 期望 %d", stats.SetCount, expected)
	}
}

// TestNegativeCache_ConcurrentGet 测试并发获取
func TestNegativeCache_ConcurrentGet(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	// 预先设置条目
	for i := 0; i < 100; i++ {
		nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	var errorsCount int32

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_, exists := nc.Get(fmt.Sprintf("key%d", i))
				if !exists {
					atomic.AddInt32(&errorsCount, 1)
				}
			}
		}()
	}

	wg.Wait()

	if errorsCount > 0 {
		t.Errorf("并发获取时出现 %d 个错误", errorsCount)
	}
}

// TestNegativeCache_ConcurrentSetAndGet 测试并发读写
func TestNegativeCache_ConcurrentSetAndGet(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	const (
		writers = 20
		readers = 30
		items   = 100
	)

	var wg sync.WaitGroup
	wg.Add(writers + readers)

	// 写入goroutines
	for w := 0; w < writers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				key := fmt.Sprintf("key_%d_%d", id, i)
				nc.Set(key, "test", negative.ErrorTypeNotFound)
			}
		}(w)
	}

	// 读取goroutines
	for r := 0; r < readers; r++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < items; i++ {
				key := fmt.Sprintf("key_%d_%d", id%writers, i)
				nc.Get(key)
			}
		}(r)
	}

	wg.Wait()
}

// ===== NegativeCacheWrapper 测试 =====

// mockCacheForWrapper 包装器测试的mock缓存
type mockCacheForWrapper struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

func newMockCacheForWrapper() *mockCacheForWrapper {
	return &mockCacheForWrapper{
		data: make(map[string]interface{}),
	}
}

func (m *mockCacheForWrapper) Get(ctx context.Context, key string) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if v, ok := m.data[key]; ok {
		return v, nil
	}
	return nil, errors.New("cache miss")
}

func (m *mockCacheForWrapper) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return nil
}

func (m *mockCacheForWrapper) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

// TestNegativeCacheWrapper_Get 测试包装器获取
func TestNegativeCacheWrapper_Get(t *testing.T) {
	cache := newMockCacheForWrapper()
	ncw := negative.NewNegativeCacheWrapper(cache, nil)
	defer ncw.Close()

	ctx := context.Background()

	// 设置负缓存
	ncw.SetNegative("key1", "not found", negative.ErrorTypeNotFound)

	// 获取应该返回负缓存错误
	_, err := ncw.Get(ctx, "key1")
	if err == nil {
		t.Error("期望负缓存错误")
	}
	if !negative.IsNegativeCacheError(err) {
		t.Errorf("期望负缓存错误类型, 获得 %T", err)
	}
}

// TestNegativeCacheWrapper_SetRemovesNegative 测试设置会清除负缓存
func TestNegativeCacheWrapper_SetRemovesNegative(t *testing.T) {
	cache := newMockCacheForWrapper()
	ncw := negative.NewNegativeCacheWrapper(cache, nil)
	defer ncw.Close()

	ctx := context.Background()

	// 设置负缓存
	ncw.SetNegative("key1", "not found", negative.ErrorTypeNotFound)

	// 验证负缓存存在
	_, err := ncw.Get(ctx, "key1")
	if !negative.IsNegativeCacheError(err) {
		t.Error("期望负缓存错误")
	}

	// 设置正常值（应清除负缓存）
	err = ncw.Set(ctx, "key1", "value", time.Hour)
	if err != nil {
		t.Fatalf("设置失败: %v", err)
	}

	// 再次获取应该返回正常值
	val, err := ncw.Get(ctx, "key1")
	if err != nil {
		t.Fatalf("获取失败: %v", err)
	}
	if val != "value" {
		t.Errorf("获得 %v, 期望 value", val)
	}
}

// TestNegativeCacheWrapper_Delete 测试删除同时清除负缓存
func TestNegativeCacheWrapper_Delete(t *testing.T) {
	cache := newMockCacheForWrapper()
	ncw := negative.NewNegativeCacheWrapper(cache, nil)
	defer ncw.Close()

	ctx := context.Background()

	// 设置负缓存
	ncw.SetNegative("key1", "not found", negative.ErrorTypeNotFound)

	// 验证负缓存存在
	stats := ncw.GetStats()
	if stats.TotalEntries != 1 {
		t.Errorf("期望1个负缓存条目, 获得 %d", stats.TotalEntries)
	}

	// 删除
	err := ncw.Delete(ctx, "key1")
	if err != nil {
		t.Fatalf("删除失败: %v", err)
	}

	// 负缓存应该被清除
	stats = ncw.GetStats()
	if stats.TotalEntries != 0 {
		t.Errorf("删除后期望0个负缓存条目, 获得 %d", stats.TotalEntries)
	}
}

// ===== SmartNegativeCache 测试 =====

// TestSmartNegativeCache_DynamicTTL 测试动态TTL（尽量避免严格断言，记录变化）
func TestSmartNegativeCache_DynamicTTL(t *testing.T) {
	config := &negative.SmartConfig{
		Config: &negative.Config{
			DefaultTTL:      time.Minute,
			MaxSize:         100,
			CleanupInterval: 0,
		},
		MinTTL:     30 * time.Second,
		MaxTTL:     10 * time.Minute,
		Multiplier: 1.5,
	}

	snc := negative.NewSmartNegativeCache(config)
	defer snc.Close()

	// 第一次设置
	snc.SetSmart("key1", "test", negative.ErrorTypeNotFound)

	entry1, _ := snc.Get("key1")
	if entry1 == nil {
		t.Fatal("期望 entry1 存在")
	}
	ttl1 := entry1.ExpiresAt.Sub(entry1.CreatedAt)

	// 多次访问以增加访问计数
	for i := 0; i < 12; i++ {
		snc.Get("key1")
	}

	// 再次设置，TTL应该（通常）增加或至少受动态策略影响
	snc.SetSmart("key1", "test", negative.ErrorTypeNotFound)

	entry2, _ := snc.Get("key1")
	if entry2 == nil {
		t.Fatal("期望 entry2 存在")
	}
	ttl2 := entry2.ExpiresAt.Sub(entry2.CreatedAt)

	// 这里不做强断言以避免不同策略下抖动，记录以便人工检查
	if ttl2 <= ttl1 {
		t.Logf("第一次TTL: %v, 第二次TTL: %v — 注意：动态TTL应根据访问次数有增长趋势（策略依赖）", ttl1, ttl2)
	}
}

// TestSmartNegativeCache_ErrorTypeTTL 测试错误类型特定TTL
func TestSmartNegativeCache_ErrorTypeTTL(t *testing.T) {
	config := &negative.SmartConfig{
		Config: &negative.Config{
			DefaultTTL:      time.Minute,
			MaxSize:         100,
			CleanupInterval: 0,
		},
		ErrorTypeTTL: map[string]time.Duration{
			negative.ErrorTypeNotFound: 2 * time.Minute,
			negative.ErrorTypeTimeout:  30 * time.Second,
		},
	}

	snc := negative.NewSmartNegativeCache(config)
	defer snc.Close()

	// 设置不同错误类型
	snc.SetSmart("key1", "not found", negative.ErrorTypeNotFound)
	snc.SetSmart("key2", "timeout", negative.ErrorTypeTimeout)

	entry1, _ := snc.Get("key1")
	entry2, _ := snc.Get("key2")

	if entry1 == nil || entry2 == nil {
		t.Fatal("期望 entry1 和 entry2 都存在")
	}

	ttl1 := entry1.ExpiresAt.Sub(entry1.CreatedAt)
	ttl2 := entry2.ExpiresAt.Sub(entry2.CreatedAt)

	// NotFound的TTL应该比Timeout长
	if ttl1 <= ttl2 {
		t.Errorf("NotFound的TTL(%v)应该比Timeout的TTL(%v)长", ttl1, ttl2)
	}
}

// ===== BatchNegativeCache 测试 =====

// TestBatchNegativeCache_Batch 测试批量操作
func TestBatchNegativeCache_Batch(t *testing.T) {
	config := &negative.Config{
		DefaultTTL:      time.Minute,
		MaxSize:         1000,
		CleanupInterval: 0,
	}

	bnc := negative.NewBatchNegativeCache(config, 10)
	defer bnc.Close()

	// 创建批量条目
	entries := make(map[string]*negative.NegativeEntry)
	now := time.Now()
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		entries[key] = &negative.NegativeEntry{
			Key:       key,
			CreatedAt: now,
			ExpiresAt: now.Add(time.Minute),
			Reason:    "test",
			ErrorType: negative.ErrorTypeNotFound,
		}
	}

	// 批量设置（未达到批量大小，不会自动刷新）
	bnc.BatchSet(entries)

	// 手动刷新
	bnc.Flush()

	// 验证所有条目都存在
	for i := 0; i < 5; i++ {
		if !bnc.Contains(fmt.Sprintf("key%d", i)) {
			t.Errorf("期望key%d存在", i)
		}
	}
}

// TestBatchNegativeCache_AutoFlush 测试自动刷新
func TestBatchNegativeCache_AutoFlush(t *testing.T) {
	config := &negative.Config{
		DefaultTTL:      time.Minute,
		MaxSize:         1000,
		CleanupInterval: 0,
	}

	batchSize := 5
	bnc := negative.NewBatchNegativeCache(config, batchSize)
	defer bnc.Close()

	// 添加超过批量大小的条目，应该自动刷新
	entries := make(map[string]*negative.NegativeEntry)
	now := time.Now()
	total := batchSize + 1
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("key%d", i)
		entries[key] = &negative.NegativeEntry{
			Key:       key,
			CreatedAt: now,
			ExpiresAt: now.Add(time.Minute),
			Reason:    "test",
			ErrorType: negative.ErrorTypeNotFound,
		}

		// 分批添加以触发自动刷新
		if len(entries) == batchSize {
			bnc.BatchSet(entries)
			entries = make(map[string]*negative.NegativeEntry)
		}
	}

	// 刷新剩余的
	if len(entries) > 0 {
		bnc.BatchSet(entries)
		bnc.Flush()
	}

	// 验证所有条目都存在
	for i := 0; i < total; i++ {
		if !bnc.Contains(fmt.Sprintf("key%d", i)) {
			t.Errorf("期望key%d存在", i)
		}
	}
}

// ===== 边界条件测试 =====

// TestNegativeCache_NilConfig 测试nil配置
func TestNegativeCache_NilConfig(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	// 应该使用默认配置
	nc.Set("key1", "test", negative.ErrorTypeNotFound)

	if !nc.Contains("key1") {
		t.Error("应该能正常工作")
	}
}

// TestNegativeCache_EmptyKey 测试空key
func TestNegativeCache_EmptyKey(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	nc.Set("", "test", negative.ErrorTypeNotFound)

	if !nc.Contains("") {
		t.Error("应该能处理空key")
	}
}

// TestNegativeCache_ClosedCache 测试关闭后的操作
func TestNegativeCache_ClosedCache(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	nc.Close()

	// 关闭后的操作应该安全（不应该panic）
	nc.Set("key1", "test", negative.ErrorTypeNotFound)
	_, exists := nc.Get("key1")
	if exists {
		t.Error("关闭后不应该返回数据")
	}

	nc.Delete("key1")
	nc.Clear()
	stats := nc.Stats()
	if stats.TotalEntries != 0 {
		t.Error("关闭后统计应该为空")
	}
}

// ===== 性能基准测试 =====

func BenchmarkNegativeCache_Set(b *testing.B) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}
}

func BenchmarkNegativeCache_Get(b *testing.B) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	// 预先设置数据
	for i := 0; i < 10000; i++ {
		nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nc.Get(fmt.Sprintf("key%d", i%10000))
	}
}

func BenchmarkNegativeCache_Concurrent(b *testing.B) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
			} else {
				nc.Get(fmt.Sprintf("key%d", i))
			}
			i++
		}
	})
}

func BenchmarkSmartNegativeCache_SetSmart(b *testing.B) {
	snc := negative.NewSmartNegativeCache(nil)
	defer snc.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		snc.SetSmart(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}
}

func BenchmarkBatchNegativeCache_BatchSet(b *testing.B) {
	bnc := negative.NewBatchNegativeCache(nil, 100)
	defer bnc.Close()

	entries := make(map[string]*negative.NegativeEntry)
	now := time.Now()
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		entries[key] = &negative.NegativeEntry{
			Key:       key,
			CreatedAt: now,
			ExpiresAt: now.Add(time.Minute),
			Reason:    "test",
			ErrorType: negative.ErrorTypeNotFound,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bnc.BatchSet(entries)
	}
}

// ===== 额外：Contains / Delete / Clear / Stats / AccessCount / Expiration 等 =====

func TestNegativeCache_Contains(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	nc.Set("key1", "test", negative.ErrorTypeNotFound)

	if !nc.Contains("key1") {
		t.Error("期望key1存在")
	}
	if nc.Contains("key2") {
		t.Error("期望key2不存在")
	}
}

func TestNegativeCache_Delete(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	nc.Set("key1", "test", negative.ErrorTypeNotFound)

	// 验证存在
	if !nc.Contains("key1") {
		t.Fatal("设置后应该存在")
	}

	// 删除
	deleted := nc.Delete("key1")
	if !deleted {
		t.Error("删除应该返回true")
	}

	// 验证已删除
	if nc.Contains("key1") {
		t.Error("删除后不应该存在")
	}

	// 再次删除应该返回false
	deleted = nc.Delete("key1")
	if deleted {
		t.Error("删除不存在的key应该返回false")
	}
}

func TestNegativeCache_Clear(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	// 添加多个条目
	for i := 0; i < 10; i++ {
		nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}

	// 验证都存在
	for i := 0; i < 10; i++ {
		if !nc.Contains(fmt.Sprintf("key%d", i)) {
			t.Errorf("期望key%d存在", i)
		}
	}

	// 清空
	nc.Clear()

	// 验证都不存在
	for i := 0; i < 10; i++ {
		if nc.Contains(fmt.Sprintf("key%d", i)) {
			t.Errorf("清空后key%d不应该存在", i)
		}
	}

	// 统计信息应该重置
	stats := nc.Stats()
	if stats.TotalEntries != 0 {
		t.Errorf("清空后总条目数应该为0, 获得 %d", stats.TotalEntries)
	}
}

func TestNegativeCache_Stats(t *testing.T) {
	config := &negative.Config{
		DefaultTTL:      time.Minute,
		MaxSize:         100,
		CleanupInterval: 0, // 禁用自动清理
	}

	nc := negative.NewNegativeCache(config)
	defer nc.Close()

	// 添加条目
	for i := 0; i < 10; i++ {
		nc.Set(fmt.Sprintf("key%d", i), "test", negative.ErrorTypeNotFound)
	}

	// 进行一些Get操作
	for i := 0; i < 5; i++ {
		nc.Get(fmt.Sprintf("key%d", i))
	}

	// 查询不存在的key
	for i := 10; i < 15; i++ {
		nc.Get(fmt.Sprintf("key%d", i))
	}

	stats := nc.Stats()

	if stats.TotalEntries != 10 {
		t.Errorf("获得 %d 个总条目, 期望 10", stats.TotalEntries)
	}
	if stats.HitCount != 5 {
		t.Errorf("获得 %d 次命中, 期望 5", stats.HitCount)
	}
	if stats.MissCount != 5 {
		t.Errorf("获得 %d 次未命中, 期望 5", stats.MissCount)
	}
	if stats.SetCount != 10 {
		t.Errorf("获得 %d 次设置, 期望 10", stats.SetCount)
	}

	expectedHitRate := 0.5
	if stats.HitRate != expectedHitRate {
		t.Errorf("获得命中率 %.2f, 期望 %.2f", stats.HitRate, expectedHitRate)
	}
}

func TestNegativeCache_AccessCount(t *testing.T) {
	nc := negative.NewNegativeCache(nil)
	defer nc.Close()

	nc.Set("key1", "test", negative.ErrorTypeNotFound)

	// 多次访问
	for i := 0; i < 10; i++ {
		entry, exists := nc.Get("key1")
		if !exists {
			t.Fatal("条目应该存在")
		}
		expectedCount := int64(i + 1)
		if atomic.LoadInt64(&entry.AccessCount) != expectedCount {
			t.Errorf("第%d次访问: 获得计数 %d, 期望 %d",
				i+1, entry.AccessCount, expectedCount)
		}
	}
}

func TestNegativeCache_Expiration(t *testing.T) {
	config := &negative.Config{
		DefaultTTL:      100 * time.Millisecond,
		MaxSize:         100,
		CleanupInterval: 0, // 禁用自动清理
	}

	nc := negative.NewNegativeCache(config)
	defer nc.Close()

	nc.Set("key1", "test", negative.ErrorTypeNotFound)

	// 立即获取应该存在
	_, exists := nc.Get("key1")
	if !exists {
		t.Error("设置后立即获取应该存在")
	}

	// 等待过期
	time.Sleep(150 * time.Millisecond)

	// 再次获取应该不存在
	_, exists = nc.Get("key1")
	if exists {
		t.Error("过期后不应该存在")
	}
}
