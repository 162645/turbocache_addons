package protection_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turbocache/internal/protection/singleflight"
)

// ===== 基础功能测试 =====

// TestGroupDo_Basic 测试基本的Do功能
func TestGroupDo_Basic(t *testing.T) {
	var g singleflight.Group
	var calls int32

	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return "result", nil
	}

	// 第一次调用
	v, err, shared := g.Do("key", fn)
	if err != nil {
		t.Errorf("意外错误: %v", err)
	}
	if v != "result" {
		t.Errorf("获得 %v, 期望 result", v)
	}
	if shared {
		t.Errorf("第一次调用不应该是共享的")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("函数调用了 %d 次, 期望 1 次", calls)
	}

	// 第二次调用（新的key，应该再次执行）
	v, err, shared = g.Do("key2", fn)
	if err != nil {
		t.Errorf("意外错误: %v", err)
	}
	if shared {
		t.Errorf("不同的key不应该共享")
	}
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("函数调用了 %d 次, 期望 2 次", calls)
	}
}

// TestGroupDo_DuplicateSuppression 测试重复调用抑制
func TestGroupDo_DuplicateSuppression(t *testing.T) {
	var g singleflight.Group
	var calls int32

	// 使用channel来控制函数执行时间
	start := make(chan struct{})
	fn := func() (interface{}, error) {
		<-start // 等待信号
		atomic.AddInt32(&calls, 1)
		return "result", nil
	}

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)

	results := make([]interface{}, n)
	errors := make([]error, n)
	shareds := make([]bool, n)

	// 启动n个并发调用
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			results[idx], errors[idx], shareds[idx] = g.Do("key", fn)
		}(i)
	}

	// 等待所有goroutine启动
	time.Sleep(10 * time.Millisecond)
	close(start) // 释放函数执行
	wg.Wait()

	// 验证只调用了一次
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("函数调用了 %d 次, 期望 1 次", got)
	}

	// 验证所有结果相同
	sharedCount := 0
	for i := 0; i < n; i++ {
		if errors[i] != nil {
			t.Errorf("调用 %d: 意外错误: %v", i, errors[i])
		}
		if results[i] != "result" {
			t.Errorf("调用 %d: 获得 %v, 期望 result", i, results[i])
		}
		if shareds[i] {
			sharedCount++
		}
	}

	// 应该有n-1个调用是shared的
	if sharedCount != n-1 {
		t.Errorf("获得 %d 个共享调用, 期望 %d", sharedCount, n-1)
	}
}

// TestGroupDo_Error 测试错误处理
func TestGroupDo_Error(t *testing.T) {
	var g singleflight.Group
	someErr := errors.New("测试错误")

	fn := func() (interface{}, error) {
		return nil, someErr
	}

	v, err, _ := g.Do("key", fn)
	if err != someErr {
		t.Errorf("获得错误 %v, 期望 %v", err, someErr)
	}
	if v != nil {
		t.Errorf("获得值 %v, 期望 nil", v)
	}
}

// TestGroupDo_Panic 测试panic处理
func TestGroupDo_Panic(t *testing.T) {
	var g singleflight.Group

	fn := func() (interface{}, error) {
		panic("测试panic")
	}

	defer func() {
		if r := recover(); r == nil {
			t.Error("期望捕获panic")
		}
	}()

	g.Do("key", fn)
}

// ===== 并发安全测试 =====

// TestGroupDo_Concurrent 测试并发安全
func TestGroupDo_Concurrent(t *testing.T) {
	var g singleflight.Group
	var calls int32

	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond)
		return "result", nil
	}

	const concurrency = 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	// 启动100个并发调用
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			v, err, _ := g.Do("key", fn)
			if err != nil {
				t.Errorf("意外错误: %v", err)
			}
			if v != "result" {
				t.Errorf("获得 %v, 期望 result", v)
			}
		}()
	}

	wg.Wait()

	// 验证只调用了一次
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("函数调用了 %d 次, 期望 1 次", got)
	}
}

// TestGroupDo_MultipleKeys 测试多个key的并发
func TestGroupDo_MultipleKeys(t *testing.T) {
	var g singleflight.Group
	var calls int32

	fn := func(key string) func() (interface{}, error) {
		return func() (interface{}, error) {
			atomic.AddInt32(&calls, 1)
			time.Sleep(5 * time.Millisecond)
			return key + "_result", nil
		}
	}

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	const callsPerKey = 20

	var wg sync.WaitGroup
	wg.Add(len(keys) * callsPerKey)

	for _, key := range keys {
		for i := 0; i < callsPerKey; i++ {
			go func(k string) {
				defer wg.Done()
				v, err, _ := g.Do(k, fn(k))
				if err != nil {
					t.Errorf("意外错误: %v", err)
				}
				expected := k + "_result"
				if v != expected {
					t.Errorf("获得 %v, 期望 %v", v, expected)
				}
			}(key)
		}
	}

	wg.Wait()

	// 每个key应该只调用一次
	expectedCalls := int32(len(keys))
	if got := atomic.LoadInt32(&calls); got != expectedCalls {
		t.Errorf("函数调用了 %d 次, 期望 %d 次", got, expectedCalls)
	}
}

// ===== DoChan 异步测试 =====

// TestGroupDoChan_Basic 测试基本的DoChan功能
func TestGroupDoChan_Basic(t *testing.T) {
	var g singleflight.Group
	var calls int32

	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond)
		return "result", nil
	}

	ch := g.DoChan("key", fn)

	// 等待结果
	result := <-ch
	if result.Err != nil {
		t.Errorf("意外错误: %v", result.Err)
	}
	if result.Val != "result" {
		t.Errorf("获得 %v, 期望 result", result.Val)
	}
	if result.Shared {
		t.Errorf("第一次调用不应该是共享的")
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("函数调用了 %d 次, 期望 1 次", calls)
	}
}

// TestGroupDoChan_Concurrent 测试DoChan的并发
func TestGroupDoChan_Concurrent(t *testing.T) {
	var g singleflight.Group
	var calls int32

	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(10 * time.Millisecond)
		return "result", nil
	}

	const n = 10
	channels := make([]<-chan singleflight.Result, n)

	// 启动n个并发调用
	for i := 0; i < n; i++ {
		channels[i] = g.DoChan("key", fn)
	}

	// 收集所有结果
	sharedCount := 0
	for i := 0; i < n; i++ {
		result := <-channels[i]
		if result.Err != nil {
			t.Errorf("调用 %d: 意外错误: %v", i, result.Err)
		}
		if result.Val != "result" {
			t.Errorf("调用 %d: 获得 %v, 期望 result", i, result.Val)
		}
		if result.Shared {
			sharedCount++
		}
	}

	// 验证只调用了一次
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Errorf("函数调用了 %d 次, 期望 1 次", got)
	}

	// 验证shared数量
	if sharedCount != n-1 {
		t.Errorf("获得 %d 个共享调用, 期望 %d", sharedCount, n-1)
	}
}

// ===== Forget 功能测试 =====

// TestGroupForget 测试Forget功能
func TestGroupForget(t *testing.T) {
	var g singleflight.Group
	var calls int32

	fn := func() (interface{}, error) {
		atomic.AddInt32(&calls, 1)
		return "result", nil
	}

	// 第一次调用
	_, _, _ = g.Do("key", fn)
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("函数调用了 %d 次, 期望 1 次", calls)
	}

	// 忘记该key
	g.Forget("key")

	// 再次调用应该重新执行
	_, _, _ = g.Do("key", fn)
	if atomic.LoadInt32(&calls) != 2 {
		t.Errorf("函数调用了 %d 次, 期望 2 次", calls)
	}
}

// ===== SingleFlightCache 测试 =====

// mockCache 模拟缓存实现
type mockCache struct {
	mu   sync.RWMutex
	data map[string]interface{}
}

func newMockCache() *mockCache {
	return &mockCache{
		data: make(map[string]interface{}),
	}
}

func (m *mockCache) Get(ctx context.Context, key string) (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if v, ok := m.data[key]; ok {
		return v, nil
	}
	return nil, errors.New("缓存未命中")
}

func (m *mockCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.data[key] = value
	return nil
}

func (m *mockCache) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.data, key)
	return nil
}

// TestSingleFlightCache_GetWithLoader 测试带loader的缓存获取
func TestSingleFlightCache_GetWithLoader(t *testing.T) {
	cache := newMockCache()
	sf := singleflight.NewSingleFlightCache(cache)

	var loads int32
	loader := func() (interface{}, error) {
		atomic.AddInt32(&loads, 1)
		time.Sleep(10 * time.Millisecond)
		return "loaded_value", nil
	}

	ctx := context.Background()

	// 第一次调用，应该触发loader
	v, err := sf.GetWithLoader(ctx, "key", loader, time.Hour)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if v != "loaded_value" {
		t.Errorf("获得 %v, 期望 loaded_value", v)
	}
	if atomic.LoadInt32(&loads) != 1 {
		t.Errorf("loader调用了 %d 次, 期望 1 次", loads)
	}

	// 第二次调用，应该从缓存获取
	v, err = sf.GetWithLoader(ctx, "key", loader, time.Hour)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if v != "loaded_value" {
		t.Errorf("获得 %v, 期望 loaded_value", v)
	}
	if atomic.LoadInt32(&loads) != 1 {
		t.Errorf("loader调用了 %d 次, 期望 1 次 (应该使用缓存)", loads)
	}
}

// TestSingleFlightCache_ConcurrentLoad 测试并发加载
func TestSingleFlightCache_ConcurrentLoad(t *testing.T) {
	cache := newMockCache()
	sf := singleflight.NewSingleFlightCache(cache)

	var loads int32
	loader := func() (interface{}, error) {
		atomic.AddInt32(&loads, 1)
		time.Sleep(50 * time.Millisecond)
		return "loaded_value", nil
	}

	ctx := context.Background()
	const concurrency = 20

	var wg sync.WaitGroup
	wg.Add(concurrency)

	// 启动并发加载
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			v, err := sf.GetWithLoader(ctx, "key", loader, time.Hour)
			if err != nil {
				t.Errorf("意外错误: %v", err)
			}
			if v != "loaded_value" {
				t.Errorf("获得 %v, 期望 loaded_value", v)
			}
		}()
	}

	wg.Wait()

	// 验证只加载了一次
	if got := atomic.LoadInt32(&loads); got != 1 {
		t.Errorf("loader调用了 %d 次, 期望 1 次", got)
	}
}

// TestSingleFlightCache_Delete 测试删除功能
func TestSingleFlightCache_Delete(t *testing.T) {
	cache := newMockCache()
	sf := singleflight.NewSingleFlightCache(cache)

	ctx := context.Background()

	// 设置值
	err := sf.Set(ctx, "key", "value", time.Hour)
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}

	// 验证存在
	v, err := cache.Get(ctx, "key")
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}
	if v != "value" {
		t.Errorf("获得 %v, 期望 value", v)
	}

	// 删除
	err = sf.Delete(ctx, "key")
	if err != nil {
		t.Fatalf("意外错误: %v", err)
	}

	// 验证已删除
	_, err = cache.Get(ctx, "key")
	if err == nil {
		t.Error("期望缓存未命中错误")
	}
}

// ===== 性能基准测试 =====

// BenchmarkGroupDo 基准测试Do方法
func BenchmarkGroupDo(b *testing.B) {
	var g singleflight.Group
	fn := func() (interface{}, error) {
		return "result", nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Do(fmt.Sprintf("key_%d", i), fn)
	}
}

// BenchmarkGroupDo_Concurrent 基准测试并发Do
func BenchmarkGroupDo_Concurrent(b *testing.B) {
	var g singleflight.Group
	fn := func() (interface{}, error) {
		return "result", nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			g.Do(fmt.Sprintf("key_%d", i%100), fn)
			i++
		}
	})
}

// BenchmarkSingleFlightCache_GetWithLoader 基准测试缓存加载
func BenchmarkSingleFlightCache_GetWithLoader(b *testing.B) {
	cache := newMockCache()
	sf := singleflight.NewSingleFlightCache(cache)
	ctx := context.Background()

	loader := func() (interface{}, error) {
		return "value", nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sf.GetWithLoader(ctx, fmt.Sprintf("key_%d", i%100), loader, time.Hour)
	}
}
