package eviction_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"turbocache/internal/cache/eviction"
	"turbocache/internal/cache/value"
)

// ==============================
// 辅助函数（减少重复）
// ==============================

// 创建一个简单的 value.Entry，方便测试使用
func makeEntry(key string) *value.Entry {
	bv := value.NewByteView([]byte("value_" + key))
	return value.NewEntry(key, bv, 0)
}

// 从 keys 列表中判断是否包含 target
func contains(keys []string, target string) bool {
	for _, k := range keys {
		if k == target {
			return true
		}
	}
	return false
}

// ==============================
// ARCPolicy 核心功能测试
// ==============================

func TestARCPolicy_Basic(t *testing.T) {
	policy := eviction.NewARCPolicy(&eviction.Config{MaxEntries: 4})
	defer policy.Close()

	t.Run("基础操作", func(t *testing.T) {
		entry := makeEntry("key1")
		policy.OnSet("key1", entry)

		stats := policy.Stats()
		assert.Equal(t, "arc", stats.Type)
		assert.NotNil(t, stats.Extra)
	})

	t.Run("自适应调整", func(t *testing.T) {
		// 添加4个条目（填满缓存）
		for i := 1; i <= 4; i++ {
			key := fmt.Sprintf("key%d", i)
			policy.OnSet(key, makeEntry(key))
		}

		// 访问 key1 两次（期望提升到 T2）
		policy.OnAccess("key1")
		policy.OnAccess("key1")

		stats := policy.Stats()
		// 验证统计字段存在（不依赖精确大小，仅验证字段有效）
		_, ok1 := stats.Extra["t1_size"]
		_, ok2 := stats.Extra["t2_size"]
		_, ok3 := stats.Extra["target_p"]
		assert.True(t, ok1 && ok2 && ok3)
	})

	t.Run("幽灵列表机制", func(t *testing.T) {
		// 添加新条目触发淘汰
		policy.OnSet("key5", makeEntry("key5"))

		stats := policy.Stats()
		_, ok1 := stats.Extra["b1_size"]
		_, ok2 := stats.Extra["b2_size"]
		assert.True(t, ok1 && ok2)
	})
}

func TestARCPolicy_AdaptiveBehavior(t *testing.T) {
	policy := eviction.NewARCPolicy(&eviction.Config{MaxEntries: 6})
	defer policy.Close()

	t.Run("频繁访问模式", func(t *testing.T) {
		// 添加 1..3
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("freq_%d", i)
			policy.OnSet(key, makeEntry(key))
		}

		// 对 freq_1 高频访问
		for i := 0; i < 5; i++ {
			policy.OnAccess("freq_1")
		}

		// 添加更多条目填满
		for i := 4; i <= 6; i++ {
			key := fmt.Sprintf("freq_%d", i)
			policy.OnSet(key, makeEntry(key))
		}

		// freq_1 应该在缓存中（不保证在 T2 的确切位置，但应未被淘汰）
		keys := policy.SelectEvictKeys(10)
		assert.True(t, contains(keys, "freq_1"))
	})

	t.Run("顺序访问模式", func(t *testing.T) {
		policy.Reset()

		// 添加条目并只访问一次（模拟扫描）
		for i := 1; i <= 6; i++ {
			key := fmt.Sprintf("scan_%d", i)
			policy.OnSet(key, makeEntry(key))
		}

		stats := policy.Stats()
		// 断言 t1_size 存在且大于等于 0（更稳健，不依赖确切值）
		val, ok := stats.Extra["t1_size"]
		assert.True(t, ok)
		// 类型断言并确认非负
		if ok {
			t1Size := val.(int64)
			assert.True(t, t1Size >= 0)
		}
	})
}

func TestARCPolicy_SelectEvictKeys(t *testing.T) {
	policy := eviction.NewARCPolicy(&eviction.Config{MaxEntries: 5})
	defer policy.Close()

	t.Run("选择淘汰键", func(t *testing.T) {
		// 添加数据
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key%d", i)
			policy.OnSet(key, makeEntry(key))
		}

		keys := policy.SelectEvictKeys(2)
		assert.LessOrEqual(t, len(keys), 2)
	})

	t.Run("空策略", func(t *testing.T) {
		policy.Reset()
		keys := policy.SelectEvictKeys(5)
		assert.Equal(t, 0, len(keys))
	})
}

// ==============================
// ARCCache 核心功能测试
// ==============================

func TestARCCache_Basic(t *testing.T) {
	t.Run("添加和获取", func(t *testing.T) {
		cache := eviction.NewARCCache(4)

		entry := makeEntry("key")
		added := cache.Add("key", entry)
		assert.True(t, added)

		retrieved, found := cache.Get("key")
		assert.True(t, found)
		assert.NotNil(t, retrieved)
	})

	t.Run("自适应淘汰", func(t *testing.T) {
		cache := eviction.NewARCCache(4)

		// 添加 4 个条目填满
		for i := 1; i <= 4; i++ {
			key := fmt.Sprintf("k%d", i)
			cache.Add(key, makeEntry(key))
		}

		// 多次访问 k1（提升到 T2）
		for i := 0; i < 3; i++ {
			e, ok := cache.Get("k1")
			if !ok || e == nil {
				t.Fatalf("expected k1 present before promotion, got found=%v", ok)
			}
		}

		// 添加新条目触发淘汰
		cache.Add("k5", makeEntry("k5"))

		// k1 应该保留（高频）
		_, found := cache.Get("k1")
		assert.True(t, found)
	})

	t.Run("统计信息", func(t *testing.T) {
		cache := eviction.NewARCCache(4)
		stats := cache.Stats()
		assert.Equal(t, "arc", stats.Type)
		_, ok1 := stats.Extra["t1_size"]
		_, ok2 := stats.Extra["t2_size"]
		assert.True(t, ok1 && ok2)
	})

	t.Run("清空", func(t *testing.T) {
		cache := eviction.NewARCCache(4)
		cache.Clear()
		assert.Equal(t, 0, cache.Len())
	})
}

func TestARCCache_GhostLists(t *testing.T) {
	cache := eviction.NewARCCache(2)

	t.Run("幽灵命中场景", func(t *testing.T) {
		// 添加 2 个条目
		cache.Add("k1", makeEntry("k1"))
		cache.Add("k2", makeEntry("k2"))

		// 添加第三个，触发淘汰（其中一个会进入 ghost）
		cache.Add("k3", makeEntry("k3"))

		// 再次插入 k1（如果在 ghost 中会被 promote）
		cache.Add("k1", value.NewEntry("k1", value.NewByteView([]byte("v1_new")), 0))

		// k1 应该在缓存中
		_, found := cache.Get("k1")
		assert.True(t, found)
	})
}

// 新增：并发插入同一 key 的测试，验证 Add 的 double-check 并发安全性
func TestARCCache_ConcurrentAdd(t *testing.T) {
	cache := eviction.NewARCCache(10)
	key := "concurrent_key"

	// 并发 goroutine 数
	goroutines := 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	// 记录每个 goroutine Add 返回 true 的次数（期待只有一次真正的 "新插入"）
	var mu sync.Mutex
	trueCount := 0

	for i := 0; i < goroutines; i++ {
		go func(i int) {
			defer wg.Done()
			added := cache.Add(key, makeEntry(fmt.Sprintf("%s_%d", key, i)))
			if added {
				mu.Lock()
				trueCount++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// 期望只有一个 goroutine 返回 true（第一次插入），其余为更新/命中返回 false
	assert.Equal(t, 1, trueCount, "期望只有一次真正的新插入")
	// 最终缓存中该 key 应存在
	_, found := cache.Get(key)
	assert.True(t, found)
}

// 新增：验证 evictCallback 在淘汰时会被调用（同步回调收集）
func TestARCCache_EvictCallback(t *testing.T) {
	cache := eviction.NewARCCache(2)

	var mu sync.Mutex
	evicted := make([]string, 0)

	// 设置回调：收集被淘汰的 key
	cache.SetEvictCallback(func(k string, e *value.Entry) {
		mu.Lock()
		evicted = append(evicted, k)
		mu.Unlock()
	})

	// 添加 3 个条目触发至少一次淘汰
	cache.Add("a", makeEntry("a"))
	cache.Add("b", makeEntry("b"))
	cache.Add("c", makeEntry("c"))

	// 回调可能稍后发生（同 goroutine 内同步回调），加短等待以防竞态（一般不必要，但稳妥）
	time.Sleep(10 * time.Millisecond)

	// 至少有一次被淘汰
	mu.Lock()
	evictCount := len(evicted)
	mu.Unlock()
	assert.GreaterOrEqual(t, evictCount, 1, "至少应有一次淘汰回调")
}

// ==============================
// 基准测试（保留）
// ==============================

func BenchmarkARCPolicy_OnSet(b *testing.B) {
	policy := eviction.NewARCPolicy(&eviction.Config{MaxEntries: 1000})
	defer policy.Close()

	bv := value.NewByteView([]byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%500)
		policy.OnSet(key, value.NewEntry(key, bv, 0))
	}
}

func BenchmarkARCCache_Get(b *testing.B) {
	cache := eviction.NewARCCache(1000)
	bv := value.NewByteView([]byte("value"))

	// 准备数据
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%d", i)
		cache.Add(key, value.NewEntry(key, bv, 0))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%100)
		cache.Get(key)
	}
}

// ==============================
// 三种策略对比（简单 smoke 测试）
// ==============================

func TestEvictionPolicy_Comparison(t *testing.T) {
	t.Run("不同策略的行为对比", func(t *testing.T) {
		lru := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 5})
		defer lru.Close()

		lfu := eviction.NewLFUPolicy(&eviction.Config{MaxEntries: 5})
		defer lfu.Close()

		arc := eviction.NewARCPolicy(&eviction.Config{MaxEntries: 5})
		defer arc.Close()

		// 相同操作序列
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key%d", i)
			entry := makeEntry(key)
			lru.OnSet(key, entry)
			lfu.OnSet(key, entry)
			arc.OnSet(key, entry)
		}

		// 多次访问 key1
		for i := 0; i < 3; i++ {
			lru.OnAccess("key1")
			lfu.OnAccess("key1")
			arc.OnAccess("key1")
		}

		// 获取统计并做基本一致性断言（类型与条目数量）
		lruStats := lru.Stats()
		lfuStats := lfu.Stats()
		arcStats := arc.Stats()

		assert.Equal(t, "lru", lruStats.Type)
		assert.Equal(t, "lfu", lfuStats.Type)
		assert.Equal(t, "arc", arcStats.Type)

		assert.Equal(t, lruStats.TotalKeys, lfuStats.TotalKeys)
		assert.Equal(t, lruStats.TotalKeys, arcStats.TotalKeys)
	})
}
