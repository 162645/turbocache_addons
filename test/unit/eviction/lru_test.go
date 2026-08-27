package eviction_test

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"

	"turbocache/internal/cache/eviction"
	"turbocache/internal/cache/value"
)

// ============================================================================
// LRUPolicy 基础功能测试
// ============================================================================

func TestLRUPolicy_Creation(t *testing.T) {
	t.Run("使用默认配置创建", func(t *testing.T) {
		policy := eviction.NewLRUPolicy(nil)
		require.NotNil(t, policy)

		stats := policy.Stats()
		assert.Equal(t, "lru", stats.Type)
	})

	t.Run("使用自定义配置创建", func(t *testing.T) {
		config := &eviction.Config{
			MaxEntries: 100,
		}

		policy := eviction.NewLRUPolicy(config)
		require.NotNil(t, policy)

		err := policy.Close()
		assert.NoError(t, err)
	})
}

func TestLRUPolicy_OnSet(t *testing.T) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 3})
	defer policy.Close()

	t.Run("设置新条目", func(t *testing.T) {
		bv := value.NewByteView([]byte("value1"))
		entry := value.NewEntry("key1", bv, 0)

		policy.OnSet("key1", entry)

		stats := policy.Stats()
		assert.Equal(t, int64(1), stats.TotalKeys)
	})

	t.Run("更新已存在条目", func(t *testing.T) {
		bv := value.NewByteView([]byte("value_updated"))
		entry := value.NewEntry("key1", bv, 0)

		policy.OnSet("key1", entry)

		stats := policy.Stats()
		assert.Equal(t, int64(1), stats.TotalKeys) // 不应增加
	})

	t.Run("达到容量限制自动淘汰", func(t *testing.T) {
		// 已有1个，再加3个，应该淘汰1个
		bv2 := value.NewByteView([]byte("value2"))
		bv3 := value.NewByteView([]byte("value3"))
		bv4 := value.NewByteView([]byte("value4"))

		policy.OnSet("key2", value.NewEntry("key2", bv2, 0))
		policy.OnSet("key3", value.NewEntry("key3", bv3, 0))
		policy.OnSet("key4", value.NewEntry("key4", bv4, 0))

		stats := policy.Stats()
		assert.Equal(t, int64(3), stats.TotalKeys) // 最多3个
		assert.True(t, stats.EvictionCount > 0)
	})
}

func TestLRUPolicy_OnAccess(t *testing.T) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 3})
	defer policy.Close()

	t.Run("访问存在的键", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key1", bv, 0)
		policy.OnSet("key1", entry)

		policy.OnAccess("key1")

		stats := policy.Stats()
		assert.True(t, stats.AccessCount > 0)
	})

	t.Run("访问不存在的键", func(t *testing.T) {
		policy.OnAccess("nonexistent")

		// 不应该崩溃，统计信息应该更新
		stats := policy.Stats()
		assert.True(t, stats.AccessCount > 0)
	})

	t.Run("访问影响淘汰顺序", func(t *testing.T) {
		policy.Reset()

		// 添加3个条目
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("value%d", i)))
			policy.OnSet(key, value.NewEntry(key, bv, 0))
		}

		// 访问key1，使其成为最近使用
		policy.OnAccess("key1")

		// 添加第4个条目，应该淘汰key2（最久未使用）
		bv := value.NewByteView([]byte("value4"))
		policy.OnSet("key4", value.NewEntry("key4", bv, 0))

		// key1应该还在，key2应该被淘汰
		keys := policy.SelectEvictKeys(3)
		keySet := make(map[string]bool)
		for _, k := range keys {
			keySet[k] = true
		}

		assert.True(t, keySet["key1"])  // key1应该在
		assert.False(t, keySet["key2"]) // key2已被淘汰
	})
}

func TestLRUPolicy_OnDelete(t *testing.T) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10})
	defer policy.Close()

	t.Run("删除存在的键", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("del_key", bv, 0)
		policy.OnSet("del_key", entry)

		stats := policy.Stats()
		before := stats.TotalKeys

		policy.OnDelete("del_key")

		stats = policy.Stats()
		assert.Equal(t, before-1, stats.TotalKeys)
	})

	t.Run("删除不存在的键", func(t *testing.T) {
		// 不应该崩溃
		assert.NotPanics(t, func() {
			policy.OnDelete("nonexistent")
		})
	})
}

func TestLRUPolicy_SelectEvictKeys(t *testing.T) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10})
	defer policy.Close()

	t.Run("选择淘汰键", func(t *testing.T) {
		// 添加5个条目
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("value%d", i)))
			policy.OnSet(key, value.NewEntry(key, bv, 0))
		}

		// 选择2个最久未使用的键
		keys := policy.SelectEvictKeys(2)
		assert.Equal(t, 2, len(keys))

		// 应该是key1和key2（最先添加的）
		assert.Contains(t, keys, "key1")
		assert.Contains(t, keys, "key2")
	})

	t.Run("请求数量超过现有条目", func(t *testing.T) {
		keys := policy.SelectEvictKeys(100)
		assert.True(t, len(keys) <= 5) // 最多返回5个
	})

	t.Run("请求0个键", func(t *testing.T) {
		keys := policy.SelectEvictKeys(0)
		assert.Nil(t, keys)
	})

	t.Run("请求负数", func(t *testing.T) {
		keys := policy.SelectEvictKeys(-1)
		assert.Nil(t, keys)
	})
}

func TestLRUPolicy_Stats(t *testing.T) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10})
	defer policy.Close()

	t.Run("统计信息准确性", func(t *testing.T) {
		// 添加条目
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("value%d", i)))
			policy.OnSet(key, value.NewEntry(key, bv, 0))
		}

		// 访问
		policy.OnAccess("key1")
		policy.OnAccess("key2")

		stats := policy.Stats()
		assert.Equal(t, "lru", stats.Type)
		assert.Equal(t, int64(3), stats.TotalKeys)
		assert.True(t, stats.AccessCount > 0)
		assert.NotNil(t, stats.Extra)
	})

	t.Run("命中率计算", func(t *testing.T) {
		policy.Reset()

		bv := value.NewByteView([]byte("value"))
		policy.OnSet("key1", value.NewEntry("key1", bv, 0))

		// 2次命中
		policy.OnAccess("key1")
		policy.OnAccess("key1")

		// 1次未命中
		policy.OnAccess("nonexistent")

		stats := policy.Stats()
		// 命中率应该在合理范围内
		assert.True(t, stats.HitRate >= 0 && stats.HitRate <= 1)
	})
}

func TestLRUPolicy_Reset(t *testing.T) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10})
	defer policy.Close()

	t.Run("重置清空所有数据", func(t *testing.T) {
		// 添加一些数据
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			policy.OnSet(key, value.NewEntry(key, bv, 0))
		}

		policy.OnAccess("key1")

		// 重置
		policy.Reset()

		stats := policy.Stats()
		assert.Equal(t, int64(0), stats.TotalKeys)
		assert.Equal(t, int64(0), stats.AccessCount)
		assert.Equal(t, int64(0), stats.EvictionCount)
	})
}

// ============================================================================
// LRUCache 基础功能测试
// ============================================================================

func TestLRUCache_Creation(t *testing.T) {
	t.Run("创建LRU缓存", func(t *testing.T) {
		cache := eviction.NewLRUCache(10)
		require.NotNil(t, cache)

		assert.Equal(t, 0, cache.Len())
	})
}

func TestLRUCache_AddGet(t *testing.T) {
	cache := eviction.NewLRUCache(3)

	t.Run("添加和获取", func(t *testing.T) {
		bv := value.NewByteView([]byte("value1"))
		entry := value.NewEntry("key1", bv, 0)

		added := cache.Add("key1", entry)
		assert.True(t, added)

		retrieved, found := cache.Get("key1")
		assert.True(t, found)
		assert.Equal(t, "value1", retrieved.Value.String())
	})

	t.Run("获取不存在的键", func(t *testing.T) {
		_, found := cache.Get("nonexistent")
		assert.False(t, found)
	})

	t.Run("更新已存在的键", func(t *testing.T) {
		bv := value.NewByteView([]byte("value_updated"))
		entry := value.NewEntry("key1", bv, 0)

		added := cache.Add("key1", entry)
		assert.False(t, added) // 返回false表示是更新

		retrieved, found := cache.Get("key1")
		assert.True(t, found)
		assert.Equal(t, "value_updated", retrieved.Value.String())
	})
}

func TestLRUCache_Remove(t *testing.T) {
	cache := eviction.NewLRUCache(10)

	t.Run("删除存在的键", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)
		cache.Add("key", entry)

		removed := cache.Remove("key")
		assert.True(t, removed)

		_, found := cache.Get("key")
		assert.False(t, found)
	})

	t.Run("删除不存在的键", func(t *testing.T) {
		removed := cache.Remove("nonexistent")
		assert.False(t, removed)
	})
}

func TestLRUCache_Contains(t *testing.T) {
	cache := eviction.NewLRUCache(10)

	bv := value.NewByteView([]byte("value"))
	entry := value.NewEntry("key", bv, 0)
	cache.Add("key", entry)

	t.Run("检查存在的键", func(t *testing.T) {
		assert.True(t, cache.Contains("key"))
	})

	t.Run("检查不存在的键", func(t *testing.T) {
		assert.False(t, cache.Contains("nonexistent"))
	})
}

func TestLRUCache_Clear(t *testing.T) {
	cache := eviction.NewLRUCache(10)

	t.Run("清空缓存", func(t *testing.T) {
		// 添加数据
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
		}

		assert.Equal(t, 5, cache.Len())

		cache.Clear()

		assert.Equal(t, 0, cache.Len())
	})
}

func TestLRUCache_Eviction(t *testing.T) {
	cache := eviction.NewLRUCache(3)

	t.Run("自动淘汰最久未使用", func(t *testing.T) {
		// 添加3个条目
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
		}

		// 访问key1，使其成为最近使用
		cache.Get("key1")

		// 添加第4个，应该淘汰key2
		bv := value.NewByteView([]byte("value4"))
		entry := value.NewEntry("key4", bv, 0)
		cache.Add("key4", entry)

		// key1应该还在
		_, found := cache.Get("key1")
		assert.True(t, found)

		// key2应该被淘汰
		_, found = cache.Get("key2")
		assert.False(t, found)
	})
}

func TestLRUCache_EvictCallback(t *testing.T) {
	cache := eviction.NewLRUCache(2)

	t.Run("淘汰回调触发", func(t *testing.T) {
		var evictedKeys []string
		var mu sync.Mutex

		cache.SetEvictCallback(func(key string, entry *value.Entry) {
			mu.Lock()
			evictedKeys = append(evictedKeys, key)
			mu.Unlock()
		})

		// 添加3个条目，应该淘汰1个
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
		}

		mu.Lock()
		assert.Equal(t, 1, len(evictedKeys))
		assert.Equal(t, "key1", evictedKeys[0])
		mu.Unlock()
	})
}

// ============================================================================
// LRU 并发测试
// ============================================================================

func TestLRUPolicy_Concurrent(t *testing.T) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 100})
	defer policy.Close()

	t.Run("并发设置", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 50

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 10; j++ {
					key := fmt.Sprintf("key_%d_%d", id, j)
					bv := value.NewByteView([]byte("value"))
					entry := value.NewEntry(key, bv, 0)
					policy.OnSet(key, entry)
				}
			}(i)
		}

		wg.Wait()

		stats := policy.Stats()
		assert.True(t, stats.TotalKeys > 0)
	})

	t.Run("并发访问", func(t *testing.T) {
		// 先添加一些数据
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			policy.OnSet(key, entry)
		}

		var wg sync.WaitGroup
		numGoroutines := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 100; j++ {
					key := fmt.Sprintf("key%d", id%10)
					policy.OnAccess(key)
				}
			}(i)
		}

		wg.Wait()

		stats := policy.Stats()
		assert.True(t, stats.AccessCount > 0)
	})

	t.Run("并发读写", func(t *testing.T) {
		var wg sync.WaitGroup

		// 并发写
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				key := fmt.Sprintf("rw_key_%d", id)
				bv := value.NewByteView([]byte("value"))
				entry := value.NewEntry(key, bv, 0)
				policy.OnSet(key, entry)
			}(i)
		}

		// 并发读
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				key := fmt.Sprintf("rw_key_%d", id)
				policy.OnAccess(key)
			}(i)
		}

		wg.Wait()

		// 不应该崩溃
		stats := policy.Stats()
		assert.NotNil(t, stats)
	})
}

func TestLRUCache_Concurrent(t *testing.T) {
	cache := eviction.NewLRUCache(100)

	t.Run("并发添加", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 50

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 10; j++ {
					key := fmt.Sprintf("key_%d_%d", id, j)
					bv := value.NewByteView([]byte("value"))
					entry := value.NewEntry(key, bv, 0)
					cache.Add(key, entry)
				}
			}(i)
		}

		wg.Wait()

		assert.True(t, cache.Len() > 0)
	})

	t.Run("并发获取", func(t *testing.T) {
		// 先添加数据
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("get_key_%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
		}

		var wg sync.WaitGroup
		numGoroutines := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				for j := 0; j < 100; j++ {
					key := fmt.Sprintf("get_key_%d", id%10)
					cache.Get(key)
				}
			}(i)
		}

		wg.Wait()

		// 验证统计
		stats := cache.Stats()
		assert.True(t, stats.AccessCount > 0)
	})

	t.Run("并发删除", func(t *testing.T) {
		// 准备数据
		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("del_key_%d", i)
			keys[i] = key
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
		}

		var wg sync.WaitGroup

		for _, key := range keys {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				cache.Remove(k)
			}(key)
		}

		wg.Wait()

		// 所有键都应该被删除
		for _, key := range keys {
			_, found := cache.Get(key)
			assert.False(t, found)
		}
	})
}

// ============================================================================
// LRU 边界情况测试
// ============================================================================

func TestLRUPolicy_EdgeCases(t *testing.T) {
	t.Run("容量为1的策略", func(t *testing.T) {
		policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 1})
		defer policy.Close()

		bv1 := value.NewByteView([]byte("value1"))
		bv2 := value.NewByteView([]byte("value2"))

		policy.OnSet("key1", value.NewEntry("key1", bv1, 0))
		policy.OnSet("key2", value.NewEntry("key2", bv2, 0))

		stats := policy.Stats()
		assert.Equal(t, int64(1), stats.TotalKeys)
		assert.True(t, stats.EvictionCount > 0)
	})

	t.Run("空策略操作", func(t *testing.T) {
		policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10})
		defer policy.Close()

		// 在空策略上执行操作不应该崩溃
		policy.OnAccess("key")
		policy.OnDelete("key")
		keys := policy.SelectEvictKeys(5)
		assert.Equal(t, 0, len(keys))

		stats := policy.Stats()
		assert.Equal(t, int64(0), stats.TotalKeys)
	})

	t.Run("重复设置相同键", func(t *testing.T) {
		policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10})
		defer policy.Close()

		bv := value.NewByteView([]byte("value"))

		for i := 0; i < 10; i++ {
			entry := value.NewEntry("same_key", bv, 0)
			policy.OnSet("same_key", entry)
		}

		stats := policy.Stats()
		assert.Equal(t, int64(1), stats.TotalKeys)
	})
}

func TestLRUCache_EdgeCases(t *testing.T) {
	t.Run("容量为0的缓存", func(t *testing.T) {
		cache := eviction.NewLRUCache(0)

		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)

		// 应该允许添加但立即淘汰
		cache.Add("key", entry)
		assert.Equal(t, 0, cache.Len())
	})

	t.Run("大量相同键的操作", func(t *testing.T) {
		cache := eviction.NewLRUCache(1)

		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)

		// 重复添加
		for i := 0; i < 1000; i++ {
			cache.Add("key", entry)
		}

		assert.Equal(t, 1, cache.Len())
	})

	t.Run("nil回调函数", func(t *testing.T) {
		cache := eviction.NewLRUCache(2)
		cache.SetEvictCallback(nil)

		// 不应该崩溃
		bv := value.NewByteView([]byte("value"))
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
		}
	})
}

// ============================================================================
// LRU 性能基准测试
// ============================================================================

func BenchmarkLRUPolicy_OnSet(b *testing.B) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10000})
	defer policy.Close()

	bv := value.NewByteView([]byte("benchmark value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		policy.OnSet(key, entry)
	}
}

func BenchmarkLRUPolicy_OnAccess(b *testing.B) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10000})
	defer policy.Close()

	// 准备数据
	bv := value.NewByteView([]byte("value"))
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		policy.OnSet(key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		policy.OnAccess(key)
	}
}

func BenchmarkLRUPolicy_SelectEvictKeys(b *testing.B) {
	policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 10000})
	defer policy.Close()

	// 准备数据
	bv := value.NewByteView([]byte("value"))
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		policy.OnSet(key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = policy.SelectEvictKeys(10)
	}
}

func BenchmarkLRUCache_Add(b *testing.B) {
	cache := eviction.NewLRUCache(10000)
	bv := value.NewByteView([]byte("benchmark value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		cache.Add(key, entry)
	}
}

func BenchmarkLRUCache_Get(b *testing.B) {
	cache := eviction.NewLRUCache(10000)
	bv := value.NewByteView([]byte("value"))

	// 准备数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		cache.Add(key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		cache.Get(key)
	}
}

func BenchmarkLRUCache_Remove(b *testing.B) {
	bv := value.NewByteView([]byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache := eviction.NewLRUCache(10000)
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		cache.Add(key, entry)
		b.StartTimer()

		cache.Remove(key)
	}
}

func BenchmarkLRUCache_Concurrent(b *testing.B) {
	cache := eviction.NewLRUCache(10000)
	bv := value.NewByteView([]byte("benchmark value"))

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i)
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
			cache.Get(key)
			i++
		}
	})
}

func BenchmarkLRUCache_WithEviction(b *testing.B) {
	cache := eviction.NewLRUCache(100) // 小容量触发频繁淘汰
	bv := value.NewByteView([]byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		cache.Add(key, entry)
	}
}

// ============================================================================
// LRU 特殊场景测试
// ============================================================================

func TestLRUPolicy_AccessPattern(t *testing.T) {
	t.Run("热点数据保留", func(t *testing.T) {
		policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 5})
		defer policy.Close()

		// 添加5个条目
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			policy.OnSet(key, entry)
		}

		// 频繁访问key1（热点数据）
		for i := 0; i < 10; i++ {
			policy.OnAccess("key1")
		}

		// 只添加少量新条目触发淘汰（插入 2 个），以验证热点被保留
		for i := 6; i <= 7; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			policy.OnSet(key, entry)
		}

		// 检查：在接下来的 2 个最久未使用候选淘汰键中，不应该包含 key1
		keys := policy.SelectEvictKeys(2)
		found := false
		for _, k := range keys {
			if k == "key1" {
				found = true
				break
			}
		}
		// key1 作为热点不应出现在最久未使用的前 2 位
		assert.False(t, found)
	})

	t.Run("冷数据快速淘汰", func(t *testing.T) {
		policy := eviction.NewLRUPolicy(&eviction.Config{MaxEntries: 3})
		defer policy.Close()

		// 添加3个条目
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			policy.OnSet(key, entry)
		}

		// 只访问key2和key3
		policy.OnAccess("key2")
		policy.OnAccess("key3")

		// 添加新条目
		bv := value.NewByteView([]byte("value4"))
		policy.OnSet("key4", value.NewEntry("key4", bv, 0))

		// key1（冷数据）应该被淘汰
		keys := policy.SelectEvictKeys(3)
		found := false
		for _, k := range keys {
			if k == "key1" {
				found = true
				break
			}
		}
		assert.False(t, found) // key1已被淘汰（不在保留集合中）
	})
}

func TestLRUCache_MemoryEfficiency(t *testing.T) {
	t.Run("大量条目的内存使用", func(t *testing.T) {
		cache := eviction.NewLRUCache(10000)

		// 添加10000个条目
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("key_%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			cache.Add(key, entry)
		}

		assert.Equal(t, 10000, cache.Len())

		// 清空应该释放内存
		cache.Clear()
		assert.Equal(t, 0, cache.Len())
	})
}
