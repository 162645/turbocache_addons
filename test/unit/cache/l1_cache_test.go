package cache_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"turbocache/internal/cache/engine"
	"turbocache/internal/cache/value"
	"turbocache/internal/config"
)

// ============================================================================
// L1Cache 创建和配置测试
// ============================================================================

func TestL1Cache_Creation(t *testing.T) {
	t.Run("使用有效配置创建", func(t *testing.T) {
		config := &config.L1CacheConfig{
			MaxSize:     1024 * 1024,
			MaxEntries:  1000,
			ShardCount:  32,
			TTL:         5 * time.Minute,
			StatsWindow: 1 * time.Minute,
		}

		cache, err := engine.NewL1Cache(config)
		require.NoError(t, err)
		require.NotNil(t, cache)

		err = cache.Close()
		assert.NoError(t, err)
	})

	t.Run("nil配置应失败", func(t *testing.T) {
		_, err := engine.NewL1Cache(nil)
		assert.Error(t, err)
	})
}

// ============================================================================
// L1Cache 基础操作测试
// ============================================================================

func TestL1Cache_GetSet(t *testing.T) {
	config := &config.L1CacheConfig{
		MaxSize:    1024 * 1024,
		MaxEntries: 100,
		ShardCount: 8,
	}

	cache, err := engine.NewL1Cache(config)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	t.Run("设置和获取值", func(t *testing.T) {
		key := "test_key"
		bv := value.NewByteView([]byte("test value"))
		entry := value.NewEntry(key, bv, 0)

		err := cache.Set(ctx, key, entry)
		require.NoError(t, err)

		retrieved, err := cache.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "test value", retrieved.Value.String())
	})

	t.Run("获取不存在的键", func(t *testing.T) {
		_, err := cache.Get(ctx, "nonexistent")
		assert.Error(t, err)
	})

	t.Run("设置空键应失败", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("", bv, 0)

		err := cache.Set(ctx, "", entry)
		assert.Error(t, err)
	})

	t.Run("设置nil条目应失败", func(t *testing.T) {
		err := cache.Set(ctx, "key", nil)
		assert.Error(t, err)
	})

	t.Run("更新已存在的键", func(t *testing.T) {
		key := "update_key"
		bv1 := value.NewByteView([]byte("value1"))
		entry1 := value.NewEntry(key, bv1, 0)

		err := cache.Set(ctx, key, entry1)
		require.NoError(t, err)

		bv2 := value.NewByteView([]byte("value2"))
		entry2 := value.NewEntry(key, bv2, 0)

		err = cache.Set(ctx, key, entry2)
		require.NoError(t, err)

		retrieved, err := cache.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "value2", retrieved.Value.String())
	})
}

func TestL1Cache_Delete(t *testing.T) {
	config := &config.L1CacheConfig{
		MaxSize:    1024 * 1024,
		MaxEntries: 100,
		ShardCount: 8,
	}

	cache, err := engine.NewL1Cache(config)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	t.Run("删除存在的键", func(t *testing.T) {
		key := "delete_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := cache.Set(ctx, key, entry)
		require.NoError(t, err)

		err = cache.Delete(ctx, key)
		assert.NoError(t, err)

		_, err = cache.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("删除不存在的键", func(t *testing.T) {
		err := cache.Delete(ctx, "nonexistent")
		// 删除不存在的键不应该报错
		assert.NoError(t, err)
	})
}

func TestL1Cache_Exists(t *testing.T) {
	config := &config.L1CacheConfig{
		MaxSize:    1024 * 1024,
		MaxEntries: 100,
		ShardCount: 8,
	}

	cache, err := engine.NewL1Cache(config)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	t.Run("检查存在的键", func(t *testing.T) {
		key := "exists_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := cache.Set(ctx, key, entry)
		require.NoError(t, err)

		exists, err := cache.Exists(ctx, key)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("检查不存在的键", func(t *testing.T) {
		exists, err := cache.Exists(ctx, "nonexistent")
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestL1Cache_Clear(t *testing.T) {
	config := &config.L1CacheConfig{
		MaxSize:    1024 * 1024,
		MaxEntries: 100,
		ShardCount: 8,
	}

	cache, err := engine.NewL1Cache(config)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	t.Run("清空缓存", func(t *testing.T) {
		// 添加多个键
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("value_%d", i)))
			entry := value.NewEntry(key, bv, 0)
			err := cache.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 清空
		err := cache.Clear(ctx)
		assert.NoError(t, err)

		// 验证所有键都被删除
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%d", i)
			exists, err := cache.Exists(ctx, key)
			assert.NoError(t, err)
			assert.False(t, exists)
		}
	})
}

// ============================================================================
// L1Cache TTL测试
// ============================================================================

func TestL1Cache_TTL(t *testing.T) {
	config := &config.L1CacheConfig{
		MaxSize:    1024 * 1024,
		MaxEntries: 100,
		ShardCount: 8,
	}

	cache, err := engine.NewL1Cache(config)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	t.Run("带TTL的条目自动过期", func(t *testing.T) {
		key := "ttl_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 50*time.Millisecond)

		err := cache.Set(ctx, key, entry)
		require.NoError(t, err)

		// 立即获取应该成功
		_, err = cache.Get(ctx, key)
		assert.NoError(t, err)

		// 等待过期
		time.Sleep(100 * time.Millisecond)

		// 获取应该失败
		_, err = cache.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("SetTTL设置过期时间", func(t *testing.T) {
		key := "expire_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := cache.Set(ctx, key, entry)
		require.NoError(t, err)

		// 设置TTL
		err = cache.SetTTL(ctx, key, 100*time.Millisecond)
		assert.NoError(t, err)

		// 获取TTL
		ttl, err := cache.GetTTL(ctx, key)
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 100*time.Millisecond)

		// 等待过期
		time.Sleep(150 * time.Millisecond)

		_, err = cache.Get(ctx, key)
		assert.Error(t, err)
	})

	t.Run("GetTTL获取剩余时间", func(t *testing.T) {
		key := "ttl_test_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 1*time.Second)

		err := cache.Set(ctx, key, entry)
		require.NoError(t, err)

		ttl, err := cache.GetTTL(ctx, key)
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 1*time.Second)
	})
}

// ============================================================================
// L1Cache 统计信息测试
// ============================================================================

func TestL1Cache_Stats(t *testing.T) {
	config := &config.L1CacheConfig{
		MaxSize:    1024 * 1024,
		MaxEntries: 100,
		ShardCount: 8,
	}

	cache, err := engine.NewL1Cache(config)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	t.Run("获取统计信息", func(t *testing.T) {
		// 添加一些数据
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("stats_key_%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("value_%d", i)))
			entry := value.NewEntry(key, bv, 0)
			err := cache.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 获取几次
		cache.Get(ctx, "stats_key_0")
		cache.Get(ctx, "stats_key_1")
		cache.Get(ctx, "nonexistent") // miss

		stats, err := cache.Stats()
		require.NoError(t, err)

		assert.Equal(t, "L1", stats.Level)
		assert.Equal(t, "memory", stats.Type)
		assert.True(t, stats.TotalKeys > 0)
		assert.True(t, stats.GetCount > 0)
		assert.True(t, stats.SetCount > 0)
		assert.True(t, stats.HitCount > 0)
		assert.True(t, stats.MissCount > 0)
		assert.True(t, stats.HitRate >= 0 && stats.HitRate <= 1)
	})

	t.Run("命中率计算", func(t *testing.T) {
		cache.Clear(ctx)

		key := "hit_rate_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)
		cache.Set(ctx, key, entry)

		// 2次命中
		cache.Get(ctx, key)
		cache.Get(ctx, key)

		// 1次未命中
		cache.Get(ctx, "nonexistent")

		stats, err := cache.Stats()
		require.NoError(t, err)

		// 命中率应该是 2/3 = 0.666...
		assert.True(t, stats.HitRate > 0.6 && stats.HitRate < 0.7)
	})
}

// ============================================================================
// L1Cache 容量限制测试
// ============================================================================

func TestL1Cache_Limits(t *testing.T) {
	t.Run("达到最大条目数限制", func(t *testing.T) {
		config := &config.L1CacheConfig{
			MaxSize:    1024 * 1024,
			MaxEntries: 5, // 最多5个条目
			ShardCount: 4,
		}

		cache, err := engine.NewL1Cache(config)
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()

		// 添加5个条目
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key_%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			err := cache.Set(ctx, key, entry)
			assert.NoError(t, err)
		}

		// 添加第6个会触发淘汰
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key_6", bv, 0)
		err = cache.Set(ctx, "key_6", entry)
		// 可能成功（淘汰旧的）或失败（容量限制）
		// 具体取决于实现
		_ = err

		stats, _ := cache.Stats()
		assert.True(t, stats.TotalKeys <= 5)
	})

	t.Run("超大值处理", func(t *testing.T) {
		config := &config.L1CacheConfig{
			MaxSize:    1024, // 1KB限制
			MaxEntries: 100,
			ShardCount: 4,
		}

		cache, err := engine.NewL1Cache(config)
		require.NoError(t, err)
		defer cache.Close()

		ctx := context.Background()

		// 尝试添加超大值
		largeValue := make([]byte, 2048)
		bv := value.NewByteView(largeValue)
		entry := value.NewEntry("large_key", bv, 0)

		err = cache.Set(ctx, "large_key", entry)
		assert.Error(t, err) // 应该失败
	})
}

// ============================================================================
// L1Cache 并发测试
// ============================================================================

func TestL1Cache_Concurrent(t *testing.T) {
	config := &config.L1CacheConfig{
		MaxSize:    1024 * 1024,
		MaxEntries: 1000,
		ShardCount: 32,
	}

	cache, err := engine.NewL1Cache(config)
	require.NoError(t, err)
	defer cache.Close()

	ctx := context.Background()

	t.Run("并发写入", func(t *testing.T) {
		var wg sync.WaitGroup
		numGoroutines := 100

		for i := 0; i < numGoroutines; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()

				key := fmt.Sprintf("concurrent_key_%d", id)
				bv := value.NewByteView([]byte(fmt.Sprintf("value_%d", id)))
				entry := value.NewEntry(key, bv, 0)

				err := cache.Set(ctx, key, entry)
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()

		stats, _ := cache.Stats()
		assert.True(t, stats.TotalKeys > 0)
	})

	t.Run("并发读写", func(t *testing.T) {
		key := "rw_key"
		bv := value.NewByteView([]byte("initial"))
		entry := value.NewEntry(key, bv, 0)
		cache.Set(ctx, key, entry)

		var wg sync.WaitGroup
		numReaders := 50
		numWriters := 50

		// 并发读
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					cache.Get(ctx, key)
				}
			}()
		}

		// 并发写
		for i := 0; i < numWriters; i++ {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					bv := value.NewByteView([]byte(fmt.Sprintf("value_%d", id)))
					entry := value.NewEntry(key, bv, 0)
					cache.Set(ctx, key, entry)
				}
			}(i)
		}

		wg.Wait()

		// 验证最终状态
		_, err := cache.Get(ctx, key)
		assert.NoError(t, err)
	})

	t.Run("并发删除", func(t *testing.T) {
		// 准备数据
		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("del_key_%d", i)
			keys[i] = key
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			cache.Set(ctx, key, entry)
		}

		var wg sync.WaitGroup

		// 并发删除
		for _, key := range keys {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				cache.Delete(ctx, k)
			}(key)
		}

		wg.Wait()

		// 验证所有键都被删除
		for _, key := range keys {
			exists, _ := cache.Exists(ctx, key)
			assert.False(t, exists)
		}
	})
}

// ============================================================================
// L1Cache 关闭测试
// ============================================================================

func TestL1Cache_Close(t *testing.T) {
	t.Run("关闭后操作应失败", func(t *testing.T) {
		config := &config.L1CacheConfig{
			MaxSize:    1024 * 1024,
			MaxEntries: 100,
			ShardCount: 8,
		}

		cache, err := engine.NewL1Cache(config)
		require.NoError(t, err)

		ctx := context.Background()

		// 添加一些数据
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)
		cache.Set(ctx, "key", entry)

		// 关闭
		err = cache.Close()
		assert.NoError(t, err)

		// 所有操作应该失败
		_, err = cache.Get(ctx, "key")
		assert.Error(t, err)

		err = cache.Set(ctx, "key2", entry)
		assert.Error(t, err)

		err = cache.Delete(ctx, "key")
		assert.Error(t, err)
	})

	t.Run("重复关闭应该安全", func(t *testing.T) {
		config := &config.L1CacheConfig{
			MaxSize:    1024 * 1024,
			MaxEntries: 100,
			ShardCount: 8,
		}

		cache, err := engine.NewL1Cache(config)
		require.NoError(t, err)

		err = cache.Close()
		assert.NoError(t, err)

		err = cache.Close()
		assert.NoError(t, err) // 不应该panic
	})
}

// ============================================================================
// L1Cache 性能基准测试
// ============================================================================

func BenchmarkL1Cache_Set(b *testing.B) {
	config := &config.L1CacheConfig{
		MaxSize:    100 * 1024 * 1024,
		MaxEntries: 100000,
		ShardCount: 64,
	}

	cache, _ := engine.NewL1Cache(config)
	defer cache.Close()

	ctx := context.Background()
	bv := value.NewByteView([]byte("benchmark value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		cache.Set(ctx, key, entry)
	}
}

func BenchmarkL1Cache_Get(b *testing.B) {
	config := &config.L1CacheConfig{
		MaxSize:    100 * 1024 * 1024,
		MaxEntries: 100000,
		ShardCount: 64,
	}

	cache, _ := engine.NewL1Cache(config)
	defer cache.Close()

	ctx := context.Background()

	// 准备数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)
		cache.Set(ctx, key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		cache.Get(ctx, key)
	}
}

func BenchmarkL1Cache_Concurrent(b *testing.B) {
	config := &config.L1CacheConfig{
		MaxSize:    100 * 1024 * 1024,
		MaxEntries: 100000,
		ShardCount: 64,
	}

	cache, _ := engine.NewL1Cache(config)
	defer cache.Close()

	ctx := context.Background()
	bv := value.NewByteView([]byte("benchmark value"))

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i)
			entry := value.NewEntry(key, bv, 0)
			cache.Set(ctx, key, entry)
			cache.Get(ctx, key)
			i++
		}
	})
}
