package cache_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"turbocache/internal/cache/storage"
	"turbocache/internal/cache/value"
)

// ============================================================================
// MemoryStorage 基础操作测试
// ============================================================================

func TestMemoryStorage_Creation(t *testing.T) {
	t.Run("使用默认配置创建", func(t *testing.T) {
		store := storage.NewMemoryStorage(nil)
		require.NotNil(t, store)

		err := store.Close()
		assert.NoError(t, err)
	})

	t.Run("使用自定义配置创建", func(t *testing.T) {
		config := &storage.Config{
			MaxSize:       1024 * 1024,
			MaxEntries:    1000,
			ShardCount:    32,
			CleanInterval: 30 * time.Second,
		}

		store := storage.NewMemoryStorage(config)
		require.NotNil(t, store)

		err := store.Close()
		assert.NoError(t, err)
	})
}

func TestMemoryStorage_GetSet(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("设置和获取值", func(t *testing.T) {
		key := "test_key"
		bv := value.NewByteView([]byte("test value"))
		entry := value.NewEntry(key, bv, 0)

		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		retrieved, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "test value", retrieved.Value.String())
	})

	t.Run("获取不存在的键", func(t *testing.T) {
		_, err := store.Get(ctx, "nonexistent")
		assert.Error(t, err)
		assert.True(t, storage.IsNotFound(err))
	})

	t.Run("设置空键应失败", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("", bv, 0)

		err := store.Set(ctx, "", entry)
		assert.Error(t, err)
	})

	t.Run("设置nil条目应失败", func(t *testing.T) {
		err := store.Set(ctx, "key", nil)
		assert.Error(t, err)
	})

	t.Run("更新已存在的键", func(t *testing.T) {
		key := "update_key"
		bv1 := value.NewByteView([]byte("value1"))
		entry1 := value.NewEntry(key, bv1, 0)

		err := store.Set(ctx, key, entry1)
		require.NoError(t, err)

		bv2 := value.NewByteView([]byte("value2"))
		entry2 := value.NewEntry(key, bv2, 0)

		err = store.Set(ctx, key, entry2)
		require.NoError(t, err)

		retrieved, err := store.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, "value2", retrieved.Value.String())
	})
}

func TestMemoryStorage_Delete(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("删除存在的键", func(t *testing.T) {
		key := "delete_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		err = store.Delete(ctx, key)
		assert.NoError(t, err)

		_, err = store.Get(ctx, key)
		assert.Error(t, err)
		assert.True(t, storage.IsNotFound(err))
	})

	t.Run("删除不存在的键", func(t *testing.T) {
		err := store.Delete(ctx, "nonexistent")
		assert.Error(t, err)
		assert.True(t, storage.IsNotFound(err))
	})
}

func TestMemoryStorage_Exists(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("检查存在的键", func(t *testing.T) {
		key := "exists_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		exists, err := store.Exists(ctx, key)
		assert.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("检查不存在的键", func(t *testing.T) {
		exists, err := store.Exists(ctx, "nonexistent")
		assert.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestMemoryStorage_TTL(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("带TTL的条目自动过期", func(t *testing.T) {
		key := "ttl_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 50*time.Millisecond)

		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		// 立即获取应该成功
		_, err = store.Get(ctx, key)
		assert.NoError(t, err)

		// 等待过期
		time.Sleep(100 * time.Millisecond)

		// 获取应该失败
		_, err = store.Get(ctx, key)
		assert.Error(t, err)
		assert.True(t, storage.IsNotFound(err))
	})

	t.Run("设置键的TTL", func(t *testing.T) {
		key := "expire_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		// 设置TTL
		err = store.Expire(ctx, key, 100*time.Millisecond)
		assert.NoError(t, err)

		// 获取TTL
		ttl, err := store.TTL(ctx, key)
		assert.NoError(t, err)
		assert.True(t, ttl > 0 && ttl <= 100*time.Millisecond)
	})

	t.Run("获取不过期键的TTL", func(t *testing.T) {
		key := "no_ttl_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		ttl, err := store.TTL(ctx, key)
		assert.NoError(t, err)
		assert.Equal(t, time.Duration(-1), ttl)
	})

	t.Run("ExpireAt设置绝对过期时间", func(t *testing.T) {
		key := "expire_at_key"
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)

		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		expireTime := time.Now().Add(50 * time.Millisecond)
		err = store.ExpireAt(ctx, key, expireTime)
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		_, err = store.Get(ctx, key)
		assert.Error(t, err)
	})
}

func TestMemoryStorage_Clear(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("清空存储", func(t *testing.T) {
		// 添加多个键
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key_%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("value_%d", i)))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 清空
		err := store.Clear(ctx)
		assert.NoError(t, err)

		// 验证所有键都被删除
		count, err := store.Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)

		size, err := store.Size()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), size)
	})
}

// ============================================================================
// MemoryStorage 批量操作测试
// ============================================================================

func TestMemoryStorage_BatchGet(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("批量获取", func(t *testing.T) {
		// 设置测试数据
		keys := []string{"key1", "key2", "key3"}
		for _, key := range keys {
			bv := value.NewByteView([]byte(key + "_value"))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 批量获取
		results, err := store.BatchGet(ctx, keys)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(results))

		for _, key := range keys {
			entry, exists := results[key]
			assert.True(t, exists)
			assert.Equal(t, key+"_value", entry.Value.String())
		}
	})

	t.Run("批量获取包含不存在的键", func(t *testing.T) {
		keys := []string{"key1", "nonexistent", "key2"}

		results, err := store.BatchGet(ctx, keys)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(results)) // 只返回存在的键

		_, exists := results["nonexistent"]
		assert.False(t, exists)
	})

	t.Run("批量获取空列表", func(t *testing.T) {
		results, err := store.BatchGet(ctx, []string{})
		assert.NoError(t, err)
		assert.Equal(t, 0, len(results))
	})
}

func TestMemoryStorage_BatchSet(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("批量设置", func(t *testing.T) {
		entries := make(map[string]*value.Entry)
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("batch_key_%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("batch_value_%d", i)))
			entries[key] = value.NewEntry(key, bv, 0)
		}

		err := store.BatchSet(ctx, entries)
		assert.NoError(t, err)

		// 验证所有条目都已设置
		for key, expectedEntry := range entries {
			retrieved, err := store.Get(ctx, key)
			require.NoError(t, err)
			assert.Equal(t, expectedEntry.Value.String(), retrieved.Value.String())
		}
	})

	t.Run("批量设置空map", func(t *testing.T) {
		err := store.BatchSet(ctx, map[string]*value.Entry{})
		assert.NoError(t, err)
	})
}

func TestMemoryStorage_BatchDelete(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("批量删除", func(t *testing.T) {
		// 设置测试数据
		keys := []string{"del1", "del2", "del3"}
		for _, key := range keys {
			bv := value.NewByteView([]byte(key + "_value"))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 批量删除
		err := store.BatchDelete(ctx, keys)
		assert.NoError(t, err)

		// 验证所有键都已删除
		for _, key := range keys {
			_, err := store.Get(ctx, key)
			assert.Error(t, err)
			assert.True(t, storage.IsNotFound(err))
		}
	})
}

// ============================================================================
// MemoryStorage 迭代器测试
// ============================================================================

func TestMemoryStorage_Iterator(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("迭代所有条目", func(t *testing.T) {
		// 设置测试数据
		expected := make(map[string]string)
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("iter_key_%d", i)
			val := fmt.Sprintf("iter_value_%d", i)
			bv := value.NewByteView([]byte(val))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
			expected[key] = val
		}

		// 迭代
		iter, err := store.Iterator(ctx)
		require.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			key := iter.Key()
			entry, err := iter.Value()
			require.NoError(t, err)

			expectedVal, exists := expected[key]
			assert.True(t, exists)
			assert.Equal(t, expectedVal, entry.Value.String())
			count++
		}

		assert.NoError(t, iter.Error())
		assert.Equal(t, len(expected), count)
	})

	t.Run("前缀迭代", func(t *testing.T) {
		// 设置测试数据
		prefixKeys := []string{"prefix:key1", "prefix:key2", "prefix:key3"}
		otherKeys := []string{"other:key1", "other:key2"}

		for _, key := range append(prefixKeys, otherKeys...) {
			bv := value.NewByteView([]byte(key + "_value"))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 前缀迭代
		iter, err := store.IteratorWithPrefix(ctx, "prefix:")
		require.NoError(t, err)
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		assert.Equal(t, len(prefixKeys), count)
	})

	t.Run("Seek定位", func(t *testing.T) {
		iter, err := store.Iterator(ctx)
		require.NoError(t, err)
		defer iter.Close()

		// 定位到特定键
		found := iter.Seek("prefix:key2")
		if found {
			assert.Equal(t, "prefix:key2", iter.Key())
		}
	})

	t.Run("Reset重置迭代器", func(t *testing.T) {
		iter, err := store.Iterator(ctx)
		require.NoError(t, err)
		defer iter.Close()

		// 迭代几次
		iter.Next()
		iter.Next()

		// 重置
		err = iter.Reset()
		assert.NoError(t, err)

		// 应该从头开始
		iter.Next()
		firstKey := iter.Key()
		assert.NotEmpty(t, firstKey)
	})
}

// ============================================================================
// MemoryStorage 统计信息测试
// ============================================================================

func TestMemoryStorage_Stats(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	t.Run("获取统计信息", func(t *testing.T) {
		// 添加一些数据
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("stats_key_%d", i)
			bv := value.NewByteView([]byte(fmt.Sprintf("value_%d", i)))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 获取统计
		stats, err := store.Stats()
		require.NoError(t, err)
		assert.True(t, stats.TotalKeys > 0)
		assert.True(t, stats.TotalSize > 0)
		assert.True(t, stats.SetCount > 0)
	})

	t.Run("Size统计", func(t *testing.T) {
		size, err := store.Size()
		assert.NoError(t, err)
		assert.True(t, size > 0)
	})

	t.Run("Count统计", func(t *testing.T) {
		count, err := store.Count()
		assert.NoError(t, err)
		assert.True(t, count > 0)
	})
}

// ============================================================================
// MemoryStorage 容量限制测试
// ============================================================================

func TestMemoryStorage_SizeLimits(t *testing.T) {
	t.Run("超过最大大小限制", func(t *testing.T) {
		config := &storage.Config{
			MaxSize:    1024, // 1KB限制
			MaxEntries: 1000,
			ShardCount: 4,
		}

		store := storage.NewMemoryStorage(config)
		defer store.Close()

		ctx := context.Background()

		// 尝试添加超大值
		largeValue := make([]byte, 2048)
		bv := value.NewByteView(largeValue)
		entry := value.NewEntry("large_key", bv, 0)

		err := store.Set(ctx, "large_key", entry)
		assert.Error(t, err) // 应该失败
	})

	t.Run("超过最大条目数限制", func(t *testing.T) {
		config := &storage.Config{
			MaxSize:    1024 * 1024,
			MaxEntries: 5, // 最多5个条目
			ShardCount: 4,
		}

		store := storage.NewMemoryStorage(config)
		defer store.Close()

		ctx := context.Background()

		// 添加5个条目应该成功
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key_%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			assert.NoError(t, err)
		}

		// 第6个应该失败
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key_6", bv, 0)
		err := store.Set(ctx, "key_6", entry)
		assert.Error(t, err)
	})
}

// ============================================================================
// MemoryStorage 并发测试
// ============================================================================

func TestMemoryStorage_Concurrent(t *testing.T) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

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

				err := store.Set(ctx, key, entry)
				assert.NoError(t, err)
			}(i)
		}

		wg.Wait()

		// 验证所有数据都已写入
		count, err := store.Count()
		assert.NoError(t, err)
		assert.True(t, count >= int64(numGoroutines))
	})

	t.Run("并发读写", func(t *testing.T) {
		key := "rw_key"
		bv := value.NewByteView([]byte("initial"))
		entry := value.NewEntry(key, bv, 0)
		err := store.Set(ctx, key, entry)
		require.NoError(t, err)

		var wg sync.WaitGroup
		numReaders := 50
		numWriters := 50

		// 并发读
		for i := 0; i < numReaders; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					_, _ = store.Get(ctx, key)
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
					_ = store.Set(ctx, key, entry)
				}
			}(i)
		}

		wg.Wait()

		// 验证最终状态
		_, err = store.Get(ctx, key)
		assert.NoError(t, err)
	})

	t.Run("并发删除", func(t *testing.T) {
		// 准备测试数据
		keys := make([]string, 100)
		for i := 0; i < 100; i++ {
			key := fmt.Sprintf("del_key_%d", i)
			keys[i] = key
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		var wg sync.WaitGroup

		// 并发删除
		for _, key := range keys {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				_ = store.Delete(ctx, k)
			}(key)
		}

		wg.Wait()

		// 验证所有键都被删除
		for _, key := range keys {
			_, err := store.Get(ctx, key)
			assert.Error(t, err)
		}
	})
}

// ============================================================================
// MemoryStorage 关闭测试
// ============================================================================

func TestMemoryStorage_Close(t *testing.T) {
	t.Run("关闭后操作应失败", func(t *testing.T) {
		store := storage.NewMemoryStorage(nil)
		ctx := context.Background()

		// 添加一些数据
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)
		err := store.Set(ctx, "key", entry)
		require.NoError(t, err)

		// 关闭
		err = store.Close()
		assert.NoError(t, err)

		// 所有操作应该失败
		_, err = store.Get(ctx, "key")
		assert.Error(t, err)

		err = store.Set(ctx, "key2", entry)
		assert.Error(t, err)

		err = store.Delete(ctx, "key")
		assert.Error(t, err)
	})

	t.Run("重复关闭应该安全", func(t *testing.T) {
		store := storage.NewMemoryStorage(nil)

		err := store.Close()
		assert.NoError(t, err)

		err = store.Close()
		assert.NoError(t, err) // 不应该panic或错误
	})
}

// ============================================================================
// MemoryStorage 清理过期条目测试
// ============================================================================

func TestMemoryStorage_Cleanup(t *testing.T) {
	t.Run("自动清理过期条目", func(t *testing.T) {
		config := &storage.Config{
			MaxSize:       1024 * 1024,
			MaxEntries:    1000,
			ShardCount:    4,
			CleanInterval: 100 * time.Millisecond, // 快速清理
		}

		store := storage.NewMemoryStorage(config)
		defer store.Close()

		ctx := context.Background()

		// 添加短TTL的条目
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("expire_key_%d", i)
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 50*time.Millisecond)
			err := store.Set(ctx, key, entry)
			require.NoError(t, err)
		}

		// 等待清理
		time.Sleep(300 * time.Millisecond)

		// 验证过期条目已被清理
		count, err := store.Count()
		assert.NoError(t, err)
		assert.Equal(t, int64(0), count)
	})
}

// ============================================================================
// StorageFactory 测试
// ============================================================================

func TestMemoryStorageFactory(t *testing.T) {
	factory := &storage.MemoryStorageFactory{}

	t.Run("创建存储实例", func(t *testing.T) {
		config := &storage.Config{
			MaxSize:    1024,
			MaxEntries: 100,
			ShardCount: 4,
		}

		store, err := factory.CreateStorage(config)
		require.NoError(t, err)
		require.NotNil(t, store)

		err = store.Close()
		assert.NoError(t, err)
	})

	t.Run("支持的存储类型", func(t *testing.T) {
		types := factory.SupportedTypes()
		assert.Contains(t, types, "memory")
	})

	t.Run("验证配置", func(t *testing.T) {
		validConfig := &storage.Config{
			MaxSize:    1024,
			MaxEntries: 100,
			ShardCount: 4,
		}

		err := factory.ValidateConfig(validConfig)
		assert.NoError(t, err)
	})

	t.Run("验证无效配置", func(t *testing.T) {
		// nil配置
		err := factory.ValidateConfig(nil)
		assert.Error(t, err)

		// 负数MaxSize
		invalidConfig := &storage.Config{
			MaxSize:    -1,
			MaxEntries: 100,
			ShardCount: 4,
		}
		err = factory.ValidateConfig(invalidConfig)
		assert.Error(t, err)

		// 负数MaxEntries
		invalidConfig.MaxSize = 1024
		invalidConfig.MaxEntries = -1
		err = factory.ValidateConfig(invalidConfig)
		assert.Error(t, err)

		// 无效ShardCount
		invalidConfig.MaxEntries = 100
		invalidConfig.ShardCount = 0
		err = factory.ValidateConfig(invalidConfig)
		assert.Error(t, err)
	})
}

// ============================================================================
// Storage Registry 测试
// ============================================================================

func TestStorageRegistry(t *testing.T) {
	t.Run("注册和创建存储", func(t *testing.T) {
		registry := storage.NewRegistry()
		factory := &storage.MemoryStorageFactory{}

		registry.Register("memory", factory)

		config := &storage.Config{
			MaxSize:    1024,
			MaxEntries: 100,
			ShardCount: 4,
		}

		store, err := registry.Create("memory", config)
		require.NoError(t, err)
		require.NotNil(t, store)

		err = store.Close()
		assert.NoError(t, err)
	})

	t.Run("列出存储类型", func(t *testing.T) {
		registry := storage.NewRegistry()
		factory := &storage.MemoryStorageFactory{}

		registry.Register("memory", factory)

		types := registry.List()
		assert.Contains(t, types, "memory")
	})

	t.Run("创建不支持的存储类型", func(t *testing.T) {
		registry := storage.NewRegistry()

		config := &storage.Config{}
		_, err := registry.Create("unsupported", config)
		assert.Error(t, err)
	})
}

// ============================================================================
// 性能基准测试
// ============================================================================

func BenchmarkMemoryStorage_Set(b *testing.B) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()
	bv := value.NewByteView([]byte("benchmark value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i)
		entry := value.NewEntry(key, bv, 0)
		_ = store.Set(ctx, key, entry)
	}
}

func BenchmarkMemoryStorage_Get(b *testing.B) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	// 准备数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)
		_ = store.Set(ctx, key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		_, _ = store.Get(ctx, key)
	}
}

func BenchmarkMemoryStorage_Delete(b *testing.B) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// 准备数据
		key := fmt.Sprintf("key_%d", i)
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)
		_ = store.Set(ctx, key, entry)
		b.StartTimer()

		_ = store.Delete(ctx, key)
	}
}

func BenchmarkMemoryStorage_Concurrent(b *testing.B) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()
	bv := value.NewByteView([]byte("benchmark value"))

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i)
			entry := value.NewEntry(key, bv, 0)
			_ = store.Set(ctx, key, entry)
			_, _ = store.Get(ctx, key)
			i++
		}
	})
}

func BenchmarkMemoryStorage_BatchSet(b *testing.B) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()
	bv := value.NewByteView([]byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		entries := make(map[string]*value.Entry)
		for j := 0; j < 100; j++ {
			key := fmt.Sprintf("key_%d_%d", i, j)
			entries[key] = value.NewEntry(key, bv, 0)
		}
		_ = store.BatchSet(ctx, entries)
	}
}

func BenchmarkMemoryStorage_Iterator(b *testing.B) {
	store := storage.NewMemoryStorage(nil)
	defer store.Close()

	ctx := context.Background()

	// 准备数据
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry(key, bv, 0)
		_ = store.Set(ctx, key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := store.Iterator(ctx)
		for iter.Next() {
			_ = iter.Key()
		}
		iter.Close()
	}
}
