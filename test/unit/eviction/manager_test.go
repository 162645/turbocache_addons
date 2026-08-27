package eviction_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"turbocache/internal/cache/eviction"
	"turbocache/internal/cache/value"
)

// ============================================================================
// Manager 基础功能测试
// ============================================================================

func TestManager_Creation(t *testing.T) {
	t.Run("使用默认配置创建", func(t *testing.T) {
		manager, err := eviction.NewManager(nil)
		require.NoError(t, err)
		require.NotNil(t, manager)

		stats := manager.Stats()
		assert.Equal(t, "lru", stats.Type)

		err = manager.Close()
		assert.NoError(t, err)
	})

	t.Run("创建LRU管理器", func(t *testing.T) {
		config := &eviction.Config{
			Type:       "lru",
			MaxEntries: 100,
		}

		manager, err := eviction.NewManager(config)
		require.NoError(t, err)
		require.NotNil(t, manager)

		stats := manager.Stats()
		assert.Equal(t, "lru", stats.Type)

		err = manager.Close()
		assert.NoError(t, err)
	})

	t.Run("创建LFU管理器", func(t *testing.T) {
		config := &eviction.Config{
			Type:       "lfu",
			MaxEntries: 100,
		}

		manager, err := eviction.NewManager(config)
		require.NoError(t, err)
		require.NotNil(t, manager)

		stats := manager.Stats()
		assert.Equal(t, "lfu", stats.Type)

		err = manager.Close()
		assert.NoError(t, err)
	})

	t.Run("创建ARC管理器", func(t *testing.T) {
		config := &eviction.Config{
			Type:       "arc",
			MaxEntries: 100,
		}

		manager, err := eviction.NewManager(config)
		require.NoError(t, err)
		require.NotNil(t, manager)

		stats := manager.Stats()
		assert.Equal(t, "arc", stats.Type)

		err = manager.Close()
		assert.NoError(t, err)
	})

	t.Run("创建无淘汰管理器", func(t *testing.T) {
		config := &eviction.Config{
			Type: "none",
		}

		manager, err := eviction.NewManager(config)
		require.NoError(t, err)
		require.NotNil(t, manager)

		stats := manager.Stats()
		assert.Equal(t, "none", stats.Type)

		err = manager.Close()
		assert.NoError(t, err)
	})

	t.Run("不支持的策略类型应失败", func(t *testing.T) {
		config := &eviction.Config{
			Type: "unsupported",
		}

		_, err := eviction.NewManager(config)
		assert.Error(t, err)
	})
}

func TestManager_Operations(t *testing.T) {
	manager, err := eviction.NewManager(&eviction.Config{
		Type:       "lru",
		MaxEntries: 10,
	})
	require.NoError(t, err)
	defer manager.Close()

	t.Run("OnSet操作", func(t *testing.T) {
		bv := value.NewByteView([]byte("test value"))
		entry := value.NewEntry("key1", bv, 0)

		manager.OnSet("key1", entry)

		stats := manager.Stats()
		assert.True(t, stats.TotalKeys > 0)
	})

	t.Run("OnAccess操作", func(t *testing.T) {
		manager.OnAccess("key1")

		stats := manager.Stats()
		assert.True(t, stats.AccessCount > 0)
	})

	t.Run("OnDelete操作", func(t *testing.T) {
		before := manager.Stats().TotalKeys

		manager.OnDelete("key1")

		after := manager.Stats().TotalKeys
		assert.Equal(t, before-1, after)
	})

	t.Run("SelectEvictKeys操作", func(t *testing.T) {
		// 添加一些条目
		for i := 0; i < 5; i++ {
			key := string(rune('a' + i))
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			manager.OnSet(key, entry)
		}

		keys := manager.SelectEvictKeys(3)
		assert.True(t, len(keys) <= 3)
	})

	t.Run("Reset操作", func(t *testing.T) {
		manager.Reset()

		stats := manager.Stats()
		assert.Equal(t, int64(0), stats.TotalKeys)
	})
}

func TestManager_Stats(t *testing.T) {
	manager, err := eviction.NewManager(&eviction.Config{
		Type:       "lru",
		MaxEntries: 10,
	})
	require.NoError(t, err)
	defer manager.Close()

	t.Run("获取统计信息", func(t *testing.T) {
		// 添加一些操作
		for i := 0; i < 3; i++ {
			key := string(rune('a' + i))
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry(key, bv, 0)
			manager.OnSet(key, entry)
		}

		manager.OnAccess("a")

		stats := manager.Stats()
		assert.Equal(t, "lru", stats.Type)
		assert.True(t, stats.TotalKeys > 0)
		assert.True(t, stats.AccessCount > 0)
		assert.NotNil(t, stats.Extra)
	})
}

func TestManager_Close(t *testing.T) {
	t.Run("关闭管理器", func(t *testing.T) {
		manager, err := eviction.NewManager(&eviction.Config{
			Type:       "lru",
			MaxEntries: 10,
		})
		require.NoError(t, err)

		err = manager.Close()
		assert.NoError(t, err)
	})

	t.Run("重复关闭应该安全", func(t *testing.T) {
		manager, err := eviction.NewManager(&eviction.Config{
			Type:       "lru",
			MaxEntries: 10,
		})
		require.NoError(t, err)

		err = manager.Close()
		assert.NoError(t, err)

		err = manager.Close()
		assert.NoError(t, err)
	})
}

// ============================================================================
// Manager 策略切换测试
// ============================================================================

func TestManager_PolicyTypes(t *testing.T) {
	policyTypes := []string{"lru", "lfu", "arc", "none"}

	for _, policyType := range policyTypes {
		t.Run("测试"+policyType+"策略", func(t *testing.T) {
			config := &eviction.Config{
				Type:       policyType,
				MaxEntries: 10,
			}

			manager, err := eviction.NewManager(config)
			require.NoError(t, err)
			defer manager.Close()

			// 基本操作测试
			bv := value.NewByteView([]byte("value"))
			entry := value.NewEntry("key", bv, 0)

			manager.OnSet("key", entry)
			manager.OnAccess("key")

			stats := manager.Stats()
			assert.Equal(t, policyType, stats.Type)

			if policyType != "none" {
				assert.True(t, stats.TotalKeys > 0)
			}
		})
	}
}

// ============================================================================
// NoEvictionPolicy 测试
// ============================================================================

func TestNoEvictionPolicy(t *testing.T) {
	policy := &eviction.NoEvictionPolicy{}

	t.Run("所有操作都是空操作", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)

		// 不应该崩溃
		assert.NotPanics(t, func() {
			policy.OnAccess("key")
			policy.OnSet("key", entry)
			policy.OnDelete("key")
			policy.Reset()
		})
	})

	t.Run("SelectEvictKeys返回空", func(t *testing.T) {
		keys := policy.SelectEvictKeys(10)
		assert.Nil(t, keys)
	})

	t.Run("Stats返回默认值", func(t *testing.T) {
		stats := policy.Stats()
		assert.Equal(t, "none", stats.Type)
		assert.Equal(t, int64(0), stats.TotalKeys)
	})

	t.Run("Close操作", func(t *testing.T) {
		err := policy.Close()
		assert.NoError(t, err)
	})
}

// ============================================================================
// Item 辅助结构测试
// ============================================================================

func TestItem_Creation(t *testing.T) {
	t.Run("创建Item", func(t *testing.T) {
		bv := value.NewByteView([]byte("test value"))
		entry := value.NewEntry("key", bv, 0)

		item := eviction.NewItem("key", entry)
		require.NotNil(t, item)

		assert.Equal(t, "key", item.Key)
		assert.Equal(t, entry, item.Entry)
		assert.Equal(t, int64(1), item.AccessCount)
		assert.Equal(t, entry.Size(), item.Size)
	})
}

func TestItem_OnAccess(t *testing.T) {
	t.Run("访问更新时间和计数", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)
		item := eviction.NewItem("key", entry)

		initialTime := item.AccessTime
		initialCount := item.AccessCount

		item.OnAccess()

		assert.True(t, item.AccessTime.After(initialTime))
		assert.Equal(t, initialCount+1, item.AccessCount)
	})
}

func TestItem_Metrics(t *testing.T) {
	bv := value.NewByteView([]byte("value"))
	entry := value.NewEntry("key", bv, 0)
	item := eviction.NewItem("key", entry)

	t.Run("Age计算", func(t *testing.T) {
		age := item.Age()
		assert.True(t, age >= 0)
	})

	t.Run("IdleTime计算", func(t *testing.T) {
		idle := item.IdleTime()
		assert.True(t, idle >= 0)
	})

	t.Run("Frequency计算", func(t *testing.T) {
		item.OnAccess()
		item.OnAccess()

		freq := item.Frequency()
		assert.True(t, freq > 0)
	})
}

// ============================================================================
// 性能基准测试
// ============================================================================

func BenchmarkManager_OnSet(b *testing.B) {
	manager, _ := eviction.NewManager(&eviction.Config{
		Type:       "lru",
		MaxEntries: 10000,
	})
	defer manager.Close()

	bv := value.NewByteView([]byte("benchmark value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune(i % 1000))
		entry := value.NewEntry(key, bv, 0)
		manager.OnSet(key, entry)
	}
}

func BenchmarkManager_OnAccess(b *testing.B) {
	manager, _ := eviction.NewManager(&eviction.Config{
		Type:       "lru",
		MaxEntries: 10000,
	})
	defer manager.Close()

	// 准备数据
	bv := value.NewByteView([]byte("value"))
	for i := 0; i < 100; i++ {
		key := string(rune(i))
		entry := value.NewEntry(key, bv, 0)
		manager.OnSet(key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := string(rune(i % 100))
		manager.OnAccess(key)
	}
}

func BenchmarkManager_SelectEvictKeys(b *testing.B) {
	manager, _ := eviction.NewManager(&eviction.Config{
		Type:       "lru",
		MaxEntries: 10000,
	})
	defer manager.Close()

	// 准备数据
	bv := value.NewByteView([]byte("value"))
	for i := 0; i < 1000; i++ {
		key := string(rune(i))
		entry := value.NewEntry(key, bv, 0)
		manager.OnSet(key, entry)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manager.SelectEvictKeys(10)
	}
}

func BenchmarkManager_Stats(b *testing.B) {
	manager, _ := eviction.NewManager(&eviction.Config{
		Type:       "lru",
		MaxEntries: 10000,
	})
	defer manager.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = manager.Stats()
	}
}

// ============================================================================
// 不同策略性能对比
// ============================================================================

func BenchmarkPolicyComparison_Set(b *testing.B) {
	policies := []string{"lru", "lfu", "arc"}

	for _, policyType := range policies {
		b.Run(policyType, func(b *testing.B) {
			manager, _ := eviction.NewManager(&eviction.Config{
				Type:       policyType,
				MaxEntries: 1000,
			})
			defer manager.Close()

			bv := value.NewByteView([]byte("value"))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := string(rune(i % 1000))
				entry := value.NewEntry(key, bv, 0)
				manager.OnSet(key, entry)
			}
		})
	}
}

func BenchmarkPolicyComparison_Access(b *testing.B) {
	policies := []string{"lru", "lfu", "arc"}

	for _, policyType := range policies {
		b.Run(policyType, func(b *testing.B) {
			manager, _ := eviction.NewManager(&eviction.Config{
				Type:       policyType,
				MaxEntries: 1000,
			})
			defer manager.Close()

			// 准备数据
			bv := value.NewByteView([]byte("value"))
			for i := 0; i < 100; i++ {
				key := string(rune(i))
				entry := value.NewEntry(key, bv, 0)
				manager.OnSet(key, entry)
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				key := string(rune(i % 100))
				manager.OnAccess(key)
			}
		})
	}
}
