package eviction_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"turbocache/internal/cache/eviction"
	"turbocache/internal/cache/value"
)

// ============================================================================
// LFUPolicy 核心功能测试
// ============================================================================

func TestLFUPolicy_Basic(t *testing.T) {
	policy := eviction.NewLFUPolicy(&eviction.Config{MaxEntries: 3})
	defer policy.Close()

	t.Run("基础设置和访问", func(t *testing.T) {
		bv := value.NewByteView([]byte("value1"))
		entry := value.NewEntry("key1", bv, 0)
		policy.OnSet("key1", entry)

		stats := policy.Stats()
		assert.Equal(t, int64(1), stats.TotalKeys)
	})

	t.Run("淘汰最少使用", func(t *testing.T) {
		// 添加3个条目
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			policy.OnSet(key, value.NewEntry(key, bv, 0))
		}

		// 频繁访问key1和key2
		for i := 0; i < 5; i++ {
			policy.OnAccess("key1")
			policy.OnAccess("key2")
		}

		// 添加第4个，应该淘汰key3（访问频率最低）
		bv := value.NewByteView([]byte("value4"))
		policy.OnSet("key4", value.NewEntry("key4", bv, 0))

		keys := policy.SelectEvictKeys(3)
		keySet := make(map[string]bool)
		for _, k := range keys {
			keySet[k] = true
		}

		// key3应该被淘汰（不在列表中）
		assert.False(t, keySet["key3"])
		// key1和key2应该保留
		assert.True(t, keySet["key1"])
		assert.True(t, keySet["key2"])
	})

	t.Run("统计信息", func(t *testing.T) {
		stats := policy.Stats()
		assert.Equal(t, "lfu", stats.Type)
		assert.NotNil(t, stats.Extra)
		assert.Contains(t, stats.Extra, "min_frequency")
	})
}

func TestLFUPolicy_FrequencyTracking(t *testing.T) {
	policy := eviction.NewLFUPolicy(&eviction.Config{MaxEntries: 10})
	defer policy.Close()

	t.Run("访问频率影响淘汰顺序", func(t *testing.T) {
		// 添加条目
		for i := 1; i <= 5; i++ {
			key := fmt.Sprintf("key%d", i)
			bv := value.NewByteView([]byte("value"))
			policy.OnSet(key, value.NewEntry(key, bv, 0))
		}

		// key1访问1次，key2访问2次，key3访问3次
		policy.OnAccess("key1")

		policy.OnAccess("key2")
		policy.OnAccess("key2")

		policy.OnAccess("key3")
		policy.OnAccess("key3")
		policy.OnAccess("key3")

		// 选择淘汰2个，应该是频率最低的
		keys := policy.SelectEvictKeys(2)

		// key4和key5应该在淘汰列表中（未访问过）
		assert.Contains(t, keys, "key4")
		assert.Contains(t, keys, "key5")
	})
}

// ============================================================================
// LFUCache 核心功能测试
// ============================================================================

func TestLFUCache_Basic(t *testing.T) {
	cache := eviction.NewLFUCache(3)

	t.Run("添加和获取", func(t *testing.T) {
		bv := value.NewByteView([]byte("value"))
		entry := value.NewEntry("key", bv, 0)

		added := cache.Add("key", entry)
		assert.True(t, added)

		retrieved, found := cache.Get("key")
		assert.True(t, found)
		assert.NotNil(t, retrieved)
	})

	t.Run("淘汰低频条目", func(t *testing.T) {
		// 添加3个条目
		for i := 1; i <= 3; i++ {
			key := fmt.Sprintf("k%d", i)
			bv := value.NewByteView([]byte("v"))
			cache.Add(key, value.NewEntry(key, bv, 0))
		}

		// 访问k1和k2多次
		for i := 0; i < 3; i++ {
			cache.Get("k1")
			cache.Get("k2")
		}

		// 添加第4个
		bv := value.NewByteView([]byte("v4"))
		cache.Add("k4", value.NewEntry("k4", bv, 0))

		// k3应该被淘汰（频率最低）
		_, found := cache.Get("k3")
		assert.False(t, found)
	})

	t.Run("清空和统计", func(t *testing.T) {
		cache.Clear()
		assert.Equal(t, 0, cache.Len())

		stats := cache.Stats()
		assert.Equal(t, "lfu", stats.Type)
	})
}

// ============================================================================
// LFU 性能基准测试
// ============================================================================

func BenchmarkLFUPolicy_OnSet(b *testing.B) {
	policy := eviction.NewLFUPolicy(&eviction.Config{MaxEntries: 10000})
	defer policy.Close()

	bv := value.NewByteView([]byte("value"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%1000)
		policy.OnSet(key, value.NewEntry(key, bv, 0))
	}
}

func BenchmarkLFUCache_Get(b *testing.B) {
	cache := eviction.NewLFUCache(1000)
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
