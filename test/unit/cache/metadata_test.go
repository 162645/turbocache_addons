package cache_test

import (
	"sync"
	"testing"
	"time"

	v "turbocache/internal/cache/value"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Metadata 基础功能测试
// ============================================================================

func TestMetadata_Creation(t *testing.T) {
	t.Run("创建（包含 TTL）", func(t *testing.T) {
		ttl := 5 * time.Minute
		meta := v.NewMetadata(100, 1024, ttl)

		require.NotNil(t, meta)
		assert.Equal(t, int64(100), meta.KeySize())
		assert.Equal(t, int64(1024), meta.ValueSize())
		assert.Equal(t, int64(1124), meta.TotalSize())
		assert.InDelta(t, ttl.Seconds(), meta.TTL().Seconds(), 1.0)
	})

	t.Run("创建（无 TTL）", func(t *testing.T) {
		meta := v.NewMetadata(50, 500, 0)

		assert.Equal(t, time.Duration(-1), meta.TTL())
		assert.False(t, meta.IsExpired())
	})
}

func TestMetadata_TimeTracking(t *testing.T) {
	t.Run("初始时间戳", func(t *testing.T) {
		before := time.Now()
		meta := v.NewMetadata(10, 100, 0)
		after := time.Now()

		assert.True(t, meta.CreatedAt().After(before) || meta.CreatedAt().Equal(before))
		assert.True(t, meta.CreatedAt().Before(after) || meta.CreatedAt().Equal(after))
		assert.Equal(t, meta.CreatedAt(), meta.AccessedAt())
		assert.Equal(t, meta.CreatedAt(), meta.UpdatedAt())
	})

	t.Run("访问时间更新", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		initial := meta.AccessedAt()

		time.Sleep(10 * time.Millisecond)
		meta.OnAccess()

		assert.True(t, meta.AccessedAt().After(initial))
		assert.Equal(t, int64(2), meta.AccessCount())
	})

	t.Run("命中计数跟踪", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)

		meta.OnHit()
		meta.OnHit()
		meta.OnHit()

		assert.Equal(t, int64(3), meta.HitCount())
		assert.Equal(t, int64(4), meta.AccessCount()) // +1 from creation
	})

	t.Run("更新追踪", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		initial := meta.UpdatedAt()

		time.Sleep(10 * time.Millisecond)
		meta.OnUpdate(200)

		assert.True(t, meta.UpdatedAt().After(initial))
		assert.Equal(t, int64(200), meta.ValueSize())
		assert.Equal(t, int64(1), meta.UpdateCount())
	})
}

func TestMetadata_TTL(t *testing.T) {
	t.Run("TTL 过期", func(t *testing.T) {
		ttl := 50 * time.Millisecond
		meta := v.NewMetadata(10, 100, ttl)

		assert.False(t, meta.IsExpired())

		time.Sleep(100 * time.Millisecond)
		assert.True(t, meta.IsExpired())
	})

	t.Run("设置 TTL", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		assert.Equal(t, time.Duration(-1), meta.TTL())

		meta.SetTTL(1 * time.Second)
		assert.True(t, meta.TTL() > 0 && meta.TTL() <= 1*time.Second)
	})

	t.Run("延长 TTL", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 1*time.Second)
		initialTTL := meta.TTL()

		meta.ExtendTTL(500 * time.Millisecond)
		newTTL := meta.TTL()

		assert.True(t, newTTL > initialTTL)
	})

	t.Run("对不过期项延长 TTL 应为无操作", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		meta.ExtendTTL(1 * time.Second)

		assert.Equal(t, time.Duration(-1), meta.TTL())
	})

	t.Run("ExpiresAt（过期时间）", func(t *testing.T) {
		ttl := 5 * time.Minute
		meta := v.NewMetadata(10, 100, ttl)

		expiresAt := meta.ExpiresAt()
		assert.True(t, expiresAt.After(time.Now()))
		assert.InDelta(t, time.Until(expiresAt).Seconds(), ttl.Seconds(), 1.0)
	})
}

func TestMetadata_Flags(t *testing.T) {
	t.Run("设置并检查标志", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)

		assert.False(t, meta.IsCompressed())
		meta.SetFlag(v.FlagCompressed)
		assert.True(t, meta.IsCompressed())

		assert.False(t, meta.IsEncrypted())
		meta.SetFlag(v.FlagEncrypted)
		assert.True(t, meta.IsEncrypted())
	})

	t.Run("清除标志", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)

		meta.SetFlag(v.FlagCompressed)
		assert.True(t, meta.IsCompressed())

		meta.ClearFlag(v.FlagCompressed)
		assert.False(t, meta.IsCompressed())
	})

	t.Run("多个标志", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)

		meta.SetFlag(v.FlagCompressed)
		meta.SetFlag(v.FlagHotKey)
		meta.SetFlag(v.FlagReadOnly)

		assert.True(t, meta.IsCompressed())
		assert.True(t, meta.IsHotKey())
		assert.True(t, meta.IsReadOnly())
		assert.False(t, meta.IsEncrypted())
	})

	t.Run("标志类型", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)

		tests := []struct {
			flag  uint32
			check func() bool
		}{
			{v.FlagCompressed, meta.IsCompressed},
			{v.FlagEncrypted, meta.IsEncrypted},
			{v.FlagReadOnly, meta.IsReadOnly},
			{v.FlagHotKey, meta.IsHotKey},
			{v.FlagNegative, meta.IsNegative},
			{v.FlagReplicating, meta.IsReplicating},
			{v.FlagDirty, meta.IsDirty},
			{v.FlagExpired, meta.IsLogicalExpired},
		}

		for _, tt := range tests {
			meta.SetFlag(tt.flag)
			assert.True(t, tt.check())
			meta.ClearFlag(tt.flag)
			assert.False(t, tt.check())
		}
	})
}

func TestMetadata_Statistics(t *testing.T) {
	t.Run("命中率计算", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)

		// 初始命中率
		assert.Equal(t, 0.0, meta.HitRate()) // 创建时不算一次访问和命中

		// 3次命中，1次访问（不命中）
		meta.OnHit()
		meta.OnHit()
		meta.OnHit()
		meta.OnAccess()

		// 3次命中 / 4次访问 = 0.75
		assert.InDelta(t, 0.75, meta.HitRate(), 0.01)
	})

	t.Run("年龄计算", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		time.Sleep(50 * time.Millisecond)

		age := meta.Age()
		assert.True(t, age >= 50*time.Millisecond)
	})

	t.Run("空闲时间计算", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		time.Sleep(50 * time.Millisecond)

		idle := meta.IdleTime()
		assert.True(t, idle >= 50*time.Millisecond)

		// 访问后空闲时间重置
		meta.OnAccess()
		idle = meta.IdleTime()
		assert.True(t, idle < 50*time.Millisecond)
	})
}

func q(t *testing.T) {
	t.Run("克隆产生独立副本", func(t *testing.T) {
		original := v.NewMetadata(10, 100, 1*time.Minute)
		original.SetFlag(v.FlagCompressed)
		original.OnHit()

		cloned := original.Clone()

		assert.Equal(t, original.KeySize(), cloned.KeySize())
		assert.Equal(t, original.ValueSize(), cloned.ValueSize())
		assert.Equal(t, original.AccessCount(), cloned.AccessCount())
		assert.True(t, cloned.IsCompressed())

		// 修改原始对象不应影响克隆
		original.OnHit()
		assert.NotEqual(t, original.HitCount(), cloned.HitCount())
	})
}

func TestMetadata_Reset(t *testing.T) {
	t.Run("重置清除所有数据", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 1*time.Minute)
		meta.SetFlag(v.FlagCompressed)
		meta.OnHit()
		meta.OnHit()

		meta.Reset()

		assert.Equal(t, int64(0), meta.KeySize())
		assert.Equal(t, int64(0), meta.ValueSize())
		assert.Equal(t, int64(0), meta.AccessCount())
		assert.Equal(t, int64(0), meta.HitCount())
		assert.False(t, meta.IsCompressed())
		assert.Equal(t, time.Duration(-1), meta.TTL())
	})
}

// ============================================================================
// Metadata 并发安全测试
// ============================================================================

func TestMetadata_Concurrent(t *testing.T) {
	t.Run("并发访问跟踪", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		var wg sync.WaitGroup
		iterations := 1000

		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				meta.OnAccess()
			}()
		}

		wg.Wait()
		assert.Equal(t, int64(iterations+1), meta.AccessCount()) // +1 from creation
	})

	t.Run("并发命中跟踪", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		var wg sync.WaitGroup
		iterations := 500

		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				meta.OnHit()
			}()
		}

		wg.Wait()
		assert.Equal(t, int64(iterations), meta.HitCount())
	})

	t.Run("并发标志操作", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		var wg sync.WaitGroup

		// 并发设置不同标志
		flags := []uint32{
			v.FlagCompressed,
			v.FlagEncrypted,
			v.FlagHotKey,
			v.FlagReadOnly,
		}

		for _, flag := range flags {
			wg.Add(2)
			go func(f uint32) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					meta.SetFlag(f)
				}
			}(flag)

			go func(f uint32) {
				defer wg.Done()
				for i := 0; i < 100; i++ {
					meta.HasFlag(f)
				}
			}(flag)
		}

		wg.Wait()

		// 验证所有标志都已设置
		for _, flag := range flags {
			assert.True(t, meta.HasFlag(flag))
		}
	})

	t.Run("并发更新和读取", func(t *testing.T) {
		meta := v.NewMetadata(10, 100, 0)
		var wg sync.WaitGroup

		// 并发更新
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				meta.OnUpdate(int64(100 + i))
				time.Sleep(time.Millisecond)
			}
		}()

		// 并发读取
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				_ = meta.ValueSize()
				_ = meta.UpdateCount()
				time.Sleep(time.Millisecond)
			}
		}()

		wg.Wait()
		assert.True(t, meta.UpdateCount() > 0)
	})
}

// ============================================================================
// Entry 测试
// ============================================================================

func TestEntry_Creation(t *testing.T) {
	t.Run("创建带TTL的条目", func(t *testing.T) {
		bv := v.NewByteView([]byte("test value"))
		entry := v.NewEntry("test_key", bv, 1*time.Minute)

		require.NotNil(t, entry)
		assert.Equal(t, "test value", entry.Value.String())
		require.NotNil(t, entry.Metadata)
		assert.True(t, entry.Metadata.TTL() > 0)
	})

	t.Run("创建不过期的条目", func(t *testing.T) {
		bv := v.NewByteView([]byte("test value"))
		entry := v.NewEntry("test_key", bv, 0)

		assert.False(t, entry.IsExpired())
	})
}

func TestEntry_Size(t *testing.T) {
	t.Run("计算条目大小", func(t *testing.T) {
		key := "test_key"
		data := []byte("test value")
		bv := v.NewByteView(data)
		entry := v.NewEntry(key, bv, 0)

		expectedSize := int64(len(key) + len(data))
		assert.Equal(t, expectedSize, entry.Size())
	})
}

func TestEntry_Expiration(t *testing.T) {
	t.Run("过期检测", func(t *testing.T) {
		bv := v.NewByteView([]byte("test"))
		entry := v.NewEntry("key", bv, 50*time.Millisecond)

		assert.False(t, entry.IsExpired())

		time.Sleep(100 * time.Millisecond)
		assert.True(t, entry.IsExpired())
	})

	t.Run("无元数据的条目不过期", func(t *testing.T) {
		entry := &v.Entry{
			Value:    v.NewByteView([]byte("test")),
			Metadata: nil,
		}

		assert.False(t, entry.IsExpired())
	})
}

func TestEntry_Clone(t *testing.T) {
	t.Run("克隆创建独立副本", func(t *testing.T) {
		bv := v.NewByteView([]byte("original"))
		original := v.NewEntry("key", bv, 1*time.Minute)
		original.Metadata.OnHit()

		cloned := original.Clone()

		assert.Equal(t, original.Value.String(), cloned.Value.String())
		assert.Equal(t, original.Metadata.HitCount(), cloned.Metadata.HitCount())

		// 修改原始对象不应影响克隆
		original.Metadata.OnHit()
		assert.NotEqual(t, original.Metadata.HitCount(), cloned.Metadata.HitCount())
	})
}

// ============================================================================
// EntryPool 测试
// ============================================================================

func TestEntryPool_Basic(t *testing.T) {
	t.Run("获取和放回", func(t *testing.T) {
		pool := v.NewEntryPool(10)

		// 放入一个条目
		bv := v.NewByteView([]byte("test"))
		entry := v.NewEntry("key", bv, 0)
		pool.Put(entry)

		// 取出应该是重置后的对象
		retrieved := pool.Get()
		assert.True(t, retrieved.Value.IsEmpty())
	})

	t.Run("从空池获取", func(t *testing.T) {
		pool := v.NewEntryPool(10)

		entry := pool.Get()
		require.NotNil(t, entry)
		assert.True(t, entry.Value.IsEmpty())
	})

	t.Run("放回nil条目", func(t *testing.T) {
		pool := v.NewEntryPool(10)

		// 不应该panic
		assert.NotPanics(t, func() {
			pool.Put(nil)
		})
	})
}

// ============================================================================
// 性能基准测试
// ============================================================================

func BenchmarkMetadata_Creation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = v.NewMetadata(100, 1024, 1*time.Minute)
	}
}

func BenchmarkMetadata_OnAccess(b *testing.B) {
	meta := v.NewMetadata(100, 1024, 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		meta.OnAccess()
	}
}

func BenchmarkMetadata_OnHit(b *testing.B) {
	meta := v.NewMetadata(100, 1024, 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		meta.OnHit()
	}
}

func BenchmarkMetadata_SetFlag(b *testing.B) {
	meta := v.NewMetadata(100, 1024, 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		meta.SetFlag(v.FlagCompressed)
	}
}

func BenchmarkMetadata_Clone(b *testing.B) {
	meta := v.NewMetadata(100, 1024, 1*time.Minute)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = meta.Clone()
	}
}

func BenchmarkEntry_Creation(b *testing.B) {
	bv := v.NewByteView([]byte("benchmark test value"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = v.NewEntry("test_key", bv, 1*time.Minute)
	}
}

func BenchmarkEntry_Clone(b *testing.B) {
	bv := v.NewByteView([]byte("benchmark test value"))
	entry := v.NewEntry("test_key", bv, 1*time.Minute)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = entry.Clone()
	}
}

func BenchmarkEntryPool(b *testing.B) {
	pool := v.NewEntryPool(100)
	bv := v.NewByteView([]byte("test"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		entry := pool.Get()
		entry.Value = bv
		pool.Put(entry)
	}
}
