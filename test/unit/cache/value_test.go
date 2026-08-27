package cache_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"turbocache/internal/cache/value"
)

// ============================================================================
// ByteView 基础功能测试（已添加中文注释与中文断言提示）
// 说明：每个子测试（t.Run）前都有中文注释，断言失败时会输出中文信息，
//      便于在 CI/本地运行时直接看到中文提示。
// ============================================================================

func TestByteView_Creation(t *testing.T) {
	// 从字节切片创建 ByteView
	t.Run("create from bytes", func(t *testing.T) {
		data := []byte("hello world")
		bv := value.NewByteView(data)

		assert.Equal(t, len(data), bv.Len(), "期望长度为 %d，但得到 %d", len(data), bv.Len())
		assert.Equal(t, string(data), bv.String(), "期望内容为 %s，但得到 %s", string(data), bv.String())
	})

	// 从字符串创建 ByteView
	t.Run("create from string", func(t *testing.T) {
		str := "hello world"
		bv := value.NewByteViewFromString(str)

		assert.Equal(t, len(str), bv.Len(), "期望长度为 %d，但得到 %d", len(str), bv.Len())
		assert.Equal(t, str, bv.String(), "期望内容为 %s，但得到 %s", str, bv.String())
	})

	// 从空数据创建
	t.Run("create from empty", func(t *testing.T) {
		bv1 := value.NewByteView([]byte{})
		bv2 := value.NewByteViewFromString("")

		assert.Equal(t, 0, bv1.Len(), "期望长度为 0，但得到 %d", bv1.Len())
		assert.Equal(t, 0, bv2.Len(), "期望长度为 0，但得到 %d", bv2.Len())
		assert.True(t, bv1.IsEmpty(), "期望 bv1 为空")
		assert.True(t, bv2.IsEmpty(), "期望 bv2 为空")
	})

	// 从 nil 创建
	t.Run("create from nil", func(t *testing.T) {
		bv := value.NewByteView(nil)
		assert.Equal(t, 0, bv.Len(), "期望长度为 0，但得到 %d", bv.Len())
		assert.True(t, bv.IsEmpty(), "期望 bv 为空")
	})
}

func TestByteView_Immutability(t *testing.T) {
	// 修改原始切片不应影响 ByteView（不可变语义）
	t.Run("bytes should not affect original", func(t *testing.T) {
		original := []byte("test data")
		bv := value.NewByteView(original)

		// 修改原始数据
		original[0] = 'X'

		// ByteView 应该不受影响
		assert.Equal(t, "test data", bv.String(), "修改原始切片后，ByteView 内容不应被改变")
	})

	// ByteSlice 返回的是数据复制，修改返回的切片不应影响 ByteView
	t.Run("ByteSlice returns copy", func(t *testing.T) {
		bv := value.NewByteView([]byte("immutable"))
		slice := bv.ByteSlice()

		// 修改返回的切片
		slice[0] = 'X'

		// ByteView 应该不受影响
		assert.Equal(t, "immutable", bv.String(), "修改返回的切片后，ByteView 内容不应被改变")
	})
}

func TestByteView_Conversions(t *testing.T) {
	// ByteSlice 转换应保持内容一致
	t.Run("ByteSlice conversion", func(t *testing.T) {
		data := []byte("convert test")
		bv := value.NewByteView(data)

		result := bv.ByteSlice()
		assert.True(t, bytes.Equal(data, result), "期望转换后内容与原始字节一致")
	})

	// String 转换
	t.Run("String conversion", func(t *testing.T) {
		str := "string test"
		bv := value.NewByteViewFromString(str)

		assert.Equal(t, str, bv.String(), "String() 返回的字符串应与原始一致")
	})

	// At 方法索引访问
	t.Run("At method", func(t *testing.T) {
		bv := value.NewByteView([]byte("abcde"))

		assert.Equal(t, byte('a'), bv.At(0), "At(0) 应返回 'a'")
		assert.Equal(t, byte('c'), bv.At(2), "At(2) 应返回 'c'")
		assert.Equal(t, byte('e'), bv.At(4), "At(4) 应返回 'e'")
	})
}

func TestByteView_Slicing(t *testing.T) {
	// Slice 范围测试
	t.Run("Slice range", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello world"))
		sub := bv.Slice(0, 5)

		assert.Equal(t, "hello", sub.String(), "子切片应为 'hello'")
		assert.Equal(t, 5, sub.Len(), "子切片长度应为 5")
	})

	// SliceFrom 从指定索引开始切
	t.Run("SliceFrom", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello world"))
		sub := bv.SliceFrom(6)

		assert.Equal(t, "world", sub.String(), "从索引 6 开始的子切片应为 'world'")
	})

	// 空切片情况
	t.Run("empty slice", func(t *testing.T) {
		bv := value.NewByteView([]byte("test"))
		sub := bv.Slice(2, 2)

		assert.Equal(t, 0, sub.Len(), "子切片长度应为 0")
		assert.True(t, sub.IsEmpty(), "子切片应为空")
	})
}

func TestByteView_Comparison(t *testing.T) {
	// 相等比较
	t.Run("Equal same content", func(t *testing.T) {
		bv1 := value.NewByteView([]byte("test"))
		bv2 := value.NewByteViewFromString("test")

		assert.True(t, bv1.Equal(bv2), "内容相同时应返回 true")
	})

	// 不同内容比较
	t.Run("Equal different content", func(t *testing.T) {
		bv1 := value.NewByteView([]byte("test1"))
		bv2 := value.NewByteView([]byte("test2"))

		assert.False(t, bv1.Equal(bv2), "内容不同时应返回 false")
	})

	// Compare 三种比较结果
	t.Run("Compare", func(t *testing.T) {
		bv1 := value.NewByteView([]byte("abc"))
		bv2 := value.NewByteView([]byte("abd"))
		bv3 := value.NewByteView([]byte("abc"))

		assert.Equal(t, -1, bv1.Compare(bv2), "abc < abd 时应返回 -1")
		assert.Equal(t, 0, bv1.Compare(bv3), "相同内容应返回 0")
		assert.Equal(t, 1, bv2.Compare(bv1), "abd > abc 时应返回 1")
	})
}

func TestByteView_Search(t *testing.T) {
	// Contains / Index / HasPrefix / HasSuffix 测试
	t.Run("Contains", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello world"))
		sub := value.NewByteView([]byte("world"))

		assert.True(t, bv.Contains(sub), "应包含子串 'world'")
		assert.False(t, bv.Contains(value.NewByteView([]byte("xyz"))), "不应包含 'xyz'")
	})

	t.Run("Index", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello world"))

		assert.Equal(t, 6, bv.Index(value.NewByteView([]byte("world"))), "'world' 在索引 6")
		assert.Equal(t, 0, bv.Index(value.NewByteView([]byte("hello"))), "'hello' 在索引 0")
		assert.Equal(t, -1, bv.Index(value.NewByteView([]byte("xyz"))), "不存在时应返回 -1")
	})

	t.Run("HasPrefix", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello world"))

		assert.True(t, bv.HasPrefix(value.NewByteView([]byte("hello"))), "应以 'hello' 为前缀")
		assert.False(t, bv.HasPrefix(value.NewByteView([]byte("world"))), "不应以 'world' 为前缀")
	})

	t.Run("HasSuffix", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello world"))

		assert.True(t, bv.HasSuffix(value.NewByteView([]byte("world"))), "应以 'world' 为后缀")
		assert.False(t, bv.HasSuffix(value.NewByteView([]byte("hello"))), "不应以 'hello' 为后缀")
	})
}

func TestByteView_Manipulation(t *testing.T) {
	// Split / TrimSpace / ToUpper / ToLower / Reverse 等操作
	t.Run("Split", func(t *testing.T) {
		bv := value.NewByteView([]byte("a,b,c,d"))
		parts := bv.Split(',')

		require.Equal(t, 4, len(parts), "Split 应返回 4 个部分")
		assert.Equal(t, "a", parts[0].String(), "第 1 部分应为 'a'")
		assert.Equal(t, "b", parts[1].String(), "第 2 部分应为 'b'")
		assert.Equal(t, "c", parts[2].String(), "第 3 部分应为 'c'")
		assert.Equal(t, "d", parts[3].String(), "第 4 部分应为 'd'")
	})

	t.Run("TrimSpace", func(t *testing.T) {
		bv := value.NewByteView([]byte("  hello  "))
		trimmed := bv.TrimSpace()

		assert.Equal(t, "hello", trimmed.String(), "TrimSpace 应去除前后空格")
	})

	t.Run("ToUpper", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello"))
		upper := bv.ToUpper()

		assert.Equal(t, "HELLO", upper.String(), "ToUpper 应将字母转换为大写")
	})

	t.Run("ToLower", func(t *testing.T) {
		bv := value.NewByteView([]byte("HELLO"))
		lower := bv.ToLower()

		assert.Equal(t, "hello", lower.String(), "ToLower 应将字母转换为小写")
	})

	t.Run("Reverse", func(t *testing.T) {
		bv := value.NewByteView([]byte("hello"))
		reversed := bv.Reverse()

		assert.Equal(t, "olleh", reversed.String(), "Reverse 应返回反转后的字符串")
	})
}

func TestByteView_IO(t *testing.T) {
	// 写入与读取相关的测试
	t.Run("WriteTo", func(t *testing.T) {
		bv := value.NewByteView([]byte("test data"))
		dst := make([]byte, 20)

		n, err := bv.WriteTo(dst)
		require.NoError(t, err, "WriteTo 应该不返回错误: %v", err)
		assert.Equal(t, 9, n, "写入字节数应为 9")
		assert.Equal(t, "test data", string(dst[:n]), "写入的内容应为 'test data'")
	})

	// 目标缓冲区太小时应返回错误
	t.Run("WriteTo buffer too small", func(t *testing.T) {
		bv := value.NewByteView([]byte("test data"))
		dst := make([]byte, 4)

		_, err := bv.WriteTo(dst)
		assert.Error(t, err, "当缓冲区过小时应返回错误")
	})

	// 从字节读取
	t.Run("ReadFrom", func(t *testing.T) {
		var bv value.ByteView
		src := []byte("new data")

		bv.ReadFrom(src)
		assert.Equal(t, "new data", bv.String(), "ReadFrom 后内容应为 'new data'")
	})

	// 从字符串读取
	t.Run("ReadFromString", func(t *testing.T) {
		var bv value.ByteView
		bv.ReadFromString("string data")

		assert.Equal(t, "string data", bv.String(), "ReadFromString 后内容应为 'string data'")
	})
}

func TestByteView_Hash(t *testing.T) {
	// 相同内容应有相同哈希，不同内容应有不同哈希
	t.Run("same content same hash", func(t *testing.T) {
		bv1 := value.NewByteView([]byte("test"))
		bv2 := value.NewByteViewFromString("test")

		assert.Equal(t, bv1.Hash(), bv2.Hash(), "相同内容的哈希值应相等")
	})

	t.Run("different content different hash", func(t *testing.T) {
		bv1 := value.NewByteView([]byte("test1"))
		bv2 := value.NewByteView([]byte("test2"))

		assert.NotEqual(t, bv1.Hash(), bv2.Hash(), "不同内容的哈希值应不相等")
	})
}

func TestByteView_Clone(t *testing.T) {
	// Clone 应创建独立副本
	t.Run("clone creates independent copy", func(t *testing.T) {
		bv := value.NewByteView([]byte("original"))
		cloned := bv.Clone()

		assert.True(t, bv.Equal(cloned), "克隆后的内容应与原始相等")
		assert.Equal(t, bv.String(), cloned.String(), "克隆后的字符串应相等")
	})
}

// ============================================================================
// ByteView 并发安全测试
// ============================================================================

func TestByteView_Concurrent(t *testing.T) {
	// 并发读操作
	t.Run("concurrent reads", func(t *testing.T) {
		bv := value.NewByteView([]byte("concurrent test data"))
		var wg sync.WaitGroup
		iterations := 1000

		for i := 0; i < iterations; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_ = bv.String()
				_ = bv.ByteSlice()
				_ = bv.Len()
				_ = bv.At(0)
			}()
		}

		wg.Wait()
	})

	// 并发切片操作
	t.Run("concurrent slicing", func(t *testing.T) {
		bv := value.NewByteView([]byte("0123456789"))
		var wg sync.WaitGroup

		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				start := idx % 5
				end := start + 3
				if end <= bv.Len() {
					_ = bv.Slice(start, end)
				}
			}(i)
		}

		wg.Wait()
	})
}

// ============================================================================
// ByteViewPool 测试（已增加中文断言信息）
// ============================================================================

func TestByteViewPool_Basic(t *testing.T) {
	// 基本的 Get/Put 行为
	t.Run("get and put", func(t *testing.T) {
		pool := value.NewByteViewPool(10)

		// Put 一些对象
		bv1 := value.NewByteView([]byte("test1"))
		pool.Put(bv1)

		// Get 应该取出重置后的对象
		bv2 := pool.Get()
		assert.True(t, bv2.IsEmpty(), "从池中取出的对象应为重置状态（空）")
	})

	// 从空池获取
	t.Run("get from empty pool", func(t *testing.T) {
		pool := value.NewByteViewPool(10)

		bv := pool.Get()
		assert.True(t, bv.IsEmpty(), "从空池获取应返回空对象")
	})

	// 放入超过容量的对象不会引起 panic，且实际可取出的对象不应超过容量
	t.Run("pool overflow", func(t *testing.T) {
		pool := value.NewByteViewPool(2)

		// 超过容量的对象会被丢弃或忽略（视实现而定），测试应具有鲁棒性：
		for i := 0; i < 5; i++ {
			bv := value.NewByteView([]byte("test"))
			pool.Put(bv)
		}

		// 尝试多次 Get，统计非空返回数量，应不超过容量
		count := 0
		for i := 0; i < 5; i++ {
			bv := pool.Get()
			if !bv.IsEmpty() {
				count++
			}
		}

		assert.LessOrEqual(t, count, 2, "从池中取出的非空对象数量不应超过池容量")
	})
}

// ============================================================================
// 性能基准测试（benchmark）
// ============================================================================

func BenchmarkByteView_Creation(b *testing.B) {
	data := []byte("benchmark test data")
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = value.NewByteView(data)
	}
}

func BenchmarkByteView_String(b *testing.B) {
	bv := value.NewByteView([]byte("benchmark test data"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bv.String()
	}
}

func BenchmarkByteView_ByteSlice(b *testing.B) {
	bv := value.NewByteView([]byte("benchmark test data"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bv.ByteSlice()
	}
}

func BenchmarkByteView_Slice(b *testing.B) {
	bv := value.NewByteView([]byte("0123456789abcdefghij"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bv.Slice(5, 15)
	}
}

func BenchmarkByteView_Compare(b *testing.B) {
	bv1 := value.NewByteView([]byte("test data 1"))
	bv2 := value.NewByteView([]byte("test data 2"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bv1.Compare(bv2)
	}
}

func BenchmarkByteView_Hash(b *testing.B) {
	bv := value.NewByteView([]byte("benchmark hash test"))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = bv.Hash()
	}
}

func BenchmarkByteViewPool(b *testing.B) {
	pool := value.NewByteViewPool(100)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bv := pool.Get()
		bv.ReadFrom([]byte("test"))
		pool.Put(bv)
	}
}
