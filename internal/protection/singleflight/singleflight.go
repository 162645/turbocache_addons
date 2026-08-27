package singleflight

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

// call 代表正在进行中或已完成的函数调用
type call struct {
	wg      sync.WaitGroup // 等待组，用于协调协程
	val     interface{}    // 函数返回值
	err     error          // 函数返回的错误
	dups    int            // 重复调用的次数（不包括 owner）
	chans   []chan Result  // 异步调用时的通道列表（owner 的 channel 设为第 0 个）
	start   time.Time      // 开始时间（用于统计）
	elapsed time.Duration  // 完成后记录的持续时间（如果已完成）
}

// Result 异步调用的结果
type Result struct {
	Val    interface{}
	Err    error
	Shared bool // 是否是共享的结果（非第一次调用）
}

// Group 管理对相同键的重复函数调用
// 确保对于同一个键，同时只有一个函数在执行
type Group struct {
	mu sync.Mutex       // protects m
	m  map[string]*call // lazily initialized
}

// Do 执行函数，如果同一个键已经有函数在执行，则等待结果
func (g *Group) Do(key string, fn func() (interface{}, error)) (v interface{}, err error, shared bool) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 如果已经有进行中的调用，则成为等待者
	if c, ok := g.m[key]; ok {
		c.dups++
		g.mu.Unlock()
		c.wg.Wait()

		// 如果被调用的函数 panic 了或调用了 runtime.Goexit，等待者也应有相同的行为
		if e, ok := c.err.(*panicError); ok {
			panic(e)
		} else if c.err == errGoexit {
			runtime.Goexit()
		}
		return c.val, c.err, true
	}

	// 当前 goroutine 成为 owner：创建并执行 call（同步执行）
	c := new(call)
	c.wg.Add(1)
	c.start = time.Now()
	// owner 的 channel list 为空 (非 DoChan 情况)
	g.m[key] = c
	g.mu.Unlock()

	// owner 执行实际调用（同步）
	g.doCall(c, key, fn)

	// owner 总是 shared == false
	if e, ok := c.err.(*panicError); ok {
		// owner 应当看到 panic
		panic(e)
	} else if c.err == errGoexit {
		runtime.Goexit()
	}
	return c.val, c.err, false
}

// DoChan 异步执行函数，返回一个通道来接收结果
func (g *Group) DoChan(key string, fn func() (interface{}, error)) <-chan Result {
	ch := make(chan Result, 1)
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	// 如果已有调用，注册为等待者（append channel）
	if c, ok := g.m[key]; ok {
		c.dups++
		c.chans = append(c.chans, ch)
		g.mu.Unlock()
		return ch
	}

	// owner：创建 call 并把 owner 的 channel 放在 chans[0]
	c := &call{chans: []chan Result{ch}}
	c.wg.Add(1)
	c.start = time.Now()
	g.m[key] = c
	g.mu.Unlock()

	// 为 DoChan 启动异步执行（owner 不阻塞在此）
	go g.doCall(c, key, fn)

	return ch
}

// doCall 执行实际的函数调用（处理 panic、Goexit、广播结果、清理 map 等）
func (g *Group) doCall(c *call, key string, fn func() (interface{}, error)) {
	// 使用 defer 做统一清理与广播
	defer func() {
		// 如果发生 panic，转换为 panicError 存入 c.err
		if r := recover(); r != nil {
			c.err = newPanicError(r)
		}

		// 记录 elapsed（若尚未记录）
		if !c.start.IsZero() && c.elapsed == 0 {
			c.elapsed = time.Since(c.start)
		}

		// 完成等待组
		c.wg.Done()

		// 从 map 中删除（无论正常或 panic，都移除，避免 stale entry）
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()

		// 广播结果给所有等待的 channel
		// owner channel 在 chans[0]（若存在），owner 的 Shared 应为 false，其他为 true
		for i, ch := range c.chans {
			shared := false
			if i != 0 && c.dups > 0 {
				shared = true
			}
			// 非阻塞发送（channel 有缓冲 1），匹配测试用例期望
			ch <- Result{Val: c.val, Err: c.err, Shared: shared}
		}

		// 如果 fn 在执行时触发 panicError，且没有 channel（即 Do 同步路径），在这里重新 panic，
		// 这样 owner（同步调用者）会看到 panic 并能被调用者的 recover 捕获。
		if e, ok := c.err.(*panicError); ok {
			if len(c.chans) == 0 {
				panic(e)
			}
		} else if c.err == errGoexit {
			if len(c.chans) == 0 {
				runtime.Goexit()
			}
		}
	}()

	// 实际调用（在外层 defer 中处理 panic）
	c.val, c.err = fn()
	// 如果没有发生 panic，上面 defer 会走到这里记录 elapsed
}

// Forget 让Group忘记给定键的值
// 这样可以让后续对Do的调用不会等待之前调用的完成
func (g *Group) Forget(key string) {
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
}

// ========== 统计接口 ==========

// Stats SingleFlight统计信息
type Stats struct {
	ActiveCalls int                   `json:"active_calls"` // 正在进行的调用数
	CallDetails map[string]CallDetail `json:"call_details"` // 每个键的调用详情
}

// CallDetail 调用详情
type CallDetail struct {
	Key        string        `json:"key"`
	Duplicates int           `json:"duplicates"` // 重复调用次数（不包括 owner）
	StartTime  time.Time     `json:"start_time"` // 开始时间
	Duration   time.Duration `json:"duration"`   // 如果正在进行中，返回 time.Since(start)，否则返回 elapsed
}

// GetStats 获取SingleFlight统计信息（报告当前 active calls）
func (g *Group) GetStats() Stats {
	g.mu.Lock()
	defer g.mu.Unlock()

	stats := Stats{
		ActiveCalls: len(g.m),
		CallDetails: make(map[string]CallDetail, len(g.m)),
	}

	now := time.Now()
	for key, c := range g.m {
		var duration time.Duration
		if !c.start.IsZero() {
			// 如果还没完成，返回 running duration
			if c.elapsed == 0 {
				duration = now.Sub(c.start)
			} else {
				duration = c.elapsed
			}
		}
		stats.CallDetails[key] = CallDetail{
			Key:        key,
			Duplicates: c.dups,
			StartTime:  c.start,
			Duration:   duration,
		}
	}

	return stats
}

// ========== panic / goexit support ==========

type panicError struct {
	value interface{}
	stack []byte
}

func (p *panicError) Error() string {
	return fmt.Sprintf("singleflight: panic recovered: %v\n%s", p.value, p.stack)
}

func newPanicError(v interface{}) error {
	stack := make([]byte, 64<<10) // 64KB
	stack = stack[:runtime.Stack(stack, false)]
	return &panicError{value: v, stack: stack}
}

// errGoexit indicates the runtime.Goexit was called
// in the user given function.
var errGoexit = errors.New("runtime.Goexit was called")

// ========== SingleFlightCache wrapper (keeps compatibility with tests) ==========

// SingleFlightCache 带有SingleFlight保护的缓存包装器
type SingleFlightCache struct {
	cache Cache
	group Group
}

// Cache 缓存接口
type Cache interface {
	Get(ctx context.Context, key string) (interface{}, error)
	Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error
	Delete(ctx context.Context, key string) error
}

// NewSingleFlightCache 创建带有SingleFlight保护的缓存
func NewSingleFlightCache(cache Cache) *SingleFlightCache {
	return &SingleFlightCache{
		cache: cache,
	}
}

// GetWithLoader 获取缓存值，如果不存在则使用loader加载
func (sf *SingleFlightCache) GetWithLoader(ctx context.Context, key string, loader func() (interface{}, error), ttl time.Duration) (interface{}, error) {
	// 首先尝试从缓存获取
	val, err := sf.cache.Get(ctx, key)
	if err == nil {
		return val, nil
	}

	// 使用SingleFlight确保相同key只有一个loader在执行
	result, loadErr, shared := sf.group.Do(key, func() (interface{}, error) {
		return loader()
	})

	if loadErr != nil {
		return nil, loadErr
	}

	// 将加载的数据设置到缓存中（仅第一次加载时）
	if !shared && result != nil {
		_ = sf.cache.Set(ctx, key, result, ttl)
	}

	return result, nil
}

// Set 设置缓存值
func (sf *SingleFlightCache) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	return sf.cache.Set(ctx, key, value, ttl)
}

// Delete 删除缓存值
func (sf *SingleFlightCache) Delete(ctx context.Context, key string) error {
	// 清除正在进行的SingleFlight调用
	sf.group.Forget(key)
	return sf.cache.Delete(ctx, key)
}

// AsyncGet 异步获取缓存值
func (sf *SingleFlightCache) AsyncGet(ctx context.Context, key string, loader func() (interface{}, error)) <-chan Result {
	resultCh := make(chan Result, 1)
	go func() {
		val, err := sf.cache.Get(ctx, key)
		if err == nil {
			resultCh <- Result{Val: val, Err: nil, Shared: false}
			return
		}
		ch := sf.group.DoChan(key, loader)
		res := <-ch
		resultCh <- res
	}()
	return resultCh
}

// ========== helper globals ==========

// FlightGroup 全局SingleFlight组
var DefaultGroup Group

// Do 使用默认组执行函数
func Do(key string, fn func() (interface{}, error)) (v interface{}, err error, shared bool) {
	return DefaultGroup.Do(key, fn)
}

// DoChan 使用默认组异步执行函数
func DoChan(key string, fn func() (interface{}, error)) <-chan Result {
	return DefaultGroup.DoChan(key, fn)
}

// Forget 让默认组忘记给定键
func Forget(key string) {
	DefaultGroup.Forget(key)
}
