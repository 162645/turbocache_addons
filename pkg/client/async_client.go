package client

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// AsyncClient 异步客户端
type AsyncClient struct {
	*Client

	// 异步操作配置
	maxConcurrency int
	workerPool     chan struct{}

	// 统计信息
	pendingOps int64
	totalOps   int64
}

// AsyncResult 异步操作结果
type AsyncResult struct {
	Value []byte
	Error error
	Done  chan struct{}
}

// AsyncBatchResult 异步批量操作结果
type AsyncBatchResult struct {
	Values map[string][]byte
	Error  error
	Done   chan struct{}
}

// NewAsyncClient 创建异步客户端
func NewAsyncClient(servers []string, config *Config, maxConcurrency int) (*AsyncClient, error) {
	if maxConcurrency <= 0 {
		maxConcurrency = 100
	}

	client, err := NewClient(servers, config)
	if err != nil {
		return nil, err
	}

	asyncClient := &AsyncClient{
		Client:         client,
		maxConcurrency: maxConcurrency,
		workerPool:     make(chan struct{}, maxConcurrency),
	}

	// 填充工作池
	for i := 0; i < maxConcurrency; i++ {
		asyncClient.workerPool <- struct{}{}
	}

	return asyncClient, nil
}

// GetAsync 异步获取缓存值
func (ac *AsyncClient) GetAsync(ctx context.Context, key string) *AsyncResult {
	result := &AsyncResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)

		// 获取工作许可
		select {
		case <-ac.workerPool:
		case <-ctx.Done():
			result.Error = ctx.Err()
			return
		}

		// 归还工作许可
		defer func() {
			ac.workerPool <- struct{}{}
		}()

		// 执行操作
		result.Value, result.Error = ac.Client.Get(ctx, key)
	}()

	return result
}

// SetAsync 异步设置缓存值
func (ac *AsyncClient) SetAsync(ctx context.Context, key string, value []byte, ttl time.Duration) *AsyncResult {
	result := &AsyncResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)

		// 获取工作许可
		select {
		case <-ac.workerPool:
		case <-ctx.Done():
			result.Error = ctx.Err()
			return
		}

		// 归还工作许可
		defer func() {
			ac.workerPool <- struct{}{}
		}()

		// 执行操作
		result.Error = ac.Client.Set(ctx, key, value, ttl)
	}()

	return result
}

// DeleteAsync 异步删除缓存值
func (ac *AsyncClient) DeleteAsync(ctx context.Context, key string) *AsyncResult {
	result := &AsyncResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)

		// 获取工作许可
		select {
		case <-ac.workerPool:
		case <-ctx.Done():
			result.Error = ctx.Err()
			return
		}

		// 归还工作许可
		defer func() {
			ac.workerPool <- struct{}{}
		}()

		// 执行操作
		result.Error = ac.Client.Delete(ctx, key)
	}()

	return result
}

// BatchGetAsync 异步批量获取
func (ac *AsyncClient) BatchGetAsync(ctx context.Context, keys []string) *AsyncBatchResult {
	result := &AsyncBatchResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)

		// 获取工作许可
		select {
		case <-ac.workerPool:
		case <-ctx.Done():
			result.Error = ctx.Err()
			return
		}

		// 归还工作许可
		defer func() {
			ac.workerPool <- struct{}{}
		}()

		// 执行操作
		result.Values, result.Error = ac.Client.BatchGet(ctx, keys)
	}()

	return result
}

// BatchSetAsync 异步批量设置
func (ac *AsyncClient) BatchSetAsync(ctx context.Context, items map[string][]byte, ttl time.Duration) *AsyncResult {
	result := &AsyncResult{
		Done: make(chan struct{}),
	}

	go func() {
		defer close(result.Done)

		// 获取工作许可
		select {
		case <-ac.workerPool:
		case <-ctx.Done():
			result.Error = ctx.Err()
			return
		}

		// 归还工作许可
		defer func() {
			ac.workerPool <- struct{}{}
		}()

		// 执行操作
		result.Error = ac.Client.BatchSet(ctx, items, ttl)
	}()

	return result
}

// GetMultipleAsync 并发获取多个键
func (ac *AsyncClient) GetMultipleAsync(ctx context.Context, keys []string) map[string]*AsyncResult {
	results := make(map[string]*AsyncResult)

	for _, key := range keys {
		results[key] = ac.GetAsync(ctx, key)
	}

	return results
}

// SetMultipleAsync 并发设置多个键
func (ac *AsyncClient) SetMultipleAsync(ctx context.Context, items map[string][]byte, ttl time.Duration) map[string]*AsyncResult {
	results := make(map[string]*AsyncResult)

	for key, value := range items {
		results[key] = ac.SetAsync(ctx, key, value, ttl)
	}

	return results
}

// WaitAll 等待所有异步操作完成
func (ac *AsyncClient) WaitAll(ctx context.Context, results map[string]*AsyncResult) error {
	for key, result := range results {
		select {
		case <-result.Done:
			if result.Error != nil {
				return fmt.Errorf("operation failed for key %s: %v", key, result.Error)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// WaitAny 等待任意一个异步操作完成
func (ac *AsyncClient) WaitAny(ctx context.Context, results map[string]*AsyncResult) (string, *AsyncResult, error) {
	// 创建一个合并的通道来监听所有结果
	done := make(chan string, len(results))

	// 为每个结果启动一个goroutine
	for key, result := range results {
		go func(k string, r *AsyncResult) {
			select {
			case <-r.Done:
				done <- k
			case <-ctx.Done():
				return
			}
		}(key, result)
	}

	// 等待第一个完成的操作
	select {
	case key := <-done:
		return key, results[key], nil
	case <-ctx.Done():
		return "", nil, ctx.Err()
	}
}

// Pipeline 管道操作
type Pipeline struct {
	client     *AsyncClient
	operations []func() *AsyncResult
	mu         sync.Mutex
}

// NewPipeline 创建管道
func (ac *AsyncClient) NewPipeline() *Pipeline {
	return &Pipeline{
		client: ac,
	}
}

// Get 添加获取操作到管道
func (p *Pipeline) Get(ctx context.Context, key string) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.operations = append(p.operations, func() *AsyncResult {
		return p.client.GetAsync(ctx, key)
	})

	return p
}

// Set 添加设置操作到管道
func (p *Pipeline) Set(ctx context.Context, key string, value []byte, ttl time.Duration) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.operations = append(p.operations, func() *AsyncResult {
		return p.client.SetAsync(ctx, key, value, ttl)
	})

	return p
}

// Delete 添加删除操作到管道
func (p *Pipeline) Delete(ctx context.Context, key string) *Pipeline {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.operations = append(p.operations, func() *AsyncResult {
		return p.client.DeleteAsync(ctx, key)
	})

	return p
}

// Execute 执行管道中的所有操作
func (p *Pipeline) Execute(ctx context.Context) ([]*AsyncResult, error) {
	p.mu.Lock()
	operations := make([]func() *AsyncResult, len(p.operations))
	copy(operations, p.operations)
	p.mu.Unlock()

	if len(operations) == 0 {
		return nil, nil
	}

	results := make([]*AsyncResult, len(operations))

	// 并发执行所有操作
	var wg sync.WaitGroup
	for i, op := range operations {
		wg.Add(1)
		go func(index int, operation func() *AsyncResult) {
			defer wg.Done()
			results[index] = operation()
		}(i, op)
	}

	// 等待所有操作完成
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return results, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Future 表示一个未来的值
type Future struct {
	result *AsyncResult
	key    string
}

// NewFuture 创建Future
func (ac *AsyncClient) NewFuture(ctx context.Context, key string) *Future {
	return &Future{
		result: ac.GetAsync(ctx, key),
		key:    key,
	}
}

// Get 获取Future的值
func (f *Future) Get(ctx context.Context) ([]byte, error) {
	select {
	case <-f.result.Done:
		return f.result.Value, f.result.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// GetWithTimeout 带超时获取Future的值
func (f *Future) GetWithTimeout(timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return f.Get(ctx)
}

// IsDone 检查Future是否完成
func (f *Future) IsDone() bool {
	select {
	case <-f.result.Done:
		return true
	default:
		return false
	}
}

// 回调函数类型
type CallbackFunc func(key string, value []byte, err error)

// GetWithCallback 异步获取并执行回调
func (ac *AsyncClient) GetWithCallback(ctx context.Context, key string, callback CallbackFunc) {
	go func() {
		result := ac.GetAsync(ctx, key)
		<-result.Done
		callback(key, result.Value, result.Error)
	}()
}

// SetWithCallback 异步设置并执行回调
func (ac *AsyncClient) SetWithCallback(ctx context.Context, key string, value []byte, ttl time.Duration, callback CallbackFunc) {
	go func() {
		result := ac.SetAsync(ctx, key, value, ttl)
		<-result.Done
		callback(key, value, result.Error)
	}()
}

// GetStats 获取异步客户端统计信息
func (ac *AsyncClient) GetStats() *AsyncStats {
	return &AsyncStats{
		MaxConcurrency: ac.maxConcurrency,
		ActiveWorkers:  ac.maxConcurrency - len(ac.workerPool),
		PendingOps:     ac.pendingOps,
		TotalOps:       ac.totalOps,
	}
}

// AsyncStats 异步客户端统计信息
type AsyncStats struct {
	MaxConcurrency int   `json:"max_concurrency"`
	ActiveWorkers  int   `json:"active_workers"`
	PendingOps     int64 `json:"pending_ops"`
	TotalOps       int64 `json:"total_ops"`
}
