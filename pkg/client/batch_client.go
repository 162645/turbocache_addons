package client

import (
	"context"
	"sync"
	"time"
)

// BatchClient 批量操作客户端
type BatchClient struct {
	client    *Client
	batchSize int
	flushTime time.Duration

	mu      sync.Mutex
	pending map[string]*batchItem
	timer   *time.Timer
	stopCh  chan struct{}
}

// batchItem 批量操作项
type batchItem struct {
	key   string
	value []byte
	ttl   time.Duration
	op    string // "set", "get", "delete"
	done  chan error
}

// BatchResult 批量操作结果
type BatchResult struct {
	Value []byte
	Found bool
	Error error
}

// NewBatchClient 创建批量客户端
func NewBatchClient(client *Client, batchSize int, flushTime time.Duration) *BatchClient {
	if batchSize <= 0 {
		batchSize = 100
	}
	if flushTime <= 0 {
		flushTime = 100 * time.Millisecond
	}

	bc := &BatchClient{
		client:    client,
		batchSize: batchSize,
		flushTime: flushTime,
		pending:   make(map[string]*batchItem),
		stopCh:    make(chan struct{}),
	}

	go bc.flushLoop()
	return bc
}

// Set 批量设置
func (bc *BatchClient) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	item := &batchItem{
		key:   key,
		value: value,
		ttl:   ttl,
		op:    "set",
		done:  make(chan error, 1),
	}

	bc.mu.Lock()
	bc.pending[key] = item
	needFlush := len(bc.pending) >= bc.batchSize
	bc.mu.Unlock()

	if needFlush {
		bc.flush()
	}

	select {
	case err := <-item.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Get 批量获取
func (bc *BatchClient) Get(ctx context.Context, key string) ([]byte, error) {
	item := &batchItem{
		key:  key,
		op:   "get",
		done: make(chan error, 1),
	}

	bc.mu.Lock()
	bc.pending[key] = item
	needFlush := len(bc.pending) >= bc.batchSize
	bc.mu.Unlock()

	if needFlush {
		bc.flush()
	}

	select {
	case err := <-item.done:
		return item.value, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Delete 批量删除
func (bc *BatchClient) Delete(ctx context.Context, key string) error {
	item := &batchItem{
		key:  key,
		op:   "delete",
		done: make(chan error, 1),
	}

	bc.mu.Lock()
	bc.pending[key] = item
	needFlush := len(bc.pending) >= bc.batchSize
	bc.mu.Unlock()

	if needFlush {
		bc.flush()
	}

	select {
	case err := <-item.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush 立即刷新
func (bc *BatchClient) Flush() {
	bc.flush()
}

// flush 执行批量操作
func (bc *BatchClient) flush() {
	bc.mu.Lock()
	if len(bc.pending) == 0 {
		bc.mu.Unlock()
		return
	}

	items := bc.pending
	bc.pending = make(map[string]*batchItem)
	bc.mu.Unlock()

	// 按操作类型分组
	setItems := make(map[string]*batchItem)
	getItems := make(map[string]*batchItem)
	delItems := make(map[string]*batchItem)

	for key, item := range items {
		switch item.op {
		case "set":
			setItems[key] = item
		case "get":
			getItems[key] = item
		case "delete":
			delItems[key] = item
		}
	}

	// 执行批量操作
	ctx := context.Background()

	// 批量Set
	if len(setItems) > 0 {
		bc.batchSet(ctx, setItems)
	}

	// 批量Get
	if len(getItems) > 0 {
		bc.batchGet(ctx, getItems)
	}

	// 批量Delete
	if len(delItems) > 0 {
		bc.batchDelete(ctx, delItems)
	}
}

// batchSet 执行批量设置
func (bc *BatchClient) batchSet(ctx context.Context, items map[string]*batchItem) {
	// 构建批量设置的 map
	setMap := make(map[string][]byte)
	var ttl time.Duration

	for key, item := range items {
		setMap[key] = item.value
		if ttl == 0 || item.ttl < ttl {
			ttl = item.ttl
		}
	}

	// 执行批量设置
	err := bc.client.BatchSet(ctx, setMap, ttl)

	// 通知所有项
	for _, item := range items {
		item.done <- err
	}
}

// batchGet 执行批量获取
func (bc *BatchClient) batchGet(ctx context.Context, items map[string]*batchItem) {
	// 构建键列表
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}

	// 执行批量获取
	results, err := bc.client.BatchGet(ctx, keys)

	// 通知所有项
	for key, item := range items {
		if err != nil {
			item.done <- err
			continue
		}

		if value, ok := results[key]; ok {
			item.value = value
			item.done <- nil
		} else {
			item.done <- ErrKeyNotFound
		}
	}
}

// batchDelete 执行批量删除
func (bc *BatchClient) batchDelete(ctx context.Context, items map[string]*batchItem) {
	// 构建键列表
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}

	// 执行批量删除
	err := bc.client.BatchDelete(ctx, keys)

	// 通知所有项
	for _, item := range items {
		item.done <- err
	}
}

// flushLoop 定时刷新循环
func (bc *BatchClient) flushLoop() {
	ticker := time.NewTicker(bc.flushTime)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bc.flush()
		case <-bc.stopCh:
			bc.flush() // 最后刷新一次
			return
		}
	}
}

// Close 关闭批量客户端
func (bc *BatchClient) Close() error {
	close(bc.stopCh)
	bc.flush()
	return nil
}
