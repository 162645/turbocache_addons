package pool

import (
	"sync"
	"time"
)

// SyncPool 基于 sync.Pool 的对象池实现
type SyncPool[T any] struct {
	pool    *sync.Pool
	newFunc func() T
	reset   func(T)
}

// NewSyncPool 创建同步对象池
func NewSyncPool[T any](newFunc func() T, resetFunc func(T)) *SyncPool[T] {
	sp := &SyncPool[T]{
		newFunc: newFunc,
		reset:   resetFunc,
	}

	sp.pool = &sync.Pool{
		New: func() interface{} {
			return sp.newFunc()
		},
	}

	return sp
}

// Get 从池中获取对象
func (sp *SyncPool[T]) Get() T {
	return sp.pool.Get().(T)
}

// Put 将对象放回池中
func (sp *SyncPool[T]) Put(obj T) {
	if sp.reset != nil {
		sp.reset(obj)
	}
	sp.pool.Put(obj)
}

// BufferPool 字节缓冲池
type BufferPool struct {
	small  *SyncPool[[]byte]
	medium *SyncPool[[]byte]
	large  *SyncPool[[]byte]
}

// NewBufferPool 创建字节缓冲池
func NewBufferPool() *BufferPool {
	return &BufferPool{
		small: NewSyncPool(
			func() []byte { return make([]byte, 0, 1024) }, // 1KB
			func(b []byte) { clear(b[:cap(b)]); b = b[:0] },
		),
		medium: NewSyncPool(
			func() []byte { return make([]byte, 0, 16*1024) }, // 16KB
			func(b []byte) { clear(b[:cap(b)]); b = b[:0] },
		),
		large: NewSyncPool(
			func() []byte { return make([]byte, 0, 64*1024) }, // 64KB
			func(b []byte) { clear(b[:cap(b)]); b = b[:0] },
		),
	}
}

// Get 根据大小获取合适的缓冲区
func (bp *BufferPool) Get(size int) []byte {
	switch {
	case size <= 1024:
		buf := bp.small.Get()
		if cap(buf) < size {
			return make([]byte, 0, size)
		}
		return buf[:0]
	case size <= 16*1024:
		buf := bp.medium.Get()
		if cap(buf) < size {
			return make([]byte, 0, size)
		}
		return buf[:0]
	case size <= 64*1024:
		buf := bp.large.Get()
		if cap(buf) < size {
			return make([]byte, 0, size)
		}
		return buf[:0]
	default:
		// 对于特别大的缓冲区，直接分配
		return make([]byte, 0, size)
	}
}

// Put 将缓冲区放回池中
func (bp *BufferPool) Put(buf []byte) {
	if buf == nil {
		return
	}

	capacity := cap(buf)
	switch {
	case capacity <= 1024:
		bp.small.Put(buf)
	case capacity <= 16*1024:
		bp.medium.Put(buf)
	case capacity <= 64*1024:
		bp.large.Put(buf)
	default:
		// 特别大的缓冲区不回收，让GC处理
	}
}

// WorkerPool 工作协程池
type WorkerPool struct {
	workers    chan chan func()
	workerQuit chan struct{}
	quit       chan struct{}
	wg         sync.WaitGroup
}

// NewWorkerPool 创建工作协程池
func NewWorkerPool(size int, bufferSize int) *WorkerPool {
	if size <= 0 {
		size = 10
	}
	if bufferSize <= 0 {
		bufferSize = 100
	}

	wp := &WorkerPool{
		workers:    make(chan chan func(), size),
		workerQuit: make(chan struct{}),
		quit:       make(chan struct{}),
	}

	// 启动工作协程
	for i := 0; i < size; i++ {
		wp.wg.Add(1)
		go wp.worker(bufferSize)
	}

	return wp
}

// worker 工作协程
func (wp *WorkerPool) worker(bufferSize int) {
	defer wp.wg.Done()

	jobQueue := make(chan func(), bufferSize)

	// 注册到工作协程池
	wp.workers <- jobQueue

	for {
		select {
		case job := <-jobQueue:
			if job != nil {
				job()
			}
			// 完成任务后重新注册
			wp.workers <- jobQueue
		case <-wp.workerQuit:
			return
		}
	}
}

// Submit 提交任务
func (wp *WorkerPool) Submit(job func()) bool {
	select {
	case jobQueue := <-wp.workers:
		select {
		case jobQueue <- job:
			return true
		default:
			// 任务队列满，放回工作协程并返回失败
			wp.workers <- jobQueue
			return false
		}
	case <-wp.quit:
		return false
	default:
		return false
	}
}

// SubmitWait 提交任务并等待执行
func (wp *WorkerPool) SubmitWait(job func()) bool {
	select {
	case jobQueue := <-wp.workers:
		jobQueue <- job
		return true
	case <-wp.quit:
		return false
	}
}

// Close 关闭工作池
func (wp *WorkerPool) Close() {
	close(wp.quit)
	close(wp.workerQuit)
	wp.wg.Wait()
}

// TimedPool 带超时的对象池
type TimedPool[T any] struct {
	pool      sync.Map
	newFunc   func() T
	resetFunc func(T)
	ttl       time.Duration
	cleaner   *time.Ticker
	quit      chan struct{}
}

// poolItem 池中的条目
type poolItem[T any] struct {
	value     T
	timestamp time.Time
}

// NewTimedPool 创建带超时的对象池
func NewTimedPool[T any](newFunc func() T, resetFunc func(T), ttl time.Duration, cleanInterval time.Duration) *TimedPool[T] {
	tp := &TimedPool[T]{
		newFunc:   newFunc,
		resetFunc: resetFunc,
		ttl:       ttl,
		cleaner:   time.NewTicker(cleanInterval),
		quit:      make(chan struct{}),
	}

	go tp.cleanup()
	return tp
}

// Get 获取对象
func (tp *TimedPool[T]) Get() T {
	var result T
	var found bool

	tp.pool.Range(func(key, value interface{}) bool {
		item := value.(*poolItem[T])
		if time.Since(item.timestamp) < tp.ttl {
			// 删除并取出
			tp.pool.Delete(key)
			result = item.value
			found = true
			return false // stop iteration
		}
		return true // continue
	})

	if !found && tp.newFunc != nil {
		result = tp.newFunc()
	}

	return result
}

// Put 放回对象
func (tp *TimedPool[T]) Put(obj T) {
	if tp.resetFunc != nil {
		tp.resetFunc(obj)
	}

	item := &poolItem[T]{
		value:     obj,
		timestamp: time.Now(),
	}

	// 使用 item 自身作为 key（每次都是唯一 pointer），value 存 item
	tp.pool.Store(item, item)
}

// cleanup 清理过期对象
func (tp *TimedPool[T]) cleanup() {
	for {
		select {
		case <-tp.cleaner.C:
			now := time.Now()
			tp.pool.Range(func(key, value interface{}) bool {
				item := value.(*poolItem[T])
				if now.Sub(item.timestamp) >= tp.ttl {
					tp.pool.Delete(key)
				}
				return true
			})
		case <-tp.quit:
			tp.cleaner.Stop()
			return
		}
	}
}

// Close 关闭定时池
func (tp *TimedPool[T]) Close() {
	close(tp.quit)
}

// Size 获取池中对象数量
func (tp *TimedPool[T]) Size() int {
	count := 0
	tp.pool.Range(func(key, value interface{}) bool {
		count++
		return true
	})
	return count
}

// 全局缓冲池实例
var (
	DefaultBufferPool = NewBufferPool()
	DefaultWorkerPool = NewWorkerPool(20, 100)
)

// 便捷函数
func GetBuffer(size int) []byte {
	return DefaultBufferPool.Get(size)
}

func PutBuffer(buf []byte) {
	DefaultBufferPool.Put(buf)
}

func SubmitJob(job func()) bool {
	return DefaultWorkerPool.Submit(job)
}

func SubmitJobWait(job func()) bool {
	return DefaultWorkerPool.SubmitWait(job)
}
