package storage

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"turbocache/internal/cache/value"
	"turbocache/internal/utils/logger"
)

// MemoryStorage 内存存储实现
type MemoryStorage struct {
	shards []*memoryShard // 把键空间切分成若干 shard，减小锁竞争（每个 shard 有自己的锁）。
	config *Config
	stats  *memoryStats
	logger logger.Logger
	closed int32

	// 清理相关
	cleanupTicker *time.Ticker
	cleanupDone   chan struct{}
	cleanupWg     sync.WaitGroup // 等待 cleanup goroutine 退出

	// 删除队列（统一处理过期删除，避免大量无控 goroutine）
	deleteCh chan string
	deleteWg sync.WaitGroup
}

// memoryShard 内存分片
type memoryShard struct {
	mu    sync.RWMutex
	data  map[string]*value.Entry
	size  int64 // 当前分片大小
	count int64 // 当前条目数
}

// memoryStats 内存存储统计
type memoryStats struct {
	totalKeys   int64
	totalSize   int64
	getCount    int64
	setCount    int64
	deleteCount int64
	hitCount    int64
	missCount   int64
	lastAccess  int64 // Unix纳秒时间戳
	lastUpdate  int64 // Unix纳秒时间戳
}

// NewMemoryStorage 创建内存存储
func NewMemoryStorage(config *Config) *MemoryStorage {
	if config == nil {
		config = &Config{
			MaxSize:       100 * 1024 * 1024, // 100MB
			MaxEntries:    100000,
			ShardCount:    64,
			CleanInterval: time.Minute,
		}
	}

	// 确保分片数是2的幂
	shardCount := config.ShardCount
	if shardCount <= 0 {
		shardCount = 64
	}
	if shardCount&(shardCount-1) != 0 {
		// 调整为最近的2的幂
		sc := 1
		for sc < config.ShardCount {
			sc <<= 1
		}
		shardCount = sc
	}

	storage := &MemoryStorage{
		shards:   make([]*memoryShard, shardCount),
		config:   config,
		stats:    &memoryStats{},
		logger:   logger.WithField("component", "memory_storage"),
		deleteCh: make(chan string, 1024), // 有界缓冲，避免瞬时洪水产生大量 goroutine
	}

	// 初始化分片
	for i := 0; i < shardCount; i++ {
		storage.shards[i] = &memoryShard{
			data: make(map[string]*value.Entry),
		}
	}

	// 启动删除 worker（统一处理过期删除）
	storage.startDeleteWorker()

	// 启动清理协程
	if config.CleanInterval > 0 {
		storage.startCleanup()
	}

	return storage
}

// getShard 获取键对应的分片（防御性：当 shards 长度异常时返回临时 shard）
func (ms *MemoryStorage) getShard(key string) *memoryShard {
	n := len(ms.shards)
	if n == 0 {
		// 防御性返回一个临时 shard（极端情况，通常不会触发）
		return &memoryShard{data: make(map[string]*value.Entry)}
	}
	hash := fnv1aHash(key)
	index := hash & uint64(n-1)
	return ms.shards[index]
}

// Get 获取值
func (ms *MemoryStorage) Get(ctx context.Context, key string) (*value.Entry, error) {
	if ms.isClosed() {
		return nil, ErrStorageClosed
	}

	atomic.AddInt64(&ms.stats.getCount, 1)
	atomic.StoreInt64(&ms.stats.lastAccess, time.Now().UnixNano())

	shard := ms.getShard(key)
	shard.mu.RLock()
	entry, exists := shard.data[key]
	shard.mu.RUnlock()

	if !exists {
		atomic.AddInt64(&ms.stats.missCount, 1)
		return nil, NewKeyNotFoundError(key)
	}

	// 检查是否过期
	if entry.IsExpired() {
		// 投递到删除队列，非阻塞；队列满时采用退避策略（spawn goroutine）避免阻塞热路径
		select {
		case ms.deleteCh <- key:
		default:
			// 回退为异步删除（短期策略），避免主路径阻塞
			go func(k string) { _ = ms.Delete(context.Background(), k) }(key)
		}
		atomic.AddInt64(&ms.stats.missCount, 1)
		return nil, NewKeyNotFoundError(key)
	}

	// 更新访问信息（确保 Metadata.OnHit 是非阻塞的）
	entry.Metadata.OnHit()
	atomic.AddInt64(&ms.stats.hitCount, 1)

	// 返回克隆，避免外部修改内部数据
	return entry.Clone(), nil
}

// Set 设置值
func (ms *MemoryStorage) Set(ctx context.Context, key string, entry *value.Entry) error {
	if ms.isClosed() {
		return ErrStorageClosed
	}

	if key == "" {
		return NewStorageError(CodeInvalidArgument, "key cannot be empty")
	}

	if entry == nil {
		return NewStorageError(CodeInvalidArgument, "entry cannot be nil")
	}

	// 检查大小限制（单条）
	keySize := int64(len(key))
	valueSize := int64(entry.Value.Len())
	totalSize := keySize + valueSize

	if ms.config.MaxSize > 0 && totalSize > ms.config.MaxSize {
		return NewValueTooLargeError(key, totalSize, ms.config.MaxSize)
	}

	atomic.AddInt64(&ms.stats.setCount, 1)
	atomic.StoreInt64(&ms.stats.lastUpdate, time.Now().UnixNano())

	shard := ms.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	// 检查是否已存在
	oldEntry, exists := shard.data[key]
	var oldSize int64
	if exists {
		oldSize = oldEntry.Size()
	}

	// 检查容量限制（基于全局统计）
	newTotalSize := atomic.LoadInt64(&ms.stats.totalSize) - oldSize + totalSize
	if ms.config.MaxSize > 0 && newTotalSize > ms.config.MaxSize {
		return NewStorageFullError(newTotalSize, ms.config.MaxSize)
	}

	newTotalCount := atomic.LoadInt64(&ms.stats.totalKeys)
	if !exists {
		newTotalCount++
	}
	if ms.config.MaxEntries > 0 && newTotalCount > ms.config.MaxEntries {
		return NewStorageFullError(newTotalCount, ms.config.MaxEntries)
	}

	// 设置条目（存入克隆）
	entryClone := entry.Clone()
	if entryClone.Metadata == nil {
		entryClone.Metadata = value.NewMetadata(keySize, valueSize, 0)
	}
	entryClone.Metadata.OnUpdate(valueSize)

	shard.data[key] = entryClone

	// 更新分片统计
	shard.size += totalSize - oldSize
	if !exists {
		shard.count++
		atomic.AddInt64(&ms.stats.totalKeys, 1)
	}

	// 更新全局统计
	atomic.AddInt64(&ms.stats.totalSize, totalSize-oldSize)

	return nil
}

// Delete 删除键
func (ms *MemoryStorage) Delete(ctx context.Context, key string) error {
	if ms.isClosed() {
		return ErrStorageClosed
	}

	atomic.AddInt64(&ms.stats.deleteCount, 1)

	shard := ms.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.data[key]
	if !exists {
		return NewKeyNotFoundError(key)
	}

	entrySize := entry.Size()
	delete(shard.data, key)

	// 更新分片统计
	shard.size -= entrySize
	shard.count--

	// 更新全局统计
	atomic.AddInt64(&ms.stats.totalKeys, -1)
	atomic.AddInt64(&ms.stats.totalSize, -entrySize)

	return nil
}

// Exists 检查键是否存在
func (ms *MemoryStorage) Exists(ctx context.Context, key string) (bool, error) {
	if ms.isClosed() {
		return false, ErrStorageClosed
	}

	shard := ms.getShard(key)
	shard.mu.RLock()
	entry, exists := shard.data[key]
	shard.mu.RUnlock()

	if !exists {
		return false, nil
	}

	// 检查是否过期
	if entry.IsExpired() {
		// 投递删除队列，避免直接在读路径删除
		select {
		case ms.deleteCh <- key:
		default:
			go func(k string) { _ = ms.Delete(context.Background(), k) }(key)
		}
		return false, nil
	}

	return true, nil
}

// BatchGet 批量获取
func (ms *MemoryStorage) BatchGet(ctx context.Context, keys []string) (map[string]*value.Entry, error) {
	if ms.isClosed() {
		return nil, ErrStorageClosed
	}

	if len(keys) == 0 {
		return make(map[string]*value.Entry), nil
	}

	result := make(map[string]*value.Entry, len(keys))
	for _, key := range keys {
		if entry, err := ms.Get(ctx, key); err == nil {
			result[key] = entry
		}
	}

	return result, nil
}

// BatchSet 批量设置
func (ms *MemoryStorage) BatchSet(ctx context.Context, entries map[string]*value.Entry) error {
	if ms.isClosed() {
		return ErrStorageClosed
	}

	if len(entries) == 0 {
		return nil
	}

	// 简单实现：逐个设置
	// 更优化的实现可以按分片分组并行处理
	for key, entry := range entries {
		if err := ms.Set(ctx, key, entry); err != nil {
			return err
		}
	}

	return nil
}

// BatchDelete 批量删除
func (ms *MemoryStorage) BatchDelete(ctx context.Context, keys []string) error {
	if ms.isClosed() {
		return ErrStorageClosed
	}

	if len(keys) == 0 {
		return nil
	}

	for _, key := range keys {
		// 忽略键不存在的错误
		_ = ms.Delete(ctx, key)
	}

	return nil
}

// Iterator 创建迭代器
func (ms *MemoryStorage) Iterator(ctx context.Context) (Iterator, error) {
	if ms.isClosed() {
		return nil, ErrStorageClosed
	}

	return ms.newIterator(""), nil
}

// IteratorWithPrefix 创建前缀迭代器
func (ms *MemoryStorage) IteratorWithPrefix(ctx context.Context, prefix string) (Iterator, error) {
	if ms.isClosed() {
		return nil, ErrStorageClosed
	}

	return ms.newIterator(prefix), nil
}

// Stats 获取统计信息
func (ms *MemoryStorage) Stats() (*Stats, error) {
	if ms.isClosed() {
		return nil, ErrStorageClosed
	}

	totalKeys := atomic.LoadInt64(&ms.stats.totalKeys)
	totalSize := atomic.LoadInt64(&ms.stats.totalSize)
	getCount := atomic.LoadInt64(&ms.stats.getCount)
	setCount := atomic.LoadInt64(&ms.stats.setCount)
	deleteCount := atomic.LoadInt64(&ms.stats.deleteCount)
	hitCount := atomic.LoadInt64(&ms.stats.hitCount)
	missCount := atomic.LoadInt64(&ms.stats.missCount)
	lastAccess := atomic.LoadInt64(&ms.stats.lastAccess)
	lastUpdate := atomic.LoadInt64(&ms.stats.lastUpdate)

	var hitRate float64
	if getCount > 0 {
		hitRate = float64(hitCount) / float64(getCount)
	}

	var avgKeySize, avgValueSize float64
	if totalKeys > 0 {
		// 这里简化处理，实际实现需要维护键和值的总大小
		avgValueSize = float64(totalSize) / float64(totalKeys)
	}

	return &Stats{
		TotalKeys:    totalKeys,
		TotalSize:    totalSize,
		UsedMemory:   totalSize,
		MaxMemory:    ms.config.MaxSize,
		GetCount:     getCount,
		SetCount:     setCount,
		DeleteCount:  deleteCount,
		HitCount:     hitCount,
		MissCount:    missCount,
		HitRate:      hitRate,
		AvgKeySize:   avgKeySize,
		AvgValueSize: avgValueSize,
		LastAccess:   time.Unix(0, lastAccess),
		LastUpdate:   time.Unix(0, lastUpdate),
	}, nil
}

// Size 获取存储总大小
func (ms *MemoryStorage) Size() (int64, error) {
	if ms.isClosed() {
		return 0, ErrStorageClosed
	}

	return atomic.LoadInt64(&ms.stats.totalSize), nil
}

// Count 获取条目总数
func (ms *MemoryStorage) Count() (int64, error) {
	if ms.isClosed() {
		return 0, ErrStorageClosed
	}

	return atomic.LoadInt64(&ms.stats.totalKeys), nil
}

// Clear 清空存储
func (ms *MemoryStorage) Clear(ctx context.Context) error {
	if ms.isClosed() {
		return ErrStorageClosed
	}

	for _, shard := range ms.shards {
		shard.mu.Lock()
		shard.data = make(map[string]*value.Entry)
		shard.size = 0
		shard.count = 0
		shard.mu.Unlock()
	}

	atomic.StoreInt64(&ms.stats.totalKeys, 0)
	atomic.StoreInt64(&ms.stats.totalSize, 0)

	return nil
}

// Expire 设置键的过期时间
func (ms *MemoryStorage) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if ms.isClosed() {
		return ErrStorageClosed
	}

	shard := ms.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	entry, exists := shard.data[key]
	if !exists {
		return NewKeyNotFoundError(key)
	}

	if entry.Metadata != nil {
		entry.Metadata.SetTTL(ttl)
	}

	return nil
}

// ExpireAt 设置键的绝对过期时间
func (ms *MemoryStorage) ExpireAt(ctx context.Context, key string, expireTime time.Time) error {
	ttl := time.Until(expireTime)
	if ttl <= 0 {
		return ms.Delete(ctx, key)
	}
	return ms.Expire(ctx, key, ttl)
}

// TTL 获取键的剩余生存时间
func (ms *MemoryStorage) TTL(ctx context.Context, key string) (time.Duration, error) {
	if ms.isClosed() {
		return 0, ErrStorageClosed
	}

	shard := ms.getShard(key)
	shard.mu.RLock()
	entry, exists := shard.data[key]
	shard.mu.RUnlock()

	if !exists {
		return 0, NewKeyNotFoundError(key)
	}

	if entry.Metadata == nil {
		return -1, nil // 永不过期
	}

	return entry.Metadata.TTL(), nil
}

// Close 关闭存储
func (ms *MemoryStorage) Close() error {
	if !atomic.CompareAndSwapInt32(&ms.closed, 0, 1) {
		return nil // 已经关闭
	}

	// 停止清理协程
	if ms.cleanupTicker != nil {
		ms.cleanupTicker.Stop()
	}
	if ms.cleanupDone != nil {
		// 通知 cleanup goroutine 退出
		close(ms.cleanupDone)
	}
	// 等待 cleanup goroutine 真正退出
	ms.cleanupWg.Wait()

	// 关闭删除队列并等待 worker 退出
	if ms.deleteCh != nil {
		close(ms.deleteCh)
	}
	ms.deleteWg.Wait()

	// 清理数据（在等待后台 worker 退出后安全进行）
	for _, shard := range ms.shards {
		shard.mu.Lock()
		shard.data = nil
		shard.mu.Unlock()
	}

	return nil
}

// isClosed 检查存储是否已关闭
func (ms *MemoryStorage) isClosed() bool {
	return atomic.LoadInt32(&ms.closed) != 0
}

// startCleanup 启动清理协程
func (ms *MemoryStorage) startCleanup() {
	ms.cleanupTicker = time.NewTicker(ms.config.CleanInterval)
	ms.cleanupDone = make(chan struct{})
	ms.cleanupWg.Add(1)

	go func() {
		defer ms.cleanupWg.Done()
		defer ms.cleanupTicker.Stop()

		for {
			select {
			case <-ms.cleanupTicker.C:
				ms.cleanup()
			case <-ms.cleanupDone:
				return
			}
		}
	}()
}

// cleanup 清理过期条目
func (ms *MemoryStorage) cleanup() {
	cleanedCount := 0
	cleanedSize := int64(0)

	for _, shard := range ms.shards {
		shard.mu.Lock()

		for key, entry := range shard.data {
			if entry.IsExpired() {
				entrySize := entry.Size()
				delete(shard.data, key)
				shard.size -= entrySize
				shard.count--
				cleanedCount++
				cleanedSize += entrySize
			}
		}

		shard.mu.Unlock()
	}

	// 更新全局统计
	if cleanedCount > 0 {
		atomic.AddInt64(&ms.stats.totalKeys, -int64(cleanedCount))
		atomic.AddInt64(&ms.stats.totalSize, -cleanedSize)

		ms.logger.Debugf("Cleaned up %d expired entries, freed %d bytes", cleanedCount, cleanedSize)
	}
}

// startDeleteWorker 启动处理删除队列的 worker
func (ms *MemoryStorage) startDeleteWorker() {
	ms.deleteWg.Add(1)
	go func() {
		defer ms.deleteWg.Done()
		for key := range ms.deleteCh {
			// 如果已经关闭，直接跳过
			if ms.isClosed() {
				continue
			}
			// 忽略删除错误（例如 key 已不存在）
			_ = ms.Delete(context.Background(), key)
		}
	}()
}

// memoryIterator 内存存储迭代器
type memoryIterator struct {
	storage *MemoryStorage
	prefix  string
	keys    []string
	current int
	err     error
}

// newIterator 创建迭代器
func (ms *MemoryStorage) newIterator(prefix string) *memoryIterator {
	iter := &memoryIterator{
		storage: ms,
		prefix:  prefix,
		current: -1,
	}

	// 收集所有匹配的键
	iter.collectKeys()

	return iter
}

// collectKeys 收集匹配的键
func (iter *memoryIterator) collectKeys() {
	var keys []string

	for _, shard := range iter.storage.shards {
		shard.mu.RLock()
		for key := range shard.data {
			if iter.prefix == "" || hasPrefix(key, iter.prefix) {
				keys = append(keys, key)
			}
		}
		shard.mu.RUnlock()
	}

	// 排序键
	sort.Strings(keys)
	iter.keys = keys
}

// Next 移动到下一个条目
func (iter *memoryIterator) Next() bool {
	if iter.err != nil {
		return false
	}

	iter.current++
	return iter.current < len(iter.keys)
}

// Key 获取当前键
func (iter *memoryIterator) Key() string {
	if iter.current < 0 || iter.current >= len(iter.keys) {
		return ""
	}
	return iter.keys[iter.current]
}

// Value 获取当前值条目
func (iter *memoryIterator) Value() (*value.Entry, error) {
	if iter.current < 0 || iter.current >= len(iter.keys) {
		return nil, fmt.Errorf("iterator out of bounds")
	}

	key := iter.keys[iter.current]
	entry, err := iter.storage.Get(context.Background(), key)
	if err != nil {
		// 键可能在迭代过程中被删除
		return nil, err
	}

	return entry, nil
}

// Error 获取迭代器错误
func (iter *memoryIterator) Error() error {
	return iter.err
}

// Close 关闭迭代器
func (iter *memoryIterator) Close() error {
	iter.keys = nil
	iter.current = -1
	iter.err = nil
	return nil
}

// Reset 重置迭代器
func (iter *memoryIterator) Reset() error {
	iter.current = -1
	iter.err = nil
	iter.collectKeys()
	return nil
}

// Seek 定位到指定键
func (iter *memoryIterator) Seek(key string) bool {
	if iter.err != nil {
		return false
	}

	// 二分查找
	index := sort.SearchStrings(iter.keys, key)
	if index < len(iter.keys) {
		iter.current = index
		return true
	}

	iter.current = len(iter.keys) // 设置为超出范围
	return false
}

// MemoryStorageFactory 内存存储工厂
type MemoryStorageFactory struct{}

// CreateStorage 创建存储实例
func (f *MemoryStorageFactory) CreateStorage(config *Config) (Storage, error) {
	return NewMemoryStorage(config), nil
}

// SupportedTypes 获取支持的存储类型
func (f *MemoryStorageFactory) SupportedTypes() []string {
	return []string{"memory"}
}

// ValidateConfig 验证配置
func (f *MemoryStorageFactory) ValidateConfig(config *Config) error {
	if config == nil {
		return ErrConfigMissing
	}

	if config.MaxSize < 0 {
		return fmt.Errorf("max_size cannot be negative")
	}

	if config.MaxEntries < 0 {
		return fmt.Errorf("max_entries cannot be negative")
	}

	if config.ShardCount <= 0 {
		return fmt.Errorf("shard_count must be positive")
	}

	if config.CleanInterval < 0 {
		return fmt.Errorf("clean_interval cannot be negative")
	}

	return nil
}

// 辅助函数

// fnv1aHash FNV-1a哈希函数
func fnv1aHash(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for i := 0; i < len(s); i++ {
		hash ^= uint64(s[i])
		hash *= prime64
	}
	return hash
}

// hasPrefix 检查字符串前缀
func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

// init 注册内存存储工厂
func init() {
	Register("memory", &MemoryStorageFactory{})
}
