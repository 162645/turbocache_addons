package storage

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"turbocache/internal/cache/value"
	"turbocache/internal/utils/logger"
)

// PersistentStorage 持久化存储实现
type PersistentStorage struct {
	mu       sync.RWMutex
	data     map[string]*value.Entry
	filePath string
	config   *Config
	logger   logger.Logger

	// 定期同步
	syncTicker *time.Ticker
	syncDone   chan struct{}

	// WAL (Write-Ahead Log)
	walFile *os.File
	walPath string

	// 统计信息
	stats *persistentStats

	// 生命周期
	closed    bool
	closeOnce sync.Once
}

// persistentStats 持久化存储统计
type persistentStats struct {
	totalKeys   int64
	totalSize   int64
	getCount    int64
	setCount    int64
	deleteCount int64
	syncCount   int64
	lastSync    time.Time
	loadTime    time.Duration
}

// WALEntry WAL日志条目
type WALEntry struct {
	Type      string       `gob:"type"` // SET, DELETE, CLEAR
	Key       string       `gob:"key"`
	Entry     *value.Entry `gob:"entry"`
	Timestamp time.Time    `gob:"timestamp"`
}

// NewPersistentStorage 创建持久化存储
func NewPersistentStorage(config *Config) (*PersistentStorage, error) {
	if config == nil {
		config = &Config{
			Persistent:   true,
			SyncInterval: 30 * time.Second,
			BackupPath:   "/tmp/turbocache",
		}
	}

	// 确保备份目录存在
	if err := os.MkdirAll(config.BackupPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create backup directory: %v", err)
	}

	filePath := filepath.Join(config.BackupPath, "cache.gob")
	walPath := filepath.Join(config.BackupPath, "cache.wal")

	ps := &PersistentStorage{
		data:     make(map[string]*value.Entry),
		filePath: filePath,
		walPath:  walPath,
		config:   config,
		logger:   logger.WithField("component", "persistent_storage"),
		syncDone: make(chan struct{}),
		stats:    &persistentStats{},
	}

	// 加载已有数据
	if err := ps.load(); err != nil {
		return nil, fmt.Errorf("failed to load persistent data: %v", err)
	}

	// 初始化WAL
	if err := ps.initWAL(); err != nil {
		return nil, fmt.Errorf("failed to initialize WAL: %v", err)
	}

	// 启动定期同步
	if config.SyncInterval > 0 {
		ps.startSync()
	}

	return ps, nil
}

// Get 获取值
func (ps *PersistentStorage) Get(ctx context.Context, key string) (*value.Entry, error) {
	if ps.closed {
		return nil, ErrStorageClosed
	}

	ps.mu.RLock()
	entry, exists := ps.data[key]
	ps.mu.RUnlock()

	ps.stats.getCount++

	if !exists {
		return nil, NewKeyNotFoundError(key)
	}

	// 检查是否过期
	if entry.IsExpired() {
		// 异步删除过期条目
		go ps.Delete(context.Background(), key)
		return nil, NewKeyNotFoundError(key)
	}

	return entry.Clone(), nil
}

// Set 设置值
func (ps *PersistentStorage) Set(ctx context.Context, key string, entry *value.Entry) error {
	if ps.closed {
		return ErrStorageClosed
	}

	if key == "" {
		return ErrInvalidKey
	}

	if entry == nil {
		return ErrInvalidValue
	}

	ps.mu.Lock()

	// 检查大小限制
	if ps.config.MaxSize > 0 {
		currentSize := ps.calculateSize()
		entrySize := entry.Size()

		if currentSize+entrySize > ps.config.MaxSize {
			ps.mu.Unlock()
			return NewStorageFullError(currentSize+entrySize, ps.config.MaxSize)
		}
	}

	// 检查条目数限制
	if ps.config.MaxEntries > 0 {
		if _, exists := ps.data[key]; !exists && int64(len(ps.data)) >= ps.config.MaxEntries {
			ps.mu.Unlock()
			return NewStorageFullError(int64(len(ps.data)+1), ps.config.MaxEntries)
		}
	}

	ps.data[key] = entry.Clone()
	ps.mu.Unlock()

	ps.stats.setCount++

	// 写入WAL
	if err := ps.writeWAL(&WALEntry{
		Type:      "SET",
		Key:       key,
		Entry:     entry,
		Timestamp: time.Now(),
	}); err != nil {
		ps.logger.Warnf("Failed to write WAL entry: %v", err)
	}

	return nil
}

// Delete 删除值
func (ps *PersistentStorage) Delete(ctx context.Context, key string) error {
	if ps.closed {
		return ErrStorageClosed
	}

	ps.mu.Lock()
	_, exists := ps.data[key]
	if exists {
		delete(ps.data, key)
	}
	ps.mu.Unlock()

	ps.stats.deleteCount++

	if !exists {
		return NewKeyNotFoundError(key)
	}

	// 写入WAL
	if err := ps.writeWAL(&WALEntry{
		Type:      "DELETE",
		Key:       key,
		Timestamp: time.Now(),
	}); err != nil {
		ps.logger.Warnf("Failed to write WAL entry: %v", err)
	}

	return nil
}

// Exists 检查键是否存在
func (ps *PersistentStorage) Exists(ctx context.Context, key string) (bool, error) {
	if ps.closed {
		return false, ErrStorageClosed
	}

	ps.mu.RLock()
	entry, exists := ps.data[key]
	ps.mu.RUnlock()

	if !exists {
		return false, nil
	}

	// 检查是否过期
	if entry.IsExpired() {
		go ps.Delete(context.Background(), key)
		return false, nil
	}

	return true, nil
}

// BatchGet 批量获取
func (ps *PersistentStorage) BatchGet(ctx context.Context, keys []string) (map[string]*value.Entry, error) {
	if ps.closed {
		return nil, ErrStorageClosed
	}

	result := make(map[string]*value.Entry)

	for _, key := range keys {
		if entry, err := ps.Get(ctx, key); err == nil {
			result[key] = entry
		}
	}

	return result, nil
}

// BatchSet 批量设置
func (ps *PersistentStorage) BatchSet(ctx context.Context, entries map[string]*value.Entry) error {
	if ps.closed {
		return ErrStorageClosed
	}

	for key, entry := range entries {
		if err := ps.Set(ctx, key, entry); err != nil {
			return err
		}
	}

	return nil
}

// BatchDelete 批量删除
func (ps *PersistentStorage) BatchDelete(ctx context.Context, keys []string) error {
	if ps.closed {
		return ErrStorageClosed
	}

	for _, key := range keys {
		ps.Delete(ctx, key) // 忽略不存在的键
	}

	return nil
}

// Iterator 创建迭代器
func (ps *PersistentStorage) Iterator(ctx context.Context) (Iterator, error) {
	if ps.closed {
		return nil, ErrStorageClosed
	}

	return ps.newIterator(""), nil
}

// IteratorWithPrefix 创建前缀迭代器
func (ps *PersistentStorage) IteratorWithPrefix(ctx context.Context, prefix string) (Iterator, error) {
	if ps.closed {
		return nil, ErrStorageClosed
	}

	return ps.newIterator(prefix), nil
}

// Stats 获取统计信息
func (ps *PersistentStorage) Stats() (*Stats, error) {
	if ps.closed {
		return nil, ErrStorageClosed
	}

	ps.mu.RLock()
	totalKeys := int64(len(ps.data))
	totalSize := ps.calculateSize()
	ps.mu.RUnlock()

	return &Stats{
		TotalKeys:   totalKeys,
		TotalSize:   totalSize,
		UsedMemory:  totalSize,
		MaxMemory:   ps.config.MaxSize,
		GetCount:    ps.stats.getCount,
		SetCount:    ps.stats.setCount,
		DeleteCount: ps.stats.deleteCount,
		HitCount:    ps.stats.getCount, // 简化处理
		MissCount:   0,
		HitRate:     1.0, // 持久化存储假设总是命中
		LastAccess:  time.Now(),
		LastUpdate:  time.Now(),
		Extra: map[string]interface{}{
			"sync_count": ps.stats.syncCount,
			"last_sync":  ps.stats.lastSync,
			"load_time":  ps.stats.loadTime,
			"file_path":  ps.filePath,
			"wal_path":   ps.walPath,
		},
	}, nil
}

// Size 获取存储总大小
func (ps *PersistentStorage) Size() (int64, error) {
	if ps.closed {
		return 0, ErrStorageClosed
	}

	ps.mu.RLock()
	size := ps.calculateSize()
	ps.mu.RUnlock()

	return size, nil
}

// Count 获取条目总数
func (ps *PersistentStorage) Count() (int64, error) {
	if ps.closed {
		return 0, ErrStorageClosed
	}

	ps.mu.RLock()
	count := int64(len(ps.data))
	ps.mu.RUnlock()

	return count, nil
}

// Clear 清空存储
func (ps *PersistentStorage) Clear(ctx context.Context) error {
	if ps.closed {
		return ErrStorageClosed
	}

	ps.mu.Lock()
	ps.data = make(map[string]*value.Entry)
	ps.mu.Unlock()

	// 写入WAL
	if err := ps.writeWAL(&WALEntry{
		Type:      "CLEAR",
		Timestamp: time.Now(),
	}); err != nil {
		ps.logger.Warnf("Failed to write WAL entry: %v", err)
	}

	return nil
}

// Expire 设置过期时间
func (ps *PersistentStorage) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if ps.closed {
		return ErrStorageClosed
	}

	ps.mu.Lock()
	entry, exists := ps.data[key]
	if exists && entry.Metadata != nil {
		entry.Metadata.SetTTL(ttl)
	}
	ps.mu.Unlock()

	if !exists {
		return NewKeyNotFoundError(key)
	}

	return nil
}

// ExpireAt 设置绝对过期时间
func (ps *PersistentStorage) ExpireAt(ctx context.Context, key string, expireTime time.Time) error {
	ttl := time.Until(expireTime)
	if ttl <= 0 {
		return ps.Delete(ctx, key)
	}
	return ps.Expire(ctx, key, ttl)
}

// TTL 获取剩余生存时间
func (ps *PersistentStorage) TTL(ctx context.Context, key string) (time.Duration, error) {
	if ps.closed {
		return 0, ErrStorageClosed
	}

	ps.mu.RLock()
	entry, exists := ps.data[key]
	ps.mu.RUnlock()

	if !exists {
		return 0, NewKeyNotFoundError(key)
	}

	if entry.Metadata == nil {
		return -1, nil // 永不过期
	}

	return entry.Metadata.TTL(), nil
}

// Close 关闭存储
func (ps *PersistentStorage) Close() error {
	var err error
	ps.closeOnce.Do(func() {
		ps.closed = true

		// 停止同步
		if ps.syncTicker != nil {
			ps.syncTicker.Stop()
		}
		close(ps.syncDone)

		// 最终同步
		if syncErr := ps.sync(); syncErr != nil {
			err = fmt.Errorf("final sync failed: %v", syncErr)
		}

		// 关闭WAL文件
		if ps.walFile != nil {
			if closeErr := ps.walFile.Close(); closeErr != nil && err == nil {
				err = fmt.Errorf("failed to close WAL file: %v", closeErr)
			}
		}

		ps.logger.Info("Persistent storage closed")
	})

	return err
}

// 私有方法

// calculateSize 计算当前存储大小
func (ps *PersistentStorage) calculateSize() int64 {
	var totalSize int64
	for _, entry := range ps.data {
		totalSize += entry.Size()
	}
	return totalSize
}

// load 加载持久化数据
func (ps *PersistentStorage) load() error {
	start := time.Now()

	// 首先尝试从主文件加载
	if err := ps.loadFromFile(ps.filePath); err != nil {
		if !os.IsNotExist(err) {
			ps.logger.Warnf("Failed to load from main file: %v", err)
		}
	}

	// 然后应用WAL日志
	if err := ps.replayWAL(); err != nil {
		ps.logger.Warnf("Failed to replay WAL: %v", err)
	}

	ps.stats.loadTime = time.Since(start)
	ps.logger.Infof("Loaded %d entries in %v", len(ps.data), ps.stats.loadTime)

	return nil
}

// loadFromFile 从文件加载数据
func (ps *PersistentStorage) loadFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	var data map[string]*value.Entry
	if err := decoder.Decode(&data); err != nil {
		return fmt.Errorf("failed to decode data: %v", err)
	}

	// 过滤过期数据
	validData := make(map[string]*value.Entry)
	for key, entry := range data {
		if !entry.IsExpired() {
			validData[key] = entry
		}
	}

	ps.data = validData
	return nil
}

// sync 同步数据到磁盘
func (ps *PersistentStorage) sync() error {
	ps.mu.RLock()
	data := make(map[string]*value.Entry)
	for key, entry := range ps.data {
		data[key] = entry.Clone()
	}
	ps.mu.RUnlock()

	// 写入临时文件
	tmpFile := ps.filePath + ".tmp"
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp file: %v", err)
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		return fmt.Errorf("failed to encode data: %v", err)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %v", err)
	}

	file.Close()

	// 原子性地替换主文件
	if err := os.Rename(tmpFile, ps.filePath); err != nil {
		return fmt.Errorf("failed to rename temp file: %v", err)
	}

	// 清理WAL文件
	if err := ps.truncateWAL(); err != nil {
		ps.logger.Warnf("Failed to truncate WAL: %v", err)
	}

	ps.stats.syncCount++
	ps.stats.lastSync = time.Now()

	ps.logger.Debugf("Synced %d entries to disk", len(data))
	return nil
}

// startSync 启动定期同步
func (ps *PersistentStorage) startSync() {
	ps.syncTicker = time.NewTicker(ps.config.SyncInterval)

	go func() {
		defer ps.syncTicker.Stop()

		for {
			select {
			case <-ps.syncTicker.C:
				if err := ps.sync(); err != nil {
					ps.logger.Errorf("Sync failed: %v", err)
				}
			case <-ps.syncDone:
				return
			}
		}
	}()
}

// initWAL 初始化WAL
func (ps *PersistentStorage) initWAL() error {
	file, err := os.OpenFile(ps.walPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open WAL file: %v", err)
	}

	ps.walFile = file
	return nil
}

// writeWAL 写入WAL
func (ps *PersistentStorage) writeWAL(entry *WALEntry) error {
	if ps.walFile == nil {
		return nil
	}

	encoder := gob.NewEncoder(ps.walFile)
	if err := encoder.Encode(entry); err != nil {
		return err
	}

	return ps.walFile.Sync()
}

// replayWAL 重放WAL日志
func (ps *PersistentStorage) replayWAL() error {
	file, err := os.Open(ps.walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // WAL文件不存在，正常情况
		}
		return err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	replayCount := 0

	for {
		var entry WALEntry
		err := decoder.Decode(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			ps.logger.Warnf("Failed to decode WAL entry: %v", err)
			continue
		}

		// 应用WAL条目
		switch entry.Type {
		case "SET":
			if entry.Entry != nil {
				ps.data[entry.Key] = entry.Entry.Clone()
				replayCount++
			}
		case "DELETE":
			delete(ps.data, entry.Key)
			replayCount++
		case "CLEAR":
			ps.data = make(map[string]*value.Entry)
			replayCount++
		}
	}

	if replayCount > 0 {
		ps.logger.Infof("Replayed %d WAL entries", replayCount)
	}

	return nil
}

// truncateWAL 截断WAL文件
func (ps *PersistentStorage) truncateWAL() error {
	if ps.walFile == nil {
		return nil
	}

	ps.walFile.Close()

	// 重新创建空的WAL文件
	file, err := os.OpenFile(ps.walPath, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}

	ps.walFile = file
	return nil
}

// newIterator 创建迭代器
func (ps *PersistentStorage) newIterator(prefix string) *persistentIterator {
	ps.mu.RLock()

	var keys []string
	for key := range ps.data {
		if prefix == "" || hasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	ps.mu.RUnlock()

	return &persistentIterator{
		storage: ps,
		keys:    keys,
		current: -1,
	}
}

// persistentIterator 持久化存储迭代器
type persistentIterator struct {
	storage *PersistentStorage
	keys    []string
	current int
	err     error
}

// Next 移动到下一个条目
func (it *persistentIterator) Next() bool {
	if it.err != nil {
		return false
	}

	it.current++
	return it.current < len(it.keys)
}

// Key 获取当前键
func (it *persistentIterator) Key() string {
	if it.current < 0 || it.current >= len(it.keys) {
		return ""
	}
	return it.keys[it.current]
}

// Value 获取当前值条目
func (it *persistentIterator) Value() (*value.Entry, error) {
	if it.current < 0 || it.current >= len(it.keys) {
		return nil, fmt.Errorf("iterator out of bounds")
	}

	key := it.keys[it.current]
	return it.storage.Get(context.Background(), key)
}

// Error 获取迭代器错误
func (it *persistentIterator) Error() error {
	return it.err
}

// Close 关闭迭代器
func (it *persistentIterator) Close() error {
	it.keys = nil
	it.current = -1
	it.err = nil
	return nil
}

// Reset 重置迭代器
func (it *persistentIterator) Reset() error {
	it.current = -1
	it.err = nil
	return nil
}

// Seek 定位到指定键
func (it *persistentIterator) Seek(key string) bool {
	if it.err != nil {
		return false
	}

	for i, k := range it.keys {
		if k >= key {
			it.current = i
			return true
		}
	}

	it.current = len(it.keys)
	return false
}

// PersistentStorageFactory 持久化存储工厂
type PersistentStorageFactory struct{}

// CreateStorage 创建存储实例
func (f *PersistentStorageFactory) CreateStorage(config *Config) (Storage, error) {
	return NewPersistentStorage(config)
}

// SupportedTypes 获取支持的存储类型
func (f *PersistentStorageFactory) SupportedTypes() []string {
	return []string{"persistent"}
}

// ValidateConfig 验证配置
func (f *PersistentStorageFactory) ValidateConfig(config *Config) error {
	if config == nil {
		return ErrConfigMissing
	}

	if !config.Persistent {
		return fmt.Errorf("persistent flag must be true")
	}

	if config.BackupPath == "" {
		return fmt.Errorf("backup_path is required")
	}

	if config.SyncInterval < 0 {
		return fmt.Errorf("sync_interval cannot be negative")
	}

	return nil
}

// 注册持久化存储工厂
func init() {
	Register("persistent", &PersistentStorageFactory{})
}
