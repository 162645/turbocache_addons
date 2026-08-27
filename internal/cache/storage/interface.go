package storage

import (
	"context"
	"time"
	"turbocache/internal/cache/value"
)

// Storage 存储接口定义
type Storage interface {
	// 基本操作
	Get(ctx context.Context, key string) (*value.Entry, error)
	Set(ctx context.Context, key string, entry *value.Entry) error
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)

	// 批量操作
	BatchGet(ctx context.Context, keys []string) (map[string]*value.Entry, error)
	BatchSet(ctx context.Context, entries map[string]*value.Entry) error
	BatchDelete(ctx context.Context, keys []string) error

	// 迭代器
	Iterator(ctx context.Context) (Iterator, error)
	IteratorWithPrefix(ctx context.Context, prefix string) (Iterator, error)

	// 统计信息
	Stats() (*Stats, error)
	Size() (int64, error)
	Count() (int64, error)

	// 清理操作
	Clear(ctx context.Context) error
	Expire(ctx context.Context, key string, ttl time.Duration) error
	ExpireAt(ctx context.Context, key string, expireTime time.Time) error
	TTL(ctx context.Context, key string) (time.Duration, error)

	// 生命周期
	Close() error
}

// Iterator 迭代器接口
type Iterator interface {
	// 移动到下一个条目
	Next() bool

	// 获取当前键
	Key() string

	// 获取当前值条目
	Value() (*value.Entry, error)

	// 获取错误信息
	Error() error

	// 关闭迭代器
	Close() error

	// 重置到开始位置
	Reset() error

	// 定位到指定键
	Seek(key string) bool
}

// Stats 存储统计信息
type Stats struct {
	// 基本统计
	TotalKeys  int64 `json:"total_keys"`
	TotalSize  int64 `json:"total_size"`
	UsedMemory int64 `json:"used_memory"`
	MaxMemory  int64 `json:"max_memory"`

	// 操作统计
	GetCount    int64 `json:"get_count"`
	SetCount    int64 `json:"set_count"`
	DeleteCount int64 `json:"delete_count"`
	HitCount    int64 `json:"hit_count"`
	MissCount   int64 `json:"miss_count"`

	// 性能统计
	HitRate      float64 `json:"hit_rate"`
	AvgKeySize   float64 `json:"avg_key_size"`
	AvgValueSize float64 `json:"avg_value_size"`

	// 时间统计
	LastAccess time.Time `json:"last_access"`
	LastUpdate time.Time `json:"last_update"`

	// 扩展统计
	Extra map[string]interface{} `json:"extra,omitempty"`
}

// Config 存储配置
type Config struct {
	// 容量配置
	MaxSize    int64 `yaml:"max_size" json:"max_size"`       // 最大字节数
	MaxEntries int64 `yaml:"max_entries" json:"max_entries"` // 最大条目数

	// 性能配置
	ShardCount int `yaml:"shard_count" json:"shard_count"` // 分片数
	BatchSize  int `yaml:"batch_size" json:"batch_size"`   // 批处理大小

	// 清理配置
	CleanInterval time.Duration `yaml:"clean_interval" json:"clean_interval"` // 清理间隔
	MaxIdleTime   time.Duration `yaml:"max_idle_time" json:"max_idle_time"`   // 最大空闲时间

	// 持久化配置
	Persistent   bool          `yaml:"persistent" json:"persistent"`       // 是否持久化
	SyncInterval time.Duration `yaml:"sync_interval" json:"sync_interval"` // 同步间隔
	BackupPath   string        `yaml:"backup_path" json:"backup_path"`     // 备份路径

	// 压缩配置
	Compression       bool  `yaml:"compression" json:"compression"`               // 是否启用压缩
	CompressThreshold int64 `yaml:"compress_threshold" json:"compress_threshold"` // 压缩阈值

	// 加密配置
	Encryption    bool   `yaml:"encryption" json:"encryption"`         // 是否启用加密
	EncryptionKey string `yaml:"encryption_key" json:"encryption_key"` // 加密密钥
}

// OperationType 操作类型
type OperationType int

const (
	OpGet OperationType = iota
	OpSet
	OpDelete
	OpExists
	OpBatchGet
	OpBatchSet
	OpBatchDelete
	OpClear
	OpExpire
	OpTTL
)

// Operation 存储操作
type Operation struct {
	Type      OperationType           `json:"type"`
	Key       string                  `json:"key"`
	Keys      []string                `json:"keys,omitempty"`
	Entry     *value.Entry            `json:"entry,omitempty"`
	Entries   map[string]*value.Entry `json:"entries,omitempty"`
	TTL       time.Duration           `json:"ttl,omitempty"`
	Timestamp time.Time               `json:"timestamp"`
	Context   map[string]string       `json:"context,omitempty"`
}

// Result 操作结果
type Result struct {
	Success   bool                    `json:"success"`
	Error     error                   `json:"error,omitempty"`
	Entry     *value.Entry            `json:"entry,omitempty"`
	Entries   map[string]*value.Entry `json:"entries,omitempty"`
	Found     bool                    `json:"found"`
	Count     int64                   `json:"count"`
	Size      int64                   `json:"size"`
	TTL       time.Duration           `json:"ttl,omitempty"`
	Timestamp time.Time               `json:"timestamp"`
}

// Batch 批量操作接口
type Batch interface {
	// 添加操作
	Get(key string) *Batch
	Set(key string, entry *value.Entry) *Batch
	Delete(key string) *Batch

	// 执行批量操作
	Execute(ctx context.Context) ([]*Result, error)

	// 清空批量操作
	Reset()

	// 获取操作数量
	Size() int
}

// TransactionalStorage 事务性存储接口
type TransactionalStorage interface {
	Storage

	// 开始事务
	BeginTx(ctx context.Context) (Transaction, error)
}

// Transaction 事务接口
type Transaction interface {
	// 基本操作（继承自Storage的操作语义）
	Get(ctx context.Context, key string) (*value.Entry, error)
	Set(ctx context.Context, key string, entry *value.Entry) error
	Delete(ctx context.Context, key string) error

	// 事务控制
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error

	// 事务状态
	IsActive() bool
}

// BackupStorage 备份存储接口
type BackupStorage interface {
	// 创建备份
	Backup(ctx context.Context, path string) error

	// 从备份恢复
	Restore(ctx context.Context, path string) error

	// 列出备份
	ListBackups() ([]BackupInfo, error)
}

// BackupInfo 备份信息
type BackupInfo struct {
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	Count     int64     `json:"count"`
	CreatedAt time.Time `json:"created_at"`
	Checksum  string    `json:"checksum"`
}

// StorageFactory 存储工厂接口
type StorageFactory interface {
	// 创建存储实例
	CreateStorage(config *Config) (Storage, error)

	// 获取支持的存储类型
	SupportedTypes() []string

	// 验证配置
	ValidateConfig(config *Config) error
}

// Registry 存储注册表
type Registry struct {
	factories map[string]StorageFactory
}

// NewRegistry 创建新的注册表
func NewRegistry() *Registry {
	return &Registry{
		factories: make(map[string]StorageFactory),
	}
}

// Register 注册存储工厂
func (r *Registry) Register(name string, factory StorageFactory) {
	r.factories[name] = factory
}

// Create 创建存储实例
func (r *Registry) Create(storageType string, config *Config) (Storage, error) {
	factory, exists := r.factories[storageType]
	if !exists {
		return nil, ErrUnsupportedStorageType
	}

	if err := factory.ValidateConfig(config); err != nil {
		return nil, err
	}

	return factory.CreateStorage(config)
}

// List 列出所有注册的存储类型
func (r *Registry) List() []string {
	var types []string
	for name := range r.factories {
		types = append(types, name)
	}
	return types
}

// 全局注册表实例
var DefaultRegistry = NewRegistry()

// 便捷函数
func Register(name string, factory StorageFactory) {
	DefaultRegistry.Register(name, factory)
}

func Create(storageType string, config *Config) (Storage, error) {
	return DefaultRegistry.Create(storageType, config)
}

func ListStorageTypes() []string {
	return DefaultRegistry.List()
}
