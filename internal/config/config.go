package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"os"
	"time"
)

// ---------- 主配置结构（包含所有子配置） ----------
type Config struct {
	Server    ServerConfig
	Cache     CacheConfig
	Cluster   ClusterConfig
	Transport TransportConfig
	Metrics   MetricsConfig
	Logger    LoggerConfig
}

// ---------- Server ----------
type ServerConfig struct {
	Host           string
	Port           int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration
	MaxHeaderBytes int
}

// ---------- Cache ----------
type CacheConfig struct {
	L1               L1CacheConfig
	L2               L2CacheConfig
	DefaultTTL       time.Duration
	MaxKeySize       int64
	MaxValueSize     int64
	EvictionPolicy   string
	EvictionInterval time.Duration
	Protection       ProtectionConfig
}

type L1CacheConfig struct {
	Enabled     bool
	MaxSize     int64
	MaxEntries  int64
	TTL         time.Duration
	ShardCount  int
	StatsWindow time.Duration
	// EvictionPolicy selects the runtime replacement algorithm: lru, lfu or arc.
	EvictionPolicy string
}

type L2CacheConfig struct {
	Enabled           bool
	ReplicationFactor int
	ConsistencyLevel  string
	SyncTimeout       time.Duration
	BackupInterval    time.Duration
}

// 保护相关
type ProtectionConfig struct {
	BloomFilter     BloomFilterConfig
	NegativeCache   NegativeCacheConfig
	SingleFlight    SingleFlightConfig
	HotKeyDetection HotKeyConfig
	CircuitBreaker  CircuitBreakerConfig
}

type BloomFilterConfig struct {
	Enabled           bool
	ExpectedItems     int64
	FalsePositiveRate float64
	RefreshInterval   time.Duration
	BatchSize         int
	FlushInterval     time.Duration
	QueueSize         int
	BootstrapDelay    time.Duration
}

type NegativeCacheConfig struct {
	Enabled bool
	TTL     time.Duration
	MaxSize int64
}

type SingleFlightConfig struct {
	Enabled bool
}

type HotKeyConfig struct {
	Enabled       bool
	Threshold     int64
	Window        time.Duration
	PreloadMargin time.Duration
}

type CircuitBreakerConfig struct {
	Enabled      bool
	Threshold    int64
	Timeout      time.Duration
	MaxRequests  int
	FailureRatio float64
}

// ---------- Cluster ----------
type ClusterConfig struct {
	NodeID      string
	Region      string
	Zone        string
	Weight      int
	Metadata    map[string]string
	Discovery   DiscoveryConfig
	Hash        HashConfig
	Replication ReplicationConfig
}

type DiscoveryConfig struct {
	Type              string
	Endpoints         []string
	RegisterPath      string
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	WatchTimeout      time.Duration
	RetryInterval     time.Duration
	MaxRetries        int
	Username          string
	Password          string
}

type HashConfig struct {
	Algorithm          string
	VirtualNodes       int
	HashFunction       string
	RebalanceThreshold float64
}

type ReplicationConfig struct {
	Factor        int
	SyncMode      string
	SyncTimeout   time.Duration
	RetryInterval time.Duration
	MaxRetries    int
}

// ---------- Transport ----------
type TransportConfig struct {
	GRPC GRPCConfig
	HTTP HTTPConfig
	Pool PoolConfig
}

type GRPCConfig struct {
	Enabled           bool
	Port              int
	MaxRecvMsgSize    int
	MaxSendMsgSize    int
	ConnectionTimeout time.Duration
	KeepaliveTime     time.Duration
	KeepaliveTimeout  time.Duration
	MaxConnectionIdle time.Duration
	TLS               TLSConfig
}

type TLSConfig struct {
	Enabled  bool
	CertFile string
	KeyFile  string
	CAFile   string
}

type HTTPConfig struct {
	Enabled bool
	Port    int
}

type PoolConfig struct {
	MaxSize             int
	MinSize             int
	MaxIdleTime         time.Duration
	DialTimeout         time.Duration
	HealthCheckInterval time.Duration
}

// ---------- Metrics ----------
type MetricsConfig struct {
	Enabled    bool
	Port       int
	Path       string
	Interval   time.Duration
	Prometheus PrometheusConfig
}

type PrometheusConfig struct {
	Enabled   bool
	Namespace string
	Subsystem string
}

// ---------- Logger ----------
type LoggerConfig struct {
	Level      string
	Format     string
	Output     string
	File       string
	MaxSize    int
	MaxAge     int
	MaxBackups int
	Compress   bool
}

// Load 从 YAML 配置文件加载配置；path 为空时返回默认零值 Config（可根据需要返回默认值）
func Load(path string) (*Config, error) {
	// 如果没有传路径，直接返回默认的 Config（调用方可以再覆盖）
	if path == "" {
		// 你也可以在这里返回带默认值的 Config
		return &Config{
			Server: ServerConfig{
				Host:           "0.0.0.0",
				Port:           8080,
				ReadTimeout:    5 * time.Second,
				WriteTimeout:   5 * time.Second,
				IdleTimeout:    60 * time.Second,
				MaxHeaderBytes: 1 << 20,
			},
			Transport: TransportConfig{
				Pool: PoolConfig{
					MaxSize:             100,
					MinSize:             10,
					MaxIdleTime:         5 * time.Minute,
					DialTimeout:         5 * time.Second,
					HealthCheckInterval: 30 * time.Second,
				},
				GRPC: GRPCConfig{
					Enabled:           true,
					Port:              50051,
					MaxRecvMsgSize:    4 * 1024 * 1024,
					MaxSendMsgSize:    4 * 1024 * 1024,
					ConnectionTimeout: 5 * time.Second,
					KeepaliveTime:     30 * time.Second,
					KeepaliveTimeout:  5 * time.Second,
					MaxConnectionIdle: 5 * time.Minute,
				},
			},
			Metrics: MetricsConfig{
				Enabled: false,
				Port:    9090,
				Path:    "/metrics",
			},
			Logger: LoggerConfig{
				Level:  "info",
				Format: "json",
				Output: "stdout",
			},
		}, nil
	}

	bs, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(bs, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}

	return &cfg, nil
}
