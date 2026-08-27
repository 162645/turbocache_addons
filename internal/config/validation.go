package config

import (
	"fmt"
	"net"
	"strings"
	"time"
)

// Validate 验证配置的有效性
func Validate(config *Config) error {
	if err := validateServer(&config.Server); err != nil {
		return fmt.Errorf("server config: %v", err)
	}

	if err := validateCache(&config.Cache); err != nil {
		return fmt.Errorf("cache config: %v", err)
	}

	if err := validateCluster(&config.Cluster); err != nil {
		return fmt.Errorf("cluster config: %v", err)
	}

	if err := validateTransport(&config.Transport); err != nil {
		return fmt.Errorf("transport config: %v", err)
	}

	if err := validateMetrics(&config.Metrics); err != nil {
		return fmt.Errorf("metrics config: %v", err)
	}

	return nil
}

// validateServer 验证服务器配置
func validateServer(config *ServerConfig) error {
	// 验证主机地址
	if config.Host == "" {
		return fmt.Errorf("host is required")
	}

	if net.ParseIP(config.Host) == nil && config.Host != "localhost" && config.Host != "0.0.0.0" {
		return fmt.Errorf("invalid host address: %s", config.Host)
	}

	// 验证端口
	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d, must be between 1 and 65535", config.Port)
	}

	// 验证超时时间
	if config.ReadTimeout < 0 {
		return fmt.Errorf("read_timeout must be non-negative")
	}

	if config.WriteTimeout < 0 {
		return fmt.Errorf("write_timeout must be non-negative")
	}

	if config.IdleTimeout < 0 {
		return fmt.Errorf("idle_timeout must be non-negative")
	}

	// 验证最大请求头大小
	if config.MaxHeaderBytes <= 0 {
		return fmt.Errorf("max_header_bytes must be positive")
	}

	return nil
}

// validateCache 验证缓存配置
func validateCache(config *CacheConfig) error {
	// 验证L1缓存
	if err := validateL1Cache(&config.L1); err != nil {
		return fmt.Errorf("l1 cache: %v", err)
	}

	// 验证L2缓存
	if err := validateL2Cache(&config.L2); err != nil {
		return fmt.Errorf("l2 cache: %v", err)
	}

	// 验证通用配置
	if config.DefaultTTL < 0 {
		return fmt.Errorf("default_ttl must be non-negative")
	}

	if config.MaxKeySize <= 0 {
		return fmt.Errorf("max_key_size must be positive")
	}

	if config.MaxValueSize <= 0 {
		return fmt.Errorf("max_value_size must be positive")
	}

	// 验证淘汰策略
	validPolicies := map[string]bool{
		"lru": true,
		"lfu": true,
		"arc": true,
	}

	if !validPolicies[strings.ToLower(config.EvictionPolicy)] {
		return fmt.Errorf("invalid eviction_policy: %s, valid options: lru, lfu, arc", config.EvictionPolicy)
	}

	if config.EvictionInterval < 0 {
		return fmt.Errorf("eviction_interval must be non-negative")
	}

	// 验证保护机制
	if err := validateProtection(&config.Protection); err != nil {
		return fmt.Errorf("protection: %v", err)
	}

	return nil
}

// validateL1Cache 验证L1缓存配置
func validateL1Cache(config *L1CacheConfig) error {
	if !config.Enabled {
		return nil // 如果未启用，跳过验证
	}

	if config.MaxSize <= 0 {
		return fmt.Errorf("max_size must be positive")
	}

	if config.MaxEntries <= 0 {
		return fmt.Errorf("max_entries must be positive")
	}

	if config.TTL < 0 {
		return fmt.Errorf("ttl must be non-negative")
	}

	if config.ShardCount <= 0 {
		return fmt.Errorf("shard_count must be positive")
	}

	// 分片数应该是2的幂
	if config.ShardCount&(config.ShardCount-1) != 0 {
		return fmt.Errorf("shard_count must be a power of 2")
	}

	if config.StatsWindow < 0 {
		return fmt.Errorf("stats_window must be non-negative")
	}

	return nil
}

// validateL2Cache 验证L2缓存配置
func validateL2Cache(config *L2CacheConfig) error {
	if !config.Enabled {
		return nil // 如果未启用，跳过验证
	}

	if config.ReplicationFactor <= 0 {
		return fmt.Errorf("replication_factor must be positive")
	}

	if config.ReplicationFactor > 10 {
		return fmt.Errorf("replication_factor too large: %d, maximum is 10", config.ReplicationFactor)
	}

	validLevels := map[string]bool{
		"strong":   true,
		"eventual": true,
	}

	if !validLevels[strings.ToLower(config.ConsistencyLevel)] {
		return fmt.Errorf("invalid consistency_level: %s, valid options: strong, eventual", config.ConsistencyLevel)
	}

	if config.SyncTimeout < 0 {
		return fmt.Errorf("sync_timeout must be non-negative")
	}

	if config.BackupInterval < 0 {
		return fmt.Errorf("backup_interval must be non-negative")
	}

	return nil
}

// validateProtection 验证保护机制配置
func validateProtection(config *ProtectionConfig) error {
	// 验证布隆过滤器
	if err := validateBloomFilter(&config.BloomFilter); err != nil {
		return fmt.Errorf("bloom_filter: %v", err)
	}

	// 验证负缓存
	if err := validateNegativeCache(&config.NegativeCache); err != nil {
		return fmt.Errorf("negative_cache: %v", err)
	}

	// 验证热点检测
	if err := validateHotKeyDetection(&config.HotKeyDetection); err != nil {
		return fmt.Errorf("hot_key_detection: %v", err)
	}

	// 验证熔断器
	if err := validateCircuitBreaker(&config.CircuitBreaker); err != nil {
		return fmt.Errorf("circuit_breaker: %v", err)
	}

	return nil
}

// validateBloomFilter 验证布隆过滤器配置
func validateBloomFilter(config *BloomFilterConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.ExpectedItems <= 0 {
		return fmt.Errorf("expected_items must be positive")
	}

	if config.FalsePositiveRate <= 0 || config.FalsePositiveRate >= 1 {
		return fmt.Errorf("false_positive_rate must be between 0 and 1")
	}

	if config.RefreshInterval < 0 {
		return fmt.Errorf("refresh_interval must be non-negative")
	}

	return nil
}

// validateNegativeCache 验证负缓存配置
func validateNegativeCache(config *NegativeCacheConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.TTL < 0 {
		return fmt.Errorf("ttl must be non-negative")
	}

	if config.MaxSize <= 0 {
		return fmt.Errorf("max_size must be positive")
	}

	return nil
}

// validateHotKeyDetection 验证热点检测配置
func validateHotKeyDetection(config *HotKeyConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.Threshold <= 0 {
		return fmt.Errorf("threshold must be positive")
	}

	if config.Window <= 0 {
		return fmt.Errorf("window must be positive")
	}

	if config.PreloadMargin < 0 {
		return fmt.Errorf("preload_margin must be non-negative")
	}

	return nil
}

// validateCircuitBreaker 验证熔断器配置
func validateCircuitBreaker(config *CircuitBreakerConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.Threshold <= 0 {
		return fmt.Errorf("threshold must be positive")
	}

	if config.Timeout <= 0 {
		return fmt.Errorf("timeout must be positive")
	}

	if config.MaxRequests <= 0 {
		return fmt.Errorf("max_requests must be positive")
	}

	if config.FailureRatio <= 0 || config.FailureRatio >= 1 {
		return fmt.Errorf("failure_ratio must be between 0 and 1")
	}

	return nil
}

// validateCluster 验证集群配置
func validateCluster(config *ClusterConfig) error {
	// NodeID 可以为空，会自动生成

	if config.Weight <= 0 {
		return fmt.Errorf("weight must be positive")
	}

	if config.Weight > 1000 {
		return fmt.Errorf("weight too large: %d, maximum is 1000", config.Weight)
	}

	// 验证服务发现配置
	if err := validateDiscovery(&config.Discovery); err != nil {
		return fmt.Errorf("discovery: %v", err)
	}

	// 验证哈希配置
	if err := validateHash(&config.Hash); err != nil {
		return fmt.Errorf("hash: %v", err)
	}

	// 验证复制配置
	if err := validateReplication(&config.Replication); err != nil {
		return fmt.Errorf("replication: %v", err)
	}

	return nil
}

// validateDiscovery 验证服务发现配置
func validateDiscovery(config *DiscoveryConfig) error {
	validTypes := map[string]bool{
		"etcd":   true,
		"consul": true,
		"static": true,
	}

	if !validTypes[strings.ToLower(config.Type)] {
		return fmt.Errorf("invalid type: %s, valid options: etcd, consul, static", config.Type)
	}

	if len(config.Endpoints) == 0 {
		return fmt.Errorf("endpoints is required")
	}

	// 验证端点地址格式
	for _, endpoint := range config.Endpoints {
		if endpoint == "" {
			return fmt.Errorf("endpoint cannot be empty")
		}
		// 这里可以添加更详细的URL格式验证
	}

	if config.RegisterPath == "" {
		return fmt.Errorf("register_path is required")
	}

	if config.LeaseTTL <= 0 {
		return fmt.Errorf("lease_ttl must be positive")
	}

	if config.HeartbeatInterval <= 0 {
		return fmt.Errorf("heartbeat_interval must be positive")
	}

	if config.WatchTimeout < 0 {
		return fmt.Errorf("watch_timeout must be non-negative")
	}

	if config.RetryInterval < 0 {
		return fmt.Errorf("retry_interval must be non-negative")
	}

	if config.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be non-negative")
	}

	return nil
}

// validateHash 验证哈希配置
func validateHash(config *HashConfig) error {
	validAlgorithms := map[string]bool{
		"consistent": true,
		"rendezvous": true,
	}

	if !validAlgorithms[strings.ToLower(config.Algorithm)] {
		return fmt.Errorf("invalid algorithm: %s, valid options: consistent, rendezvous", config.Algorithm)
	}

	if config.VirtualNodes <= 0 {
		return fmt.Errorf("virtual_nodes must be positive")
	}

	if config.VirtualNodes > 1000 {
		return fmt.Errorf("virtual_nodes too large: %d, maximum is 1000", config.VirtualNodes)
	}

	validHashFunctions := map[string]bool{
		"crc32":  true,
		"sha1":   true,
		"sha256": true,
	}

	if !validHashFunctions[strings.ToLower(config.HashFunction)] {
		return fmt.Errorf("invalid hash_function: %s, valid options: crc32, sha1, sha256", config.HashFunction)
	}

	if config.RebalanceThreshold < 0 || config.RebalanceThreshold > 1 {
		return fmt.Errorf("rebalance_threshold must be between 0 and 1")
	}

	return nil
}

// validateReplication 验证复制配置
func validateReplication(config *ReplicationConfig) error {
	if config.Factor < 0 {
		return fmt.Errorf("factor must be non-negative")
	}

	if config.Factor > 10 {
		return fmt.Errorf("factor too large: %d, maximum is 10", config.Factor)
	}

	validSyncModes := map[string]bool{
		"sync":  true,
		"async": true,
	}

	if !validSyncModes[strings.ToLower(config.SyncMode)] {
		return fmt.Errorf("invalid sync_mode: %s, valid options: sync, async", config.SyncMode)
	}

	if config.SyncTimeout < 0 {
		return fmt.Errorf("sync_timeout must be non-negative")
	}

	if config.RetryInterval < 0 {
		return fmt.Errorf("retry_interval must be non-negative")
	}

	if config.MaxRetries < 0 {
		return fmt.Errorf("max_retries must be non-negative")
	}

	return nil
}

// validateTransport 验证传输层配置
func validateTransport(config *TransportConfig) error {
	// 验证gRPC配置
	if err := validateGRPC(&config.GRPC); err != nil {
		return fmt.Errorf("grpc: %v", err)
	}

	// 验证HTTP配置
	if err := validateHTTP(&config.HTTP); err != nil {
		return fmt.Errorf("http: %v", err)
	}

	// 验证连接池配置
	if err := validatePool(&config.Pool); err != nil {
		return fmt.Errorf("pool: %v", err)
	}

	// 至少要启用一种传输方式
	if !config.GRPC.Enabled && !config.HTTP.Enabled {
		return fmt.Errorf("at least one transport protocol must be enabled")
	}

	return nil
}

// validateGRPC 验证gRPC配置
func validateGRPC(config *GRPCConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d, must be between 1 and 65535", config.Port)
	}

	if config.MaxRecvMsgSize <= 0 {
		return fmt.Errorf("max_recv_msg_size must be positive")
	}

	if config.MaxSendMsgSize <= 0 {
		return fmt.Errorf("max_send_msg_size must be positive")
	}

	if config.ConnectionTimeout < 0 {
		return fmt.Errorf("connection_timeout must be non-negative")
	}

	if config.KeepaliveTime < 0 {
		return fmt.Errorf("keepalive_time must be non-negative")
	}

	if config.KeepaliveTimeout < 0 {
		return fmt.Errorf("keepalive_timeout must be non-negative")
	}

	if config.MaxConnectionIdle < 0 {
		return fmt.Errorf("max_connection_idle must be non-negative")
	}

	return nil
}

// validateHTTP 验证HTTP配置
func validateHTTP(config *HTTPConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d, must be between 1 and 65535", config.Port)
	}

	return nil
}

// validatePool 验证连接池配置
func validatePool(config *PoolConfig) error {
	if config.MaxSize <= 0 {
		return fmt.Errorf("max_size must be positive")
	}

	if config.MinSize < 0 {
		return fmt.Errorf("min_size must be non-negative")
	}

	if config.MinSize > config.MaxSize {
		return fmt.Errorf("min_size (%d) cannot be greater than max_size (%d)", config.MinSize, config.MaxSize)
	}

	if config.MaxIdleTime < 0 {
		return fmt.Errorf("max_idle_time must be non-negative")
	}

	if config.DialTimeout < 0 {
		return fmt.Errorf("dial_timeout must be non-negative")
	}

	if config.HealthCheckInterval < 0 {
		return fmt.Errorf("health_check_interval must be non-negative")
	}

	return nil
}

// validateMetrics 验证监控配置
func validateMetrics(config *MetricsConfig) error {
	if !config.Enabled {
		return nil
	}

	if config.Port <= 0 || config.Port > 65535 {
		return fmt.Errorf("invalid port: %d, must be between 1 and 65535", config.Port)
	}

	if config.Path == "" {
		return fmt.Errorf("path is required")
	}

	if !strings.HasPrefix(config.Path, "/") {
		return fmt.Errorf("path must start with '/'")
	}

	if config.Interval < 0 {
		return fmt.Errorf("interval must be non-negative")
	}

	return nil
}

// ValidateAndFix 验证并修复配置
func ValidateAndFix(config *Config) error {
	// 修复一些常见的配置问题

	// 确保至少有一个传输协议启用
	if !config.Transport.GRPC.Enabled && !config.Transport.HTTP.Enabled {
		config.Transport.GRPC.Enabled = true
	}

	// 确保L1和L2至少有一个启用
	if !config.Cache.L1.Enabled && !config.Cache.L2.Enabled {
		config.Cache.L1.Enabled = true
	}

	// 自动生成NodeID如果未提供
	if config.Cluster.NodeID == "" {
		config.Cluster.NodeID = generateNodeID()
	}

	// 设置合理的默认TTL
	if config.Cache.DefaultTTL == 0 {
		config.Cache.DefaultTTL = time.Hour
	}

	// 确保心跳间隔小于租约TTL
	if config.Cluster.Discovery.HeartbeatInterval >= config.Cluster.Discovery.LeaseTTL {
		config.Cluster.Discovery.HeartbeatInterval = config.Cluster.Discovery.LeaseTTL / 3
	}

	// 验证修复后的配置
	return Validate(config)
}

// generateNodeID 生成节点ID
func generateNodeID() string {
	return fmt.Sprintf("node-%d", time.Now().UnixNano())
}
