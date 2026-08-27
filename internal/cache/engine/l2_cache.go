package engine

import (
	"context"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	"turbocache/api/generated/cachepb"
	"turbocache/internal/cache/storage"
	"turbocache/internal/cache/value"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/config"
	"turbocache/internal/protection/bloom"
	"turbocache/internal/protection/circuit"
	"turbocache/internal/protection/negative"
	"turbocache/internal/protection/singleflight"
	"turbocache/internal/transport/pool"
	"turbocache/internal/utils/logger"
)

// L2Cache L2分布式缓存引擎(带保护机制)
type L2Cache struct {
	config   *config.L2CacheConfig
	logger   logger.Logger
	hashRing *hash.ConsistentHash
	connPool *pool.ConnectionPool

	// 本地节点信息
	localNodeID string
	// ========== 保护机制 ==========
	// 布隆过滤器 - 防穿透
	bloomFilter  *bloom.BloomFilter
	bloomEnabled bool
	bloomConfig  config.BloomFilterConfig
	// 熔断器管理器 - 防雪崩
	circuitManager *circuit.CircuitBreakerManager
	circuitEnabled bool
	// 负缓存 - 缓存空结果
	negativeCache   *negative.NegativeCache
	negativeEnabled bool
	// SingleFlight - 防击穿
	singleFlight singleflight.Group
	sfEnabled    bool

	// Cluster Bloom state. A negative result is authoritative only while ready
	// and not stale; otherwise reads fail open to the normal L2 route.
	bloomReady      atomic.Bool
	bloomStale      atomic.Bool
	bloomVersion    atomic.Uint64
	bloomFailOpen   int64
	bloomRejected   int64
	bloomSyncErrors int64
	bloomRebuilds   int64
	bloomQueue      chan bloomUpdate
	bloomStop       chan struct{}
	bloomWG         sync.WaitGroup
	bloomKeys       func(context.Context) ([]string, error)
	bloomKeysMu     sync.RWMutex

	// 统计信息
	stats *L2Stats

	// 生命周期管理
	closed    int32
	closeOnce sync.Once
	asyncSem  chan struct{}
	asyncWG   sync.WaitGroup
}

type bloomUpdate struct {
	key  string
	done chan error
}

// L2Stats L2缓存统计信息
type L2Stats struct {
	// 基本统计
	totalKeys int64
	totalSize int64

	// 操作统计
	getCount    int64
	setCount    int64
	deleteCount int64

	// 命中统计
	localHitCount  int64
	remoteHitCount int64
	missCount      int64

	// 网络统计
	networkCalls  int64
	networkErrors int64

	// 复制统计
	replicationCount  int64
	replicationErrors int64

	// ========== 保护机制统计 ==========
	bloomFilterHits    int64 // 布隆过滤器拦截次数
	bloomFilterMisses  int64 // 布隆过滤器放行次数
	negativeCacheHits  int64 // 负缓存命中次数
	circuitOpenCount   int64 // 熔断器打开次数
	singleFlightShared int64 // SingleFlight共享次数

	// 时间统计
	startTime  int64
	lastAccess int64
	lastUpdate int64
}

// NewL2Cache 创建L2分布式缓存实例
func NewL2Cache(
	cfg *config.L2CacheConfig,
	protectionCfg *config.ProtectionConfig,
	hashRing *hash.ConsistentHash,
	connPool *pool.ConnectionPool,
	localNodeID string,
) (*L2Cache, error) {
	if cfg == nil {
		return nil, fmt.Errorf("L2 cache config is required")
	}
	if hashRing == nil {
		return nil, fmt.Errorf("hash ring is required")
	}
	if connPool == nil {
		return nil, fmt.Errorf("connection pool is required")
	}

	cache := &L2Cache{
		config:      cfg,
		logger:      logger.WithField("component", "l2_cache"),
		hashRing:    hashRing,
		connPool:    connPool,
		localNodeID: localNodeID,
		stats: &L2Stats{
			startTime: time.Now().UnixNano(),
		},
		// Bound eventual-replication work. Without this guard a warmup or traffic
		// spike can create one goroutine per key per replica and exhaust a node.
		asyncSem: make(chan struct{}, 256),
	}

	// 初始化保护机制
	if err := cache.initProtections(protectionCfg); err != nil {
		return nil, fmt.Errorf("failed to init protections: %w", err)
	}

	cache.logger.Infof("L2 cache initialized: replication_factor=%d, consistency=%s, protections=[bloom=%v, circuit=%v, negative=%v, singleflight=%v]",
		cfg.ReplicationFactor, cfg.ConsistencyLevel,
		cache.bloomEnabled, cache.circuitEnabled, cache.negativeEnabled, cache.sfEnabled)

	return cache, nil
}

// initProtections 初始化保护机制
func (l2 *L2Cache) initProtections(protectionCfg *config.ProtectionConfig) error {
	if protectionCfg == nil {
		// 默认只启用SingleFlight
		l2.sfEnabled = true
		l2.logger.Info("Protection config is nil, only SingleFlight enabled")
		return nil
	}

	// 1. 初始化布隆过滤器
	if protectionCfg.BloomFilter.Enabled {
		bloomConfig := &bloom.Config{
			ExpectedElements:  uint64(protectionCfg.BloomFilter.ExpectedItems),
			FalsePositiveRate: protectionCfg.BloomFilter.FalsePositiveRate,
			AutoRefresh:       protectionCfg.BloomFilter.RefreshInterval > 0,
			RefreshPeriod:     protectionCfg.BloomFilter.RefreshInterval,
		}
		l2.bloomFilter = bloom.NewBloomFilter(bloomConfig)
		l2.bloomEnabled = true
		l2.bloomConfig = protectionCfg.BloomFilter
		queueSize := protectionCfg.BloomFilter.QueueSize
		if queueSize <= 0 {
			queueSize = 16384
		}
		l2.bloomQueue = make(chan bloomUpdate, queueSize)
		l2.bloomStop = make(chan struct{})
		l2.startBloomWorkers(protectionCfg.BloomFilter)
		l2.logger.Info("Bloom filter enabled for L2 cache")
	}

	// 2. 初始化熔断器管理器
	if protectionCfg.CircuitBreaker.Enabled {
		cbConfig := &circuit.Config{
			Enabled:          true,
			Threshold:        protectionCfg.CircuitBreaker.Threshold,
			Timeout:          protectionCfg.CircuitBreaker.Timeout,
			MaxRequests:      protectionCfg.CircuitBreaker.MaxRequests,
			FailureRatio:     protectionCfg.CircuitBreaker.FailureRatio,
			MinRequests:      20, // 默认最小请求数
			SuccessThreshold: 5,  // 默认成功阈值
		}
		l2.circuitManager = circuit.NewCircuitBreakerManager(cbConfig)
		l2.circuitEnabled = true
		l2.logger.Info("Circuit breaker enabled for L2 cache")
	}

	// 3. 初始化负缓存
	if protectionCfg.NegativeCache.Enabled {
		negConfig := &negative.Config{
			DefaultTTL:      protectionCfg.NegativeCache.TTL,
			MaxSize:         protectionCfg.NegativeCache.MaxSize,
			CleanupInterval: time.Minute, // 默认清理间隔
		}
		l2.negativeCache = negative.NewNegativeCache(negConfig)
		l2.negativeEnabled = true
		l2.logger.Info("Negative cache enabled for L2 cache")
	}

	// 4. 初始化SingleFlight
	if protectionCfg.SingleFlight.Enabled {
		l2.sfEnabled = true
		l2.logger.Info("SingleFlight enabled for L2 cache")
	}

	return nil
}

// Get 获取缓存值(带保护机制)
func (l2 *L2Cache) Get(ctx context.Context, key string) (*value.Entry, error) {
	if l2.isClosed() {
		return nil, fmt.Errorf("L2 cache is closed")
	}

	atomic.AddInt64(&l2.stats.getCount, 1)
	atomic.StoreInt64(&l2.stats.lastAccess, time.Now().UnixNano())

	// ========== 保护层1: 布隆过滤器 - 快速判断key是否可能存在 ==========
	if l2.bloomEnabled && l2.bloomFilter != nil && l2.bloomReady.Load() && !l2.bloomStale.Load() {
		if !l2.bloomFilter.TestString(key) {
			// 布隆过滤器判断key绝对不存在
			atomic.AddInt64(&l2.stats.bloomFilterHits, 1)
			atomic.AddInt64(&l2.stats.missCount, 1)
			l2.logger.Debugf("Bloom filter blocked non-existent key: %s", key)
			return nil, fmt.Errorf("key not found (bloom filter): %s", key)
		}
		atomic.AddInt64(&l2.stats.bloomFilterMisses, 1)
	} else if l2.bloomEnabled {
		atomic.AddInt64(&l2.bloomFailOpen, 1)
	}

	// ========== 保护层2: 负缓存 - 检查是否是已知的不存在的key ==========
	if l2.negativeEnabled && l2.negativeCache != nil {
		if entry, exists := l2.negativeCache.Get(key); exists {
			atomic.AddInt64(&l2.stats.negativeCacheHits, 1)
			atomic.AddInt64(&l2.stats.missCount, 1)
			l2.logger.Debugf("Negative cache hit for key: %s, reason: %s", key, entry.Reason)
			return nil, fmt.Errorf("key not found (negative cache): %s", key)
		}
	}

	// 获取负责该键的节点
	nodes := l2.hashRing.GetNodes(key, l2.config.ReplicationFactor)
	if len(nodes) == 0 {
		atomic.AddInt64(&l2.stats.missCount, 1)
		return nil, fmt.Errorf("no nodes available for key: %s", key)
	}

	// 检查是否在本地节点
	isLocal := l2.isLocalNode(nodes[0])
	if isLocal {
		atomic.AddInt64(&l2.stats.localHitCount, 1)
		// 🔧 修复：如果是在本地节点，且走到了这里（说明 L1 没中），
		// 应该返回 KeyNotFound，而不是一个会导致 Panic 的自定义错误字符串。
		return nil, fmt.Errorf("%w: should be handled by L1 cache", storage.ErrKeyNotFound)
	}

	// ========== 保护层3: SingleFlight - 防止缓存击穿 ==========
	var entry *value.Entry
	var err error

	if l2.sfEnabled {
		// 使用SingleFlight确保同一个key只有一个请求在执行
		result, sfErr, shared := l2.singleFlight.Do(key, func() (interface{}, error) {
			return l2.getFromRemoteWithCircuit(ctx, key, nodes[0])
		})

		if shared {
			atomic.AddInt64(&l2.stats.singleFlightShared, 1)
			l2.logger.Debugf("SingleFlight shared result for key: %s", key)
		}

		if sfErr != nil {
			err = sfErr
		} else {
			entry = result.(*value.Entry)
		}
	} else {
		// 不使用SingleFlight,直接获取
		entry, err = l2.getFromRemoteWithCircuit(ctx, key, nodes[0])
	}

	if err != nil {
		atomic.AddInt64(&l2.stats.missCount, 1)

		// 如果是"不存在"类型的错误,添加到负缓存
		if l2.negativeEnabled && l2.negativeCache != nil && isNotFoundError(err) {
			l2.negativeCache.Set(key, err.Error(), negative.ErrorTypeNotFound)
			l2.logger.Debugf("Added key to negative cache: %s", key)
		}

		return nil, err
	}

	atomic.AddInt64(&l2.stats.remoteHitCount, 1)

	// 成功获取后,添加到布隆过滤器
	if l2.bloomEnabled && l2.bloomFilter != nil {
		l2.bloomFilter.AddString(key)
	}

	return entry, nil
}

// getFromRemoteWithCircuit 从远程节点获取数据(带熔断器保护)
func (l2 *L2Cache) getFromRemoteWithCircuit(ctx context.Context, key string, node *hash.Node) (*value.Entry, error) {
	// ========== 保护层4: 熔断器 - 防止节点故障导致雪崩 ==========
	if l2.circuitEnabled && l2.circuitManager != nil {
		breaker := l2.circuitManager.GetOrCreate(node.ID)

		var resultEntry *value.Entry

		// 使用熔断器执行远程调用
		err := breaker.Execute(ctx, func() error {
			entry, getErr := l2.getFromRemote(ctx, key, node)
			if getErr != nil {
				return getErr
			}
			resultEntry = entry
			return nil
		})

		if err != nil {
			if err == circuit.ErrCircuitOpen {
				atomic.AddInt64(&l2.stats.circuitOpenCount, 1)
				l2.logger.Warnf("Circuit breaker open for node: %s", node.ID)
			}
			return nil, err
		}

		return resultEntry, nil
	}

	// 直接调用(如果没有熔断器或熔断器未启用)
	return l2.getFromRemote(ctx, key, node)
}

// 🔧 修改：getFromRemote 从远程节点获取数据（添加请求深度标记）
func (l2 *L2Cache) getFromRemote(ctx context.Context, key string, node *hash.Node) (*value.Entry, error) {
	atomic.AddInt64(&l2.stats.networkCalls, 1)

	var entry *value.Entry

	err := l2.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		// 🔑 关键修改：在请求头中添加"转发深度"标记
		req := &cachepb.GetRequest{
			Key:   key,
			Group: "",
			Metadata: &cachepb.RequestMetadata{
				RequestId: generateRequestID(),
				ClientId:  l2.localNodeID,
				Headers: map[string]string{
					"X-TurboCache-Depth":  "0",            // 初始深度为0
					"X-TurboCache-Source": l2.localNodeID, // 源节点ID
				},
			},
		}

		resp, rpcErr := client.Get(ctx, req)
		if rpcErr != nil {
			return rpcErr
		}

		if !resp.Found {
			return fmt.Errorf("key not found: %s", key)
		}

		byteView := value.NewByteView(resp.Value)
		entry = &value.Entry{
			Value: byteView,
		}

		if resp.Metadata != nil {
			metadata := &value.Metadata{}
			entry.Metadata = metadata
		}

		return nil
	})

	if err != nil {
		atomic.AddInt64(&l2.stats.networkErrors, 1)
		return nil, err
	}

	return entry, nil
}

// 🔧 新增：生成请求ID
func generateRequestID() string {
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), randomString(8))
}

// 🔧 新增：生成随机字符串
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
	}
	return string(b)
}

// Set 设置缓存值(带保护机制)
func (l2 *L2Cache) Set(ctx context.Context, key string, entry *value.Entry) error {
	if l2.isClosed() {
		return fmt.Errorf("L2 cache is closed")
	}

	atomic.AddInt64(&l2.stats.setCount, 1)
	atomic.StoreInt64(&l2.stats.lastUpdate, time.Now().UnixNano())

	if l2.negativeEnabled && l2.negativeCache != nil {
		l2.negativeCache.Delete(key)
	}
	if l2.sfEnabled {
		l2.singleFlight.Forget(key)
	}

	nodes := l2.hashRing.GetNodes(key, l2.config.ReplicationFactor)
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for key: %s", key)
	}

	var err error
	switch l2.config.ConsistencyLevel {
	case "strong":
		err = l2.setWithStrongConsistency(ctx, key, entry, nodes)
	case "eventual":
		err = l2.setWithEventualConsistency(ctx, key, entry, nodes)
	default:
		err = l2.setWithEventualConsistency(ctx, key, entry, nodes)
	}
	if err != nil {
		return err
	}
	if l2.bloomEnabled {
		// Bloom is an existence index: overwriting a key that the complete global
		// view already admits does not change any bit and needs no cluster RPC.
		alreadyKnown := l2.bloomReady.Load() && !l2.bloomStale.Load() && l2.bloomFilter.TestString(key)
		if alreadyKnown {
			return nil
		}
		l2.ObserveBloomKeys([]string{key})
		if err := l2.syncBloomKey(ctx, key); err != nil {
			l2.bloomStale.Store(true)
			return fmt.Errorf("data stored but cluster bloom update failed: %w", err)
		}
	}
	return nil
}

// Delete 删除缓存值(带保护机制)
func (l2 *L2Cache) Delete(ctx context.Context, key string) error {
	if l2.isClosed() {
		return fmt.Errorf("L2 cache is closed")
	}

	atomic.AddInt64(&l2.stats.deleteCount, 1)

	// 删除后,清理保护机制
	defer func() {
		// 注意: 布隆过滤器不支持删除,只能等待自动刷新

		// 从负缓存中移除
		if l2.negativeEnabled && l2.negativeCache != nil {
			l2.negativeCache.Delete(key)
		}

		// 清除SingleFlight缓存
		if l2.sfEnabled {
			l2.singleFlight.Forget(key)
		}
	}()

	nodes := l2.hashRing.GetNodes(key, l2.config.ReplicationFactor)
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for key: %s", key)
	}

	return l2.deleteFromNodes(ctx, key, nodes)
}

const (
	BloomInternalHeader     = "X-TurboCache-Bloom-Operation"
	BloomDeltaOperation     = "delta"
	BloomSnapshotOperation  = "snapshot"
	BloomLocalKeysOperation = "local-keys"
)

// SetBloomKeysProvider supplies local live keys for cluster-wide rebuilds.
func (l2 *L2Cache) SetBloomKeysProvider(provider func(context.Context) ([]string, error)) {
	l2.bloomKeysMu.Lock()
	l2.bloomKeys = provider
	l2.bloomKeysMu.Unlock()
}

// ObserveBloomKeys applies a received delta without rebroadcasting it.
func (l2 *L2Cache) ObserveBloomKeys(keys []string) {
	if !l2.bloomEnabled || l2.bloomFilter == nil {
		return
	}
	for _, key := range keys {
		if key != "" {
			l2.bloomFilter.AddString(key)
		}
	}
	l2.bloomVersion.Add(uint64(len(keys)))
}

func (l2 *L2Cache) syncBloomKey(ctx context.Context, key string) error {
	if !l2.bloomEnabled || key == "" {
		return nil
	}
	update := bloomUpdate{key: key, done: make(chan error, 1)}
	select {
	case l2.bloomQueue <- update:
	case <-ctx.Done():
		return ctx.Err()
	default:
		l2.bloomStale.Store(true)
		atomic.AddInt64(&l2.bloomSyncErrors, 1)
		return fmt.Errorf("bloom update queue is full")
	}
	select {
	case err := <-update.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BloomSnapshot encodes readiness, version, element estimate and the bitset.
func (l2 *L2Cache) BloomSnapshot() ([]byte, error) {
	if !l2.bloomEnabled || l2.bloomFilter == nil {
		return nil, fmt.Errorf("cluster bloom is disabled")
	}
	bits, err := l2.bloomFilter.MarshalBinary()
	if err != nil {
		return nil, err
	}
	stats := l2.bloomFilter.Stats()
	out := make([]byte, 17+len(bits))
	if l2.bloomReady.Load() && !l2.bloomStale.Load() {
		out[0] = 1
	}
	binary.BigEndian.PutUint64(out[1:9], l2.bloomVersion.Load())
	binary.BigEndian.PutUint64(out[9:17], stats.ElementsAdded)
	copy(out[17:], bits)
	return out, nil
}

// ApplyBloomSnapshot performs a double-buffered filter swap.
func (l2 *L2Cache) ApplyBloomSnapshot(snapshot []byte) error {
	if !l2.bloomEnabled || l2.bloomFilter == nil {
		return nil
	}
	if len(snapshot) < 18 {
		return fmt.Errorf("invalid bloom snapshot")
	}
	ready := snapshot[0] == 1
	version := binary.BigEndian.Uint64(snapshot[1:9])
	elements := binary.BigEndian.Uint64(snapshot[9:17])
	if err := l2.bloomFilter.ReplaceBinary(snapshot[17:], elements); err != nil {
		return err
	}
	l2.bloomVersion.Store(version)
	l2.bloomStale.Store(!ready)
	l2.bloomReady.Store(ready)
	return nil
}

func (l2 *L2Cache) startBloomWorkers(cfg config.BloomFilterConfig) {
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	flushInterval := cfg.FlushInterval
	if flushInterval <= 0 {
		flushInterval = 10 * time.Millisecond
	}

	l2.bloomWG.Add(1)
	go func() {
		defer l2.bloomWG.Done()
		ticker := time.NewTicker(flushInterval)
		defer ticker.Stop()
		batch := make([]bloomUpdate, 0, batchSize)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			updates := append([]bloomUpdate(nil), batch...)
			batch = batch[:0]
			keys := make([]string, 0, len(updates))
			for _, update := range updates {
				keys = append(keys, update.key)
			}
			err := l2.broadcastBloomDelta(keys)
			if err != nil {
				atomic.AddInt64(&l2.bloomSyncErrors, 1)
				l2.bloomStale.Store(true)
			}
			for _, update := range updates {
				update.done <- err
			}
		}
		for {
			select {
			case update := <-l2.bloomQueue:
				batch = append(batch, update)
				if len(batch) >= batchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-l2.bloomStop:
				flush()
				return
			}
		}
	}()

	l2.bloomWG.Add(1)
	go l2.bloomMaintenanceLoop()
}

func (l2 *L2Cache) bloomMaintenanceLoop() {
	defer l2.bloomWG.Done()
	delay := l2.bloomConfig.BootstrapDelay
	if delay <= 0 {
		delay = 2 * time.Second
	}
	rebuild := l2.bloomConfig.RefreshInterval
	if rebuild <= 0 {
		rebuild = 5 * time.Minute
	}
	bootstrapTicker := time.NewTicker(delay)
	rebuildTicker := time.NewTicker(rebuild)
	defer bootstrapTicker.Stop()
	defer rebuildTicker.Stop()
	for {
		select {
		case <-bootstrapTicker.C:
			if !l2.bloomReady.Load() {
				l2.bootstrapBloom()
			}
		case <-rebuildTicker.C:
			if l2.isBloomLeader() {
				if err := l2.rebuildClusterBloom(); err != nil {
					atomic.AddInt64(&l2.bloomSyncErrors, 1)
				}
			}
		case <-l2.bloomStop:
			return
		}
	}
}

func (l2 *L2Cache) bloomNodes() []*hash.Node {
	nodeMap := l2.hashRing.GetAllNodes()
	nodes := make([]*hash.Node, 0, len(nodeMap))
	for _, node := range nodeMap {
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].ID < nodes[j].ID })
	return nodes
}

func (l2 *L2Cache) isBloomLeader() bool {
	nodes := l2.bloomNodes()
	return len(nodes) > 0 && nodes[0].ID == l2.localNodeID
}

func (l2 *L2Cache) bootstrapBloom() {
	for _, node := range l2.bloomNodes() {
		if l2.isLocalNode(node) {
			continue
		}
		snapshot, err := l2.fetchBloomSnapshot(node)
		if err == nil && len(snapshot) > 0 && snapshot[0] == 1 {
			if applyErr := l2.ApplyBloomSnapshot(snapshot); applyErr == nil {
				return
			}
		}
	}
	// A deterministic leader bootstraps a fresh in-memory cluster. Other nodes
	// remain fail-open until they pull its ready snapshot on the next tick.
	if l2.isBloomLeader() {
		l2.bloomStale.Store(false)
		l2.bloomReady.Store(true)
	}
}

func (l2 *L2Cache) broadcastBloomDelta(keys []string) error {
	items := make([]*cachepb.SetItem, 0, len(keys))
	for _, key := range keys {
		items = append(items, &cachepb.SetItem{Key: key})
	}
	var firstErr error
	for _, node := range l2.bloomNodes() {
		if l2.isLocalNode(node) {
			continue
		}
		err := l2.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, callErr := cachepb.NewCacheServiceClient(conn).BatchSet(ctx, &cachepb.BatchSetRequest{
				Items: items,
				Metadata: &cachepb.RequestMetadata{Headers: map[string]string{
					BloomInternalHeader:          BloomDeltaOperation,
					"X-TurboCache-Bloom-Version": strconv.FormatUint(l2.bloomVersion.Load(), 10),
				}},
			})
			return callErr
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (l2 *L2Cache) fetchBloomSnapshot(node *hash.Node) ([]byte, error) {
	var snapshot []byte
	err := l2.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		resp, callErr := cachepb.NewCacheServiceClient(conn).BatchGet(ctx, &cachepb.BatchGetRequest{
			Metadata: &cachepb.RequestMetadata{Headers: map[string]string{BloomInternalHeader: BloomSnapshotOperation}},
		})
		if callErr != nil {
			return callErr
		}
		if len(resp.Results) == 0 {
			return fmt.Errorf("empty bloom snapshot")
		}
		snapshot = append([]byte(nil), resp.Results[0].Value...)
		return nil
	})
	return snapshot, err
}

func (l2 *L2Cache) localBloomKeys(ctx context.Context) ([]string, error) {
	l2.bloomKeysMu.RLock()
	provider := l2.bloomKeys
	l2.bloomKeysMu.RUnlock()
	if provider == nil {
		return nil, fmt.Errorf("local key provider is not configured")
	}
	return provider(ctx)
}

func (l2 *L2Cache) fetchLocalKeys(node *hash.Node) ([]string, error) {
	if l2.isLocalNode(node) {
		return l2.localBloomKeys(context.Background())
	}
	var keys []string
	err := l2.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, callErr := cachepb.NewCacheServiceClient(conn).BatchGet(ctx, &cachepb.BatchGetRequest{
			Metadata: &cachepb.RequestMetadata{Headers: map[string]string{BloomInternalHeader: BloomLocalKeysOperation}},
		})
		if callErr != nil {
			return callErr
		}
		for _, result := range resp.Results {
			keys = append(keys, result.Key)
		}
		return nil
	})
	return keys, err
}

func (l2 *L2Cache) rebuildClusterBloom() error {
	all := make(map[string]struct{})
	for _, node := range l2.bloomNodes() {
		keys, err := l2.fetchLocalKeys(node)
		if err != nil {
			return err // keep the old complete snapshot; never publish a partial one
		}
		for _, key := range keys {
			all[key] = struct{}{}
		}
	}
	next := bloom.NewBloomFilter(&bloom.Config{
		ExpectedElements:  uint64(l2.bloomConfig.ExpectedItems),
		FalsePositiveRate: l2.bloomConfig.FalsePositiveRate,
	})
	for key := range all {
		next.AddString(key)
	}
	bits, err := next.MarshalBinary()
	_ = next.Close()
	if err != nil {
		return err
	}
	version := l2.bloomVersion.Add(1)
	snapshot := make([]byte, 17+len(bits))
	snapshot[0] = 1
	binary.BigEndian.PutUint64(snapshot[1:9], version)
	binary.BigEndian.PutUint64(snapshot[9:17], uint64(len(all)))
	copy(snapshot[17:], bits)
	if err := l2.ApplyBloomSnapshot(snapshot); err != nil {
		return err
	}
	atomic.AddInt64(&l2.bloomRebuilds, 1)
	for _, node := range l2.bloomNodes() {
		if l2.isLocalNode(node) {
			continue
		}
		if err := l2.sendBloomSnapshot(node, snapshot); err != nil {
			atomic.AddInt64(&l2.bloomSyncErrors, 1)
		}
	}
	return nil
}

func (l2 *L2Cache) sendBloomSnapshot(node *hash.Node, snapshot []byte) error {
	return l2.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := cachepb.NewCacheServiceClient(conn).BatchSet(ctx, &cachepb.BatchSetRequest{
			Items:    []*cachepb.SetItem{{Key: "__cluster_bloom_snapshot__", Value: snapshot}},
			Metadata: &cachepb.RequestMetadata{Headers: map[string]string{BloomInternalHeader: BloomSnapshotOperation}},
		})
		return err
	})
}

// Stats 获取统计信息(包含保护机制统计)
func (l2 *L2Cache) Stats() (*L2CacheStats, error) {
	if l2.isClosed() {
		return nil, fmt.Errorf("L2 cache is closed")
	}

	stats := &L2CacheStats{
		Level:     "L2",
		Type:      "distributed",
		TotalKeys: atomic.LoadInt64(&l2.stats.totalKeys),
		TotalSize: atomic.LoadInt64(&l2.stats.totalSize),

		GetCount:    atomic.LoadInt64(&l2.stats.getCount),
		SetCount:    atomic.LoadInt64(&l2.stats.setCount),
		DeleteCount: atomic.LoadInt64(&l2.stats.deleteCount),

		LocalHitCount:  atomic.LoadInt64(&l2.stats.localHitCount),
		RemoteHitCount: atomic.LoadInt64(&l2.stats.remoteHitCount),
		MissCount:      atomic.LoadInt64(&l2.stats.missCount),
		HitRate:        l2.calculateHitRate(),

		NetworkCalls:  atomic.LoadInt64(&l2.stats.networkCalls),
		NetworkErrors: atomic.LoadInt64(&l2.stats.networkErrors),

		ReplicationCount:  atomic.LoadInt64(&l2.stats.replicationCount),
		ReplicationErrors: atomic.LoadInt64(&l2.stats.replicationErrors),

		// 保护机制统计
		BloomFilterHits:    atomic.LoadInt64(&l2.stats.bloomFilterHits),
		BloomFilterMisses:  atomic.LoadInt64(&l2.stats.bloomFilterMisses),
		NegativeCacheHits:  atomic.LoadInt64(&l2.stats.negativeCacheHits),
		CircuitOpenCount:   atomic.LoadInt64(&l2.stats.circuitOpenCount),
		SingleFlightShared: atomic.LoadInt64(&l2.stats.singleFlightShared),
		BloomReady:         l2.bloomReady.Load(),
		BloomStale:         l2.bloomStale.Load(),
		BloomVersion:       l2.bloomVersion.Load(),
		BloomFailOpen:      atomic.LoadInt64(&l2.bloomFailOpen),
		BloomRejected:      atomic.LoadInt64(&l2.stats.bloomFilterHits),
		BloomSyncErrors:    atomic.LoadInt64(&l2.bloomSyncErrors),
		BloomRebuilds:      atomic.LoadInt64(&l2.bloomRebuilds),
		BloomQueueDepth:    len(l2.bloomQueue),

		StartTime:  time.Unix(0, atomic.LoadInt64(&l2.stats.startTime)),
		LastAccess: time.Unix(0, atomic.LoadInt64(&l2.stats.lastAccess)),
		LastUpdate: time.Unix(0, atomic.LoadInt64(&l2.stats.lastUpdate)),

		Config: map[string]interface{}{
			"replication_factor":   l2.config.ReplicationFactor,
			"consistency_level":    l2.config.ConsistencyLevel,
			"sync_timeout":         l2.config.SyncTimeout.String(),
			"bloom_enabled":        l2.bloomEnabled,
			"circuit_enabled":      l2.circuitEnabled,
			"negative_enabled":     l2.negativeEnabled,
			"singleflight_enabled": l2.sfEnabled,
		},
	}

	// 添加保护机制的详细统计
	if l2.bloomFilter != nil {
		stats.BloomFilterStats = l2.bloomFilter.Stats()
	}
	if l2.negativeCache != nil {
		stats.NegativeCacheStats = l2.negativeCache.Stats()
	}
	if l2.circuitManager != nil {
		// 获取所有熔断器的统计
		stats.CircuitBreakerStats = make(map[string]*circuit.Stats)
		for name, breaker := range l2.circuitManager.GetAll() {
			stats.CircuitBreakerStats[name] = breaker.GetStats()
		}
	}

	return stats, nil
}

// Close 关闭L2缓存
func (l2 *L2Cache) Close() error {
	var err error
	l2.closeOnce.Do(func() {
		atomic.StoreInt32(&l2.closed, 1)
		l2.asyncWG.Wait()

		// 关闭保护机制
		if l2.bloomFilter != nil {
			if l2.bloomStop != nil {
				close(l2.bloomStop)
				l2.bloomWG.Wait()
			}
			l2.bloomFilter.Close()
		}
		if l2.negativeCache != nil {
			l2.negativeCache.Close()
		}

		l2.logger.Info("L2 cache closed")
	})

	return err
}

// ========== 私有辅助方法 ==========

func (l2 *L2Cache) isClosed() bool {
	return atomic.LoadInt32(&l2.closed) != 0
}

func (l2 *L2Cache) isLocalNode(node *hash.Node) bool {
	return node.ID == l2.localNodeID
}

// isNotFoundError 判断是否是"不存在"类型的错误
func isNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "not found") ||
		strings.Contains(errMsg, "does not exist") ||
		strings.Contains(errMsg, "no such key")
}

func (l2 *L2Cache) calculateHitRate() float64 {
	localHits := atomic.LoadInt64(&l2.stats.localHitCount)
	remoteHits := atomic.LoadInt64(&l2.stats.remoteHitCount)
	misses := atomic.LoadInt64(&l2.stats.missCount)

	totalRequests := localHits + remoteHits + misses
	if totalRequests == 0 {
		return 0
	}

	return float64(localHits+remoteHits) / float64(totalRequests)
}

// ========== 原有的复制和删除方法 ==========

func (l2 *L2Cache) setWithStrongConsistency(ctx context.Context, key string, entry *value.Entry, nodes []*hash.Node) error {
	var wg sync.WaitGroup
	errors := make([]error, len(nodes))

	for i, node := range nodes {
		wg.Add(1)
		go func(index int, n *hash.Node) {
			defer wg.Done()

			if l2.isLocalNode(n) {
				return
			}

			errors[index] = l2.setToRemote(ctx, key, entry, n)
		}(i, node)
	}

	wg.Wait()

	var failedCount int
	for _, err := range errors {
		if err != nil {
			failedCount++
			l2.logger.Warnf("Failed to replicate to node: %v", err)
		}
	}

	if failedCount > 0 {
		return fmt.Errorf("failed to replicate to %d nodes", failedCount)
	}

	atomic.AddInt64(&l2.stats.replicationCount, int64(len(nodes)))
	return nil
}

func (l2 *L2Cache) setWithEventualConsistency(ctx context.Context, key string, entry *value.Entry, nodes []*hash.Node) error {
	if len(nodes) == 0 {
		return fmt.Errorf("no nodes available for replication")
	}

	for _, node := range nodes {
		if l2.isLocalNode(node) {
			continue
		}

		select {
		case l2.asyncSem <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		l2.asyncWG.Add(1)
		go func(n *hash.Node) {
			defer func() {
				<-l2.asyncSem
				l2.asyncWG.Done()
			}()
			timeout := l2.config.SyncTimeout
			if timeout <= 0 {
				timeout = 2 * time.Second
			}
			replicaCtx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			if err := l2.setToRemote(replicaCtx, key, entry, n); err != nil {
				atomic.AddInt64(&l2.stats.replicationErrors, 1)
				l2.logger.Warnf("Async replication failed to node %s: %v", n.ID, err)
			} else {
				atomic.AddInt64(&l2.stats.replicationCount, 1)
			}
		}(node)
	}

	return nil
}

func (l2 *L2Cache) setToRemote(ctx context.Context, key string, entry *value.Entry, node *hash.Node) error {
	atomic.AddInt64(&l2.stats.networkCalls, 1)

	return l2.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		req := &cachepb.SetRequest{
			Key:   key,
			Value: entry.Value.ByteSlice(),
			Group: "",
			Metadata: &cachepb.RequestMetadata{Headers: map[string]string{
				"X-TurboCache-Internal": "replica",
			}},
		}

		if entry.Metadata != nil {
			ttl := entry.Metadata.TTL()
			if ttl > 0 {
				req.Ttl = durationpb.New(ttl)
			}
		}

		resp, err := client.Set(ctx, req)
		if err != nil {
			atomic.AddInt64(&l2.stats.networkErrors, 1)
			return err
		}

		if !resp.Success {
			return fmt.Errorf("remote set failed: %s", resp.Status.Message)
		}

		return nil
	})
}

func (l2 *L2Cache) deleteFromNodes(ctx context.Context, key string, nodes []*hash.Node) error {
	var wg sync.WaitGroup
	errors := make([]error, len(nodes))

	for i, node := range nodes {
		wg.Add(1)
		go func(index int, n *hash.Node) {
			defer wg.Done()

			if l2.isLocalNode(n) {
				return
			}

			errors[index] = l2.deleteFromRemote(ctx, key, n)
		}(i, node)
	}

	wg.Wait()

	var successCount int
	for _, err := range errors {
		if err == nil {
			successCount++
		} else {
			l2.logger.Warnf("Failed to delete from node: %v", err)
		}
	}

	if successCount == 0 && len(nodes) > 0 {
		return fmt.Errorf("failed to delete from all nodes")
	}

	return nil
}

func (l2 *L2Cache) deleteFromRemote(ctx context.Context, key string, node *hash.Node) error {
	atomic.AddInt64(&l2.stats.networkCalls, 1)

	return l2.connPool.WithConnection(node.Address, func(conn *grpc.ClientConn) error {
		client := cachepb.NewCacheServiceClient(conn)

		req := &cachepb.DeleteRequest{
			Key:   key,
			Group: "",
			Metadata: &cachepb.RequestMetadata{Headers: map[string]string{
				"X-TurboCache-Internal": "replica",
			}},
		}

		resp, err := client.Delete(ctx, req)
		if err != nil {
			atomic.AddInt64(&l2.stats.networkErrors, 1)
			return err
		}

		if !resp.Success {
			return fmt.Errorf("remote delete failed: %s", resp.Status.Message)
		}

		return nil
	})
}

// ========== 统计信息结构 ==========

// L2CacheStats L2缓存统计信息(扩展版)
type L2CacheStats struct {
	Level     string `json:"level"`
	Type      string `json:"type"`
	TotalKeys int64  `json:"total_keys"`
	TotalSize int64  `json:"total_size"`

	GetCount    int64 `json:"get_count"`
	SetCount    int64 `json:"set_count"`
	DeleteCount int64 `json:"delete_count"`

	LocalHitCount  int64   `json:"local_hit_count"`
	RemoteHitCount int64   `json:"remote_hit_count"`
	MissCount      int64   `json:"miss_count"`
	HitRate        float64 `json:"hit_rate"`

	NetworkCalls  int64 `json:"network_calls"`
	NetworkErrors int64 `json:"network_errors"`

	ReplicationCount  int64 `json:"replication_count"`
	ReplicationErrors int64 `json:"replication_errors"`

	// 保护机制统计
	BloomFilterHits    int64  `json:"bloom_filter_hits"`
	BloomFilterMisses  int64  `json:"bloom_filter_misses"`
	NegativeCacheHits  int64  `json:"negative_cache_hits"`
	CircuitOpenCount   int64  `json:"circuit_open_count"`
	SingleFlightShared int64  `json:"singleflight_shared"`
	BloomReady         bool   `json:"bloom_ready"`
	BloomStale         bool   `json:"bloom_stale"`
	BloomVersion       uint64 `json:"bloom_version"`
	BloomFailOpen      int64  `json:"bloom_fail_open"`
	BloomRejected      int64  `json:"bloom_rejected"`
	BloomSyncErrors    int64  `json:"bloom_sync_errors"`
	BloomRebuilds      int64  `json:"bloom_rebuilds"`
	BloomQueueDepth    int    `json:"bloom_queue_depth"`

	StartTime  time.Time `json:"start_time"`
	LastAccess time.Time `json:"last_access"`
	LastUpdate time.Time `json:"last_update"`

	Config map[string]interface{} `json:"config"`

	// 保护机制详细统计
	BloomFilterStats    *bloom.Stats              `json:"bloom_filter_stats,omitempty"`
	NegativeCacheStats  *negative.Stats           `json:"negative_cache_stats,omitempty"`
	CircuitBreakerStats map[string]*circuit.Stats `json:"circuit_breaker_stats,omitempty"`
}
