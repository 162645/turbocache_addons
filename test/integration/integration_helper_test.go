package integration

import (
	"context"
	"fmt"
	"net"
	"time"

	"turbocache/internal/cache/engine"
	"turbocache/internal/cluster/discovery"
	"turbocache/internal/cluster/hash"
	"turbocache/internal/config"
	"turbocache/internal/transport/grpc"
	"turbocache/internal/transport/pool"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestNode 表示一个用于测试的集群节点
type TestNode struct {
	ID         string
	Config     *config.Config
	GRPCServer *grpc.Server
	MultiCache *engine.MultiLevelCache
	Discovery  discovery.DiscoveryService
	HashRing   *hash.ConsistentHash
}

// StartTestNode 启动一个测试节点
func StartTestNode(id string, port int, etcdEndpoints []string, registerPath string) (*TestNode, error) {
	if registerPath == "" {
		registerPath = "/turbocache/test/nodes"
	}
	cfg := &config.Config{
		Cluster: config.ClusterConfig{
			NodeID: id,
			Region: "test-region",
			Zone:   "test-zone",
			Discovery: config.DiscoveryConfig{
				Type:              "etcd",
				Endpoints:         etcdEndpoints,
				RegisterPath:      registerPath,
				LeaseTTL:          5 * time.Second,
				HeartbeatInterval: 1 * time.Second,
				WatchTimeout:      5 * time.Second,
			},
			Hash: config.HashConfig{
				VirtualNodes: 50,
				HashFunction: "xxhash",
			},
		},
		Transport: config.TransportConfig{
			GRPC: config.GRPCConfig{
				Enabled: true,
				Port:    port,
			},
			Pool: config.PoolConfig{
				MaxSize:     10,
				MinSize:     1,
				DialTimeout: 2 * time.Second,
			},
		},
		Cache: config.CacheConfig{
			L1: config.L1CacheConfig{
				Enabled:    true,
				MaxSize:    10 * 1024 * 1024,
				MaxEntries: 1000,
			},
			L2: config.L2CacheConfig{
				Enabled:           true,
				ReplicationFactor: 1,        // 🔧 修复：确保至少有一个副本，否则 GetNodes(key, 0) 返回空
				ConsistencyLevel:  "strong", // 使用强一致性确保测试结果稳定
			},
		},
	}

	// 初始化各组件
	l1Cache, _ := engine.NewL1Cache(&cfg.Cache.L1)
	hashFunc := hash.GetHashFunc(cfg.Cluster.Hash.HashFunction)
	hashRing := hash.NewConsistentHash(cfg.Cluster.Hash.VirtualNodes, hashFunc)

	connPool := pool.NewConnectionPool(&pool.Config{
		MaxSize: 10,
		MinSize: 1,
	})

	l2Cache, _ := engine.NewL2Cache(&cfg.Cache.L2, &cfg.Cache.Protection, hashRing, connPool, id)
	multiCache := engine.NewMultiLevelCache(&cfg.Cache, l1Cache, l2Cache)

	discoveryService, err := discovery.NewEtcdDiscovery(&cfg.Cluster.Discovery)
	if err != nil {
		return nil, err
	}

	grpcServer, err := grpc.NewServer(cfg, multiCache, hashRing, connPool)
	if err != nil {
		return nil, err
	}

	node := &TestNode{
		ID:         id,
		Config:     cfg,
		GRPCServer: grpcServer,
		MultiCache: multiCache,
		Discovery:  discoveryService,
		HashRing:   hashRing,
	}

	// 启动服务器
	go grpcServer.Start()

	// 启动节点发现监听 (类似 main.go)，确保 hashRing 同步
	go func() {
		ctx := context.Background()
		// 1. 加载初始节点
		if initialNodes, err := discoveryService.GetNodes(ctx); err == nil {
			for _, nIn := range initialNodes {
				hashRing.AddNode(&hash.Node{
					ID: nIn.ID, Address: nIn.Address, Weight: nIn.Weight, Metadata: nIn.Metadata,
				})
			}
		}

		// 2. 监听后续变化
		eventCh, err := discoveryService.Watch(ctx)
		if err != nil {
			return
		}
		for event := range eventCh {
			hNode := &hash.Node{
				ID: event.Node.ID, Address: event.Node.Address, Weight: event.Node.Weight, Metadata: event.Node.Metadata,
			}
			switch event.Type {
			case discovery.EventNodeAdded:
				hashRing.AddNode(hNode)
			case discovery.EventNodeUpdated:
				hashRing.UpdateNode(hNode)
			case discovery.EventNodeRemoved:
				hashRing.RemoveNode(hNode.ID)
			}
		}
	}()

	// 注册节点
	nodeInfo := &discovery.NodeInfo{
		ID:      id,
		Address: fmt.Sprintf("127.0.0.1:%d", port), // 🔧 修改：使用 127.0.0.1 避免 localhost 解析问题
		Status:  "healthy",
	}
	discoveryService.Register(context.Background(), nodeInfo)

	// 给一点时间让节点注册和发现完成同步
	time.Sleep(500 * time.Millisecond)

	return node, nil
}

func (n *TestNode) Stop() {
	if n.GRPCServer != nil {
		n.GRPCServer.GracefulStop()
	}
	if n.Discovery != nil {
		n.Discovery.Unregister(context.Background(), n.ID)
		n.Discovery.Close()
	}
	if n.MultiCache != nil {
		n.MultiCache.Close()
	}
}

// GetFreePort 获取一个可用的本地端口
func GetFreePort() int {
	addr, _ := net.ResolveTCPAddr("tcp", "127.0.0.1:0") // 🔧 修改：使用 127.0.0.1
	l, _ := net.ListenTCP("tcp", addr)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// CleanupEtcd 清理 Etcd 中的测试数据，防止 stale 节点影响测试
func CleanupEtcd(endpoints []string, prefix string) error {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return err
	}
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = cli.Delete(ctx, prefix, clientv3.WithPrefix())
	return err
}
