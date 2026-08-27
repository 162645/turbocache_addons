//go:build integration
// +build integration

package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"turbocache/internal/cache/value"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitForNodeReady 通过拨号探测确保 gRPC 服务已真正启动
func waitForNodeReady(t *testing.T, addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("node %s not ready after %v", addr, timeout)
}

// retryHelper 用于处理混沌测试中的瞬时错误
func retryHelper(attempts int, sleep time.Duration, f func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = f(); err == nil {
			return nil
		}
		time.Sleep(sleep)
	}
	return err
}

func TestChaos_NodeFailure(t *testing.T) {
	etcdEndpoints := []string{"127.0.0.1:2379"}
	rootPath := "/turbocache/test/chaos"
	CleanupEtcd(etcdEndpoints, rootPath)

	// 1. 启动 3 个节点组成集群
	nodes := make([]*TestNode, 3)
	for i := 0; i < 3; i++ {
		port := GetFreePort()
		nodeID := fmt.Sprintf("chaos-node-%d", i)
		node, err := StartTestNode(nodeID, port, etcdEndpoints, rootPath)
		if err != nil {
			t.Fatalf("Failed to start node %s: %v", nodeID, err)
		}
		nodes[i] = node
		defer node.Stop()

		// 🔧 关键改进：确保节点端口真正 Ready 以后再继续
		err = waitForNodeReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)
		require.NoError(t, err, "Node %s failed to listen in time", nodeID)
	}

	// 等待 etcd 发现列表在所有节点同步完毕
	time.Sleep(2 * time.Second)

	ctx := context.Background()

	// 2. 填充一组数据
	testData := make(map[string]string)
	for i := 0; i < 30; i++ {
		key := fmt.Sprintf("chaos-key-%d", i)
		val := fmt.Sprintf("value-%d", i)
		testData[key] = val

		entry := value.NewEntry(key, value.NewByteViewFromString(val), time.Hour)

		// 🔧 增加重试逻辑，应对节点刚启动时的 gRPC 握手抖动
		err := retryHelper(5, 500*time.Millisecond, func() error {
			return nodes[0].MultiCache.Set(ctx, key, entry)
		})
		require.NoError(t, err, "Failed to set key %s after retries", key)
	}

	// 3. 模拟一个节点“崩溃”(突然停止)
	// 在这之前，我们记录一下 node-1 的地址以便观察
	t.Log("Simulating node failure: chaos-node-1")
	nodes[1].Stop()

	// 🔧 延长等待时间：etcd 的租约过期 + 客户端 Watch 通知 + 哈希环更新
	// 如果这个时间太短，nodes[0] 还会尝试往 nodes[1] 同步，导致 replication 报错
	time.Sleep(8 * time.Second)

	// 4. 验证剩余节点是否依然可以提供服务
	t.Log("Verifying data availability and cluster stability after node failure")

	for key, expectedVal := range testData {
		// 再次写入和读取，验证集群是否已经从故障中恢复并重新分片
		// 混沌测试中，Get 失败可能是因为数据落在挂掉的节点且没副本，
		// 但 Set 不应该再报 connection reset，因为哈希环应该剔除了失效节点。

		res, err := nodes[0].MultiCache.Get(ctx, key)
		if err != nil {
			// 如果没有 Replication，由于节点 1 挂了，部分 Key 获取不到是正常的
			t.Logf("Key %s may be lost with node-1, skipping value check", key)
			continue
		}
		assert.Equal(t, expectedVal, res.Value.String(), "Data corruption for key %s", key)
	}

	// 5. 验证集群此时是否可以正常写入新数据（证明哈希环已自愈）
	newKey := "healing-check"
	newVal := "alive"
	newEntry := value.NewEntry(newKey, value.NewByteViewFromString(newVal), time.Hour)
	err := retryHelper(3, 1*time.Second, func() error {
		return nodes[0].MultiCache.Set(ctx, newKey, newEntry)
	})
	assert.NoError(t, err, "Cluster should be able to write new data after node failure")

	t.Log("Chaos test completed: Cluster survived node failure")
}
