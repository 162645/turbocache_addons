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

// waitForGRPCReady 通过实际拨号确保 gRPC 端口已经可以接受连接
func waitForGRPCReady(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout: node at %s is not responding", addr)
}

// retryWithBackoff 封装重试逻辑
func retryWithBackoff(attempts int, sleep time.Duration, f func() error) error {
	var err error
	for i := 0; i < attempts; i++ {
		if err = f(); err == nil {
			return nil
		}
		time.Sleep(sleep + time.Duration(i)*100*time.Millisecond)
	}
	return err
}

func TestClusterIntegration(t *testing.T) {
	etcdEndpoints := []string{"127.0.0.1:2379"}
	rootPath := "/turbocache/test/cluster"
	CleanupEtcd(etcdEndpoints, rootPath)

	// 1. 启动节点 1
	port1 := GetFreePort()
	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	node1, err := StartTestNode("node1", port1, etcdEndpoints, rootPath)
	if err != nil {
		t.Skip("Skipping integration test: etcd not available")
		return
	}
	defer node1.Stop()
	require.NoError(t, waitForGRPCReady(addr1, 5*time.Second))

	// 2. 启动节点 2
	port2 := GetFreePort()
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)
	node2, err := StartTestNode("node2", port2, etcdEndpoints, rootPath)
	require.NoError(t, err)
	defer node2.Stop()
	require.NoError(t, waitForGRPCReady(addr2, 5*time.Second))

	// 等待哈希环稳定
	time.Sleep(2 * time.Second)

	ctx := context.Background()
	key := "cluster-test-key"
	val := "cluster-test-value"
	entry := value.NewEntry(key, value.NewByteViewFromString(val), time.Hour)

	// 3. 写入数据 (带重试)
	err = retryWithBackoff(5, 500*time.Millisecond, func() error {
		return node1.MultiCache.Set(ctx, key, entry)
	})
	require.NoError(t, err)

	// 4. 在不同节点读取数据
	t.Run("ReadFromNode1", func(t *testing.T) {
		res, err := node1.MultiCache.Get(ctx, key)
		require.NoError(t, err)
		assert.Equal(t, val, res.Value.String())
	})

	t.Run("ReadFromNode2", func(t *testing.T) {
		var res *value.Entry // 🔧 关键修复点：使用指针类型
		err = retryWithBackoff(3, 500*time.Millisecond, func() error {
			var getErr error
			res, getErr = node2.MultiCache.Get(ctx, key)
			return getErr
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, val, res.Value.String())
	})

	t.Log("Cluster integration test passed")
}
