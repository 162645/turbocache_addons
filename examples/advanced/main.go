package main

//
//import (
//	"context"
//	"fmt"
//	"log"
//	"sync"
//	"time"
//
//	"turbocache/pkg/client"
//)
//
//func main() {
//	fmt.Println("=== TurboCache Advanced Features Example ===\n")
//
//	// 创建客户端
//	c, err := client.NewClient(&client.Config{
//		Servers:     []string{"localhost:9090"},
//		Timeout:     5 * time.Second,
//		MaxRetries:  3,
//		EnableAsync: true,
//	})
//	if err != nil {
//		log.Fatalf("Failed to create client: %v", err)
//	}
//	defer c.Close()
//
//	ctx := context.Background()
//
//	// 示例1: 异步操作
//	asyncExample(ctx, c)
//
//	// 示例2: 并发访问
//	concurrencyExample(ctx, c)
//
//	// 示例3: 条件设置
//	conditionalSetExample(ctx, c)
//
//	// 示例4: 热点数据处理
//	hotKeyExample(ctx, c)
//
//	// 示例5: 缓存穿透防护
//	cacheProtectionExample(ctx, c)
//
//	fmt.Println("\n=== All advanced examples completed ===")
//}
//
//// asyncExample 异步操作示例
//func asyncExample(ctx context.Context, c *client.Client) {
//	fmt.Println("Example 1: Async Operations")
//	fmt.Println("----------------------------")
//
//	asyncClient := c.Async()
//
//	// 异步设置多个键
//	futures := make([]*client.Future, 0)
//	for i := 0; i < 10; i++ {
//		key := fmt.Sprintf("async:key:%d", i)
//		value := []byte(fmt.Sprintf("value-%d", i))
//		future := asyncClient.Set(ctx, key, value, time.Minute)
//		futures = append(futures, future)
//	}
//
//	// 等待所有操作完成
//	for i, future := range futures {
//		if err := future.Wait(ctx); err != nil {
//			log.Printf("Async set %d failed: %v", i, err)
//		}
//	}
//	fmt.Println("✓ Async set 10 keys completed")
//
//	// 异步批量获取
//	keys := make([]string, 10)
//	for i := 0; i < 10; i++ {
//		keys[i] = fmt.Sprintf("async:key:%d", i)
//	}
//
//	resultFuture := asyncClient.BatchGet(ctx, keys)
//	results, err := resultFuture.GetResult(ctx)
//	if err != nil {
//		log.Fatalf("Async batch get failed: %v", err)
//	}
//	fmt.Printf("✓ Async batch get returned %d results\n\n", len(results.(map[string][]byte)))
//}
//
//// concurrencyExample 并发访问示例
//func concurrencyExample(ctx context.Context, c *client.Client) {
//	fmt.Println("Example 2: Concurrent Access")
//	fmt.Println("-----------------------------")
//
//	// 准备测试数据
//	testKey := "concurrent:counter"
//	c.Set(ctx, testKey, []byte("0"), time.Minute)
//
//	// 并发读取
//	var wg sync.WaitGroup
//	concurrency := 100
//	successCount := 0
//	var mu sync.Mutex
//
//	startTime := time.Now()
//
//	for i := 0; i < concurrency; i++ {
//		wg.Add(1)
//		go func(id int) {
//			defer wg.Done()
//
//			_, err := c.Get(ctx, testKey)
//			if err == nil {
//				mu.Lock()
//				successCount++
//				mu.Unlock()
//			}
//		}(i)
//	}
//
//	wg.Wait()
//	duration := time.Since(startTime)
//
//	fmt.Printf("✓ Completed %d concurrent reads in %v\n", successCount, duration)
//	fmt.Printf("✓ Average latency: %.2fms per request\n\n",
//		float64(duration.Milliseconds())/float64(successCount))
//}
//
//// conditionalSetExample 条件设置示例
//func conditionalSetExample(ctx context.Context, c *client.Client) {
//	fmt.Println("Example 3: Conditional Set")
//	fmt.Println("---------------------------")
//
//	key := "conditional:lock"
//
//	// NX操作 - 仅在键不存在时设置
//	err := c.SetNX(ctx, key, []byte("lock-owner-1"), time.Second*10)
//	if err != nil {
//		log.Fatalf("SetNX failed: %v", err)
//	}
//	fmt.Println("✓ SetNX succeeded (key did not exist)")
//
//	// 再次尝试SetNX - 应该失败
//	err = c.SetNX(ctx, key, []byte("lock-owner-2"), time.Second*10)
//	if err != nil {
//		fmt.Println("✓ SetNX failed as expected (key exists)")
//	}
//
//	// XX操作 - 仅在键存在时设置
//	err = c.SetXX(ctx, key, []byte("lock-owner-1-renewed"), time.Second*10)
//	if err != nil {
//		log.Fatalf("SetXX failed: %v", err)
//	}
//	fmt.Println("✓ SetXX succeeded (key exists)")
//
//	// 清理
//	c.Delete(ctx, key)
//
//	// 尝试XX操作在不存在的键上
//	err = c.SetXX(ctx, "nonexistent:key", []byte("value"), time.Second*10)
//	if err != nil {
//		fmt.Println("✓ SetXX failed as expected (key does not exist)")
//	}
//	fmt.Println()
//}
//
//// hotKeyExample 热点数据处理示例
//func hotKeyExample(ctx context.Context, c *client.Client) {
//	fmt.Println("Example 4: Hot Key Handling")
//	fmt.Println("----------------------------")
//
//	hotKey := "hot:popular-product"
//	c.Set(ctx, hotKey, []byte("iPhone 15 Pro"), time.Minute)
//
//	// 模拟热点访问
//	var wg sync.WaitGroup
//	accessCount := 1000
//	startTime := time.Now()
//
//	for i := 0; i < accessCount; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//			c.Get(ctx, hotKey)
//		}()
//	}
//
//	wg.Wait()
//	duration := time.Since(startTime)
//
//	fmt.Printf("✓ Handled %d hot key accesses in %v\n", accessCount, duration)
//	fmt.Printf("✓ Throughput: %.0f requests/second\n",
//		float64(accessCount)/duration.Seconds())
//
//	// 获取热点统计
//	stats, _ := c.Stats(ctx)
//	fmt.Printf("✓ Current hit rate: %.2f%%\n\n", stats.HitRate*100)
//}
//
//// cacheProtectionExample 缓存防护示例
//func cacheProtectionExample(ctx context.Context, c *client.Client) {
//	fmt.Println("Example 5: Cache Protection")
//	fmt.Println("----------------------------")
//
//	// 模拟缓存穿透场景 - 查询不存在的键
//	nonExistentKeys := make([]string, 100)
//	for i := 0; i < 100; i++ {
//		nonExistentKeys[i] = fmt.Sprintf("nonexistent:key:%d", i)
//	}
//
//	missCount := 0
//	for _, key := range nonExistentKeys {
//		_, err := c.Get(ctx, key)
//		if err != nil {
//			missCount++
//		}
//	}
//
//	fmt.Printf("✓ Handled %d cache misses safely (bloom filter protection)\n", missCount)
//
//	// 模拟缓存击穿场景 - 同时请求相同的过期键
//	expiredKey := "protection:expired-key"
//	c.Set(ctx, expiredKey, []byte("value"), time.Millisecond*100)
//	time.Sleep(time.Millisecond * 150) // 等待过期
//
//	var wg sync.WaitGroup
//	simultaneousRequests := 100
//	actualLoads := 0
//	var loadMu sync.Mutex
//
//	// 使用singleflight确保只有一个请求加载数据
//	for i := 0; i < simultaneousRequests; i++ {
//		wg.Add(1)
//		go func() {
//			defer wg.Done()
//
//			// 尝试获取，如果不存在则加载
//			_, err := c.GetOrLoad(ctx, expiredKey, func() ([]byte, error) {
//				loadMu.Lock()
//				actualLoads++
//				loadMu.Unlock()
//				return []byte("loaded-value"), nil
//			}, time.Minute)
//
//			if err != nil {
//				log.Printf("GetOrLoad failed: %v", err)
//			}
//		}()
//	}
//
//	wg.Wait()
//
//	fmt.Printf("✓ %d simultaneous requests, only %d actual loads (singleflight protection)\n",
//		simultaneousRequests, actualLoads)
//
//	// 缓存雪崩防护 - 随机TTL
//	fmt.Println("✓ Using random TTL jitter to prevent cache avalanche")
//	for i := 0; i < 10; i++ {
//		key := fmt.Sprintf("avalanche:key:%d", i)
//		baseTTL := time.Minute
//		jitter := time.Duration(i) * time.Second // 添加抖动
//		c.Set(ctx, key, []byte("value"), baseTTL+jitter)
//	}
//	fmt.Println("✓ Set 10 keys with jittered TTL")
//}
