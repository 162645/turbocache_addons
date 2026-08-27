package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"turbocache/pkg/client"
)

func main() {
	fmt.Println("=== TurboCache Basic Usage Example ===")
	fmt.Println()

	// 创建客户端
	c, err := client.NewClient([]string{"localhost:9090"}, &client.Config{
		ConnectionTimeout: 5 * time.Second,  // 原来你写的 Timeout -> 这里对应 ConnectionTimeout
		RequestTimeout:    30 * time.Second, // 可按需设置
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}
	defer c.Close()

	ctx := context.Background()

	// 示例1: 基本的Get/Set操作
	fmt.Println("Example 1: Basic Get/Set")
	fmt.Println("-------------------------")

	// 设置键值
	err = c.Set(ctx, "user:1001", []byte("John Doe"), time.Minute*10)
	if err != nil {
		log.Fatalf("Failed to set: %v", err)
	}
	fmt.Println("✓ Set user:1001 = John Doe")

	// 获取键值
	value, err := c.Get(ctx, "user:1001")
	if err != nil {
		log.Fatalf("Failed to get: %v", err)
	}
	fmt.Printf("✓ Get user:1001 = %s\n\n", string(value))

	// 示例2: 使用缓存组
	fmt.Println("Example 2: Using Cache Groups")
	fmt.Println("------------------------------")

	// 设置到特定组
	err = c.SetWithGroup(ctx, "users", "user:1002", []byte("Jane Smith"), time.Minute*10)
	if err != nil {
		log.Fatalf("Failed to set with group: %v", err)
	}
	fmt.Println("✓ Set users/user:1002 = Jane Smith")

	// 从特定组获取
	value, err = c.GetWithGroup(ctx, "users", "user:1002")
	if err != nil {
		log.Fatalf("Failed to get from group: %v", err)
	}
	fmt.Printf("✓ Get users/user:1002 = %s\n\n", string(value))

	// 示例3: 批量操作
	fmt.Println("Example 3: Batch Operations")
	fmt.Println("----------------------------")

	// 批量设置
	items := map[string][]byte{
		"product:1": []byte("Laptop"),
		"product:2": []byte("Mouse"),
		"product:3": []byte("Keyboard"),
	}

	err = c.BatchSet(ctx, items, time.Minute*10)
	if err != nil {
		log.Fatalf("Failed to batch set: %v", err)
	}
	fmt.Println("✓ Batch set 3 products")

	// 批量获取
	keys := []string{"product:1", "product:2", "product:3"}
	results, err := c.BatchGet(ctx, keys)
	if err != nil {
		log.Fatalf("Failed to batch get: %v", err)
	}

	fmt.Println("✓ Batch get results:")
	for key, value := range results {
		fmt.Printf("  %s = %s\n", key, string(value))
	}
	fmt.Println()

	// 示例4: TTL操作
	fmt.Println("Example 4: TTL Operations")
	fmt.Println("-------------------------")

	// 设置带过期时间的键
	err = c.Set(ctx, "session:abc123", []byte("active"), time.Second*5)
	if err != nil {
		log.Fatalf("Failed to set with TTL: %v", err)
	}
	fmt.Println("✓ Set session:abc123 with 5s TTL")

	// 检查TTL
	ttl, err := c.TTL(ctx, "session:abc123")
	if err != nil {
		log.Fatalf("Failed to get TTL: %v", err)
	}
	fmt.Printf("✓ Current TTL: %v\n", ttl)

	// 等待过期
	fmt.Println("Waiting for expiration...")
	time.Sleep(6 * time.Second)

	// 验证已过期
	_, err = c.Get(ctx, "session:abc123")
	if err != nil {
		fmt.Println("✓ Key expired as expected")
	}
	fmt.Println()

	// 示例5: 删除操作
	fmt.Println("Example 5: Delete Operations")
	fmt.Println("-----------------------------")

	// 删除单个键
	err = c.Delete(ctx, "user:1001")
	if err != nil {
		log.Fatalf("Failed to delete: %v", err)
	}
	fmt.Println("✓ Deleted user:1001")

	// 批量删除
	deleteKeys := []string{"product:1", "product:2", "product:3"}
	err = c.BatchDelete(ctx, deleteKeys)
	if err != nil {
		log.Fatalf("Failed to batch delete: %v", err)
	}
	fmt.Println("✓ Batch deleted products")
	fmt.Println()

	// 示例6: 获取统计信息
	fmt.Println("Example 6: Statistics")
	fmt.Println("---------------------")

	stats, err := c.Stats(ctx)
	if err != nil {
		log.Fatalf("Failed to get stats: %v", err)
	}

	fmt.Printf("✓ Cache Statistics:\n")
	fmt.Printf("  Total Keys: %d\n", stats.TotalKeys)
	fmt.Printf("  Total Size: %d bytes\n", stats.TotalSize)
	fmt.Printf("  Hit Count: %d\n", stats.HitCount)
	fmt.Printf("  Miss Count: %d\n", stats.MissCount)
	fmt.Printf("  Hit Rate: %.2f%%\n", stats.HitRate*100)
	fmt.Println()

	fmt.Println("=== Example completed successfully ===")
}
