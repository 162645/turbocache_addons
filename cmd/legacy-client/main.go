package main // 🔹程序入口包，构建为可执行二进制时必须为 main 包

import (
	"context"       // 🔹用于传递上下文（context）支持超时/取消等
	"encoding/json" // 🔹将 stats 等结构格式化为 JSON 输出
	"flag"          // 🔹命令行参数解析
	"fmt"           // 🔹格式化输出
	"os"            // 🔹与操作系统交互（退出、标准输入/输出）
	"strings"       // 🔹字符串工具（比如转小写）
	"time"          // 🔹时间/超时相关

	"turbocache/pkg/client" // 🔹项目内部客户端包，封装了与 TurboCache 服务的 RPC/HTTP 调用
)

var (
	serverAddr = flag.String("server", "localhost:9090", "TurboCache server address") // 🔹命令行标志：目标服务地址（默认 localhost:9090）
	timeout    = flag.Duration("timeout", 5*time.Second, "Request timeout")           // 🔹命令行标志：默认请求超时 5 秒
	verbose    = flag.Bool("v", false, "Verbose output")                              // 🔹命令行标志：详细输出开关
)

func main() {
	flag.Usage = printUsage // 🔹将 flag 的帮助打印函数替换为自定义 printUsage（显示更友好的用法文档）
	flag.Parse()            // 🔹解析命令行参数，将标志和非标志参数填充到相应变量中

	if flag.NArg() < 1 { // 🔹检查非标志参数数量，至少需要一个命令，如 get/set 等
		printUsage() // 🔹若没有命令，打印用法说明
		os.Exit(1)   // 🔹非正常退出（返回码 1）
	}

	command := flag.Arg(0) // 🔹第一个非标志参数视为命令名称

	// 创建客户端（注意：NewClient 接受 servers 列表和可选 config）
	c, err := client.NewClient([]string{*serverAddr}, nil) // 🔹创建一个客户端实例，连接到 serverAddr（当前实现接收 servers 列表）
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err) // 🔹创建失败则打印错误并退出
		os.Exit(1)
	}
	defer c.Close() // 🔹确保在 main 结束时关闭客户端，释放资源（连接、goroutine 等）

	ctx, cancel := context.WithTimeout(context.Background(), *timeout) // 🔹为后续操作创建带超时的上下文，方便统一控制超时
	defer cancel()                                                     // 🔹确保 cancel 在函数返回时被调用，避免上下文泄露

	// 执行命令
	switch strings.ToLower(command) { // 🔹将命令小写化，支持大小写混合输入
	case "get":
		handleGet(ctx, c) // 🔹读取键值
	case "set":
		handleSet(ctx, c) // 🔹写入键值
	case "delete", "del":
		handleDelete(ctx, c) // 🔹删除键值（支持两种别名）
	case "stats":
		handleStats(ctx, c) // 🔹获取服务/缓存统计
	case "health":
		handleHealth(ctx, c) // 🔹健康检查（Ping）
	case "clear":
		handleClear(ctx, c) // 🔹清除组（CLI 提示 group，但 client.Clear 当前不使用 group）
	case "benchmark", "bench":
		handleBenchmark(ctx, c) // 🔹运行基准测试（简单的 set/get 循环）
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command) // 🔹未知命令处理
		printUsage()
		os.Exit(1)
	}
}

func handleGet(ctx context.Context, c *client.Client) {
	if flag.NArg() < 3 { // 🔹get 命令期望至少两个参数：group 和 key（这里 group 保留但未使用）
		fmt.Fprintln(os.Stderr, "Usage: get <group> <key>")
		os.Exit(1)
	}

	// 当前 client.Get 只接受 key；保留 group 参数但不传给底层（如需传 group，请参见下文）
	// group := flag.Arg(1)
	key := flag.Arg(2) // 🔹实际使用的 key 参数位置

	value, err := c.Get(ctx, key) // 🔹通过 client 调用 Get（底层会做 RPC/HTTP 请求）
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get failed: %v\n", err) // 🔹出现错误（比如网络、超时、key 不存在等）则打印并退出
		os.Exit(1)
	}

	if *verbose { // 🔹verbose 模式下打印更多信息（例如 key 等）
		fmt.Printf("Key: %s\nValue:\n", key)
	}
	fmt.Println(string(value)) // 🔹把返回的字节数组按字符串打印（假设存的是文本）；若是二进制，可按其他方式处理
}

func handleSet(ctx context.Context, c *client.Client) {
	if flag.NArg() < 4 { // 🔹set 期望 group key value 三个位置参数（TTL 可选）
		fmt.Fprintln(os.Stderr, "Usage: set <group> <key> <value> [ttl_seconds]")
		os.Exit(1)
	}

	// group := flag.Arg(1)
	key := flag.Arg(2)   // 🔹key 字符串
	value := flag.Arg(3) // 🔹value 字符串（CLI 简单示例，不处理复杂二进制或空格的情况）

	var ttl time.Duration
	if flag.NArg() >= 5 { // 🔹如果提供了 TTL（秒）
		seconds := 0
		fmt.Sscanf(flag.Arg(4), "%d", &seconds) // 🔹从字符串解析为整数秒
		ttl = time.Duration(seconds) * time.Second
	}

	if err := c.Set(ctx, key, []byte(value), ttl); err != nil { // 🔹调用客户端 Set，将 value 转换为 []byte
		fmt.Fprintf(os.Stderr, "Set failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK") // 🔹成功写入提示
	if *verbose {
		fmt.Printf("Key: %s\nTTL: %v\n", key, ttl) // 🔹verbose 模式下显示更多信息
	}
}

func handleDelete(ctx context.Context, c *client.Client) {
	if flag.NArg() < 3 { // 🔹delete 期望 group 和 key 两个参数（group 未传到 client）
		fmt.Fprintln(os.Stderr, "Usage: delete <group> <key>")
		os.Exit(1)
	}

	// group := flag.Arg(1)
	key := flag.Arg(2) // 🔹取出 key

	if err := c.Delete(ctx, key); err != nil { // 🔹通过 client.Delete 发起删除请求
		fmt.Fprintf(os.Stderr, "Delete failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("OK") // 🔹删除成功提示
}

func handleStats(ctx context.Context, c *client.Client) {
	// CLI 支持可选 group，但当前 client.Stats 不接受 group
	// group := ""
	// if flag.NArg() >= 2 {
	//     group = flag.Arg(1)
	// }

	stats, err := c.Stats(ctx) // 🔹调用 client.Stats 获取统计结构（通常是 map 或自定义结构）
	if err != nil {
		fmt.Fprintf(os.Stderr, "Stats failed: %v\n", err)
		os.Exit(1)
	}

	// 格式化输出
	data, err := json.MarshalIndent(stats, "", "  ") // 🔹漂亮地缩进 JSON，便于阅读
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to format stats: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(string(data)) // 🔹把格式化后的统计信息打印到 stdout
}

func handleHealth(ctx context.Context, c *client.Client) {
	// pkg/client 提供的是 Ping(ctx) error，返回 nil 表示 healthy
	if err := c.Ping(ctx); err != nil { // 🔹调用 Ping：如果发生错误（连接失败、超时），认为不健康
		fmt.Fprintf(os.Stderr, "Health check failed: %v\n", err)
		fmt.Println("Unhealthy")
		os.Exit(1)
	}
	fmt.Println("Healthy") // 🔹健康检查通过
	os.Exit(0)             // 🔹成功后显式退出（返回码 0）
}

func handleClear(ctx context.Context, c *client.Client) {
	if flag.NArg() < 2 { // 🔹clear 期望至少一个参数 group（虽未发送到服务器）
		fmt.Fprintln(os.Stderr, "Usage: clear <group>")
		os.Exit(1)
	}

	// group := flag.Arg(1) // 当前 client.Clear() 没有 group 参数，调用时会忽略 group
	if err := c.Clear(ctx); err != nil { // 🔹调用 client.Clear 清空缓存（实现可能是清空所有或特定分区，取决于 client 实现）
		fmt.Fprintf(os.Stderr, "Clear failed: %v\n", err)
		os.Exit(1)
	}

	// 使用传入的 group 名作为输出提示（尽管没有发送到服务器）
	fmt.Printf("Group '%s' cleared\n", flag.Arg(1)) // 🔹友好提示，告知用户输入的 group 名
}

func handleBenchmark(ctx context.Context, c *client.Client) {
	count := 1000 // 🔹默认基准操作次数
	if flag.NArg() >= 2 {
		fmt.Sscanf(flag.Arg(1), "%d", &count) // 🔹从命令行读取用户指定的次数（如果提供）
	}

	// 使用单个组名做示例（client 当前不采用 group）
	group := "benchmark"               // 🔹占位 group（保留以便将来支持）
	value := []byte("benchmark_value") // 🔹要写入的示例值（固定）

	fmt.Printf("Running benchmark with %d operations...\n", count)

	// Set benchmark
	start := time.Now()          // 🔹记录开始时间
	for i := 0; i < count; i++ { // 🔹循环执行 count 次 Set 操作
		key := fmt.Sprintf("key_%d", i) // 🔹构造逐序列 key
		_ = c.Set(ctx, key, value, 0)   // 🔹忽略返回错误以简化基准（真实基准通常要记录失败率）
	}
	setDuration := time.Since(start) // 🔹计算 Set 总耗时

	// Get benchmark
	start = time.Now()           // 🔹重新记录开始时间
	for i := 0; i < count; i++ { // 🔹循环执行 count 次 Get 操作
		key := fmt.Sprintf("key_%d", i)
		_, _ = c.Get(ctx, key) // 🔹同样忽略错误，关注吞吐/延迟
	}
	getDuration := time.Since(start) // 🔹计算 Get 总耗时

	fmt.Println("\nResults:")
	fmt.Printf("Set: %d ops in %v (%.2f ops/sec)\n",
		count, setDuration, float64(count)/setDuration.Seconds()) // 🔹输出写入吞吐
	fmt.Printf("Get: %d ops in %v (%.2f ops/sec)\n",
		count, getDuration, float64(count)/getDuration.Seconds()) // 🔹输出读取吞吐

	_ = group // 防止未使用警告（保留以便将来支持 group）
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `TurboCache Client

Usage:
  turbocache-client [options] <command> [arguments]

Commands:
  get <group> <key>              Get value from cache
  set <group> <key> <value> [ttl]  Set value to cache
  delete <group> <key>           Delete key from cache
  stats [group]                  Get statistics
  health                         Health check
  clear <group>                  Clear cache group
  benchmark [count]              Run benchmark

Options:
  -server string    Server address (default "localhost:9090")
  -timeout duration Request timeout (default 5s)
  -v               Verbose output

Examples:
  turbocache-client get user user:123
  turbocache-client set user user:123 "John Doe" 3600
  turbocache-client delete user user:123
  turbocache-client stats
  turbocache-client benchmark 10000

`) // 🔹帮助信息：多行原样字符串，显示命令与示例
}
