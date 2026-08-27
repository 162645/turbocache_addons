package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"turbocache/pkg/client"
)

// BenchmarkConfig 基准测试配置
type BenchmarkConfig struct {
	ServerAddrs    []string
	Concurrency    int
	Duration       time.Duration
	KeySize        int
	ValueSize      int
	ReadRatio      float64
	TotalOps       int
	KeyRange       int
	RequestTimeout time.Duration
	Output         string
	ReadyFile      string
	MissingOnly    bool
}

// BenchmarkResult 基准测试结果
type BenchmarkResult struct {
	TotalOps          int64
	SuccessOps        int64
	FailedOps         int64
	TotalDuration     time.Duration
	AvgLatency        time.Duration
	MinLatency        time.Duration
	MaxLatency        time.Duration
	P50Latency        time.Duration
	P95Latency        time.Duration
	P99Latency        time.Duration
	Throughput        float64
	SuccessThroughput float64
	ErrorRate         float64
	Servers           []string
	Concurrency       int
	ValueSize         int
	ReadRatio         float64
	KeyRange          int
	MissingOnly       bool
	Samples           []TimeSample
}

type TimeSample struct {
	Second       int     `json:"second"`
	Operations   int64   `json:"operations"`
	Successful   int64   `json:"successful"`
	Failed       int64   `json:"failed"`
	SuccessRate  float64 `json:"success_rate"`
	MaxLatencyMS float64 `json:"max_latency_ms"`
}

func main() {
	config := parseFlags()

	fmt.Println("=== TurboCache Benchmark Tool ===")
	fmt.Printf("Servers: %s\n", strings.Join(config.ServerAddrs, ","))
	fmt.Printf("Concurrency: %d\n", config.Concurrency)
	fmt.Printf("Duration: %v\n", config.Duration)
	fmt.Printf("Key Size: %d bytes\n", config.KeySize)
	fmt.Printf("Value Size: %d bytes\n", config.ValueSize)
	fmt.Printf("Read Ratio: %.2f%%\n", config.ReadRatio*100)
	fmt.Printf("Key Range: %d\n", config.KeyRange)
	fmt.Println()

	// 创建客户端（使用 pkg/client 中存在的 API）
	c, err := client.NewClient(config.ServerAddrs, nil)
	if err != nil {
		fmt.Printf("Failed to create client: %v\n", err)
		return
	}
	defer c.Close()

	if !config.MissingOnly {
		fmt.Println("Warming up...")
		warmup(c, config)
		fmt.Println("Warmup completed")
	} else {
		fmt.Println("Missing-key mode: warmup skipped")
	}
	if config.ReadyFile != "" {
		if err := os.WriteFile(config.ReadyFile, []byte("ready\n"), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write ready file: %v\n", err)
			os.Exit(1)
		}
	}
	fmt.Println()

	// 运行基准测试
	fmt.Println("Running benchmark...")
	result := runBenchmark(c, config)

	// 打印结果
	printResults(result)
	if config.Output != "" {
		data, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to encode JSON result: %v\n", err)
			os.Exit(1)
		}
		if err := os.WriteFile(config.Output, data, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to write result: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Result JSON:          %s\n", config.Output)
	}
}

func parseFlags() *BenchmarkConfig {
	config := &BenchmarkConfig{}

	servers := flag.String("servers", "localhost:9090", "Comma-separated server addresses")
	legacyServer := flag.String("server", "", "Deprecated: single server address")
	flag.IntVar(&config.Concurrency, "c", 10, "Number of concurrent connections")
	flag.DurationVar(&config.Duration, "d", 10*time.Second, "Test duration")
	flag.IntVar(&config.KeySize, "ks", 16, "Key size in bytes")
	flag.IntVar(&config.ValueSize, "vs", 1024, "Value size in bytes")
	flag.Float64Var(&config.ReadRatio, "r", 0.8, "Read ratio (0.0-1.0)")
	flag.IntVar(&config.TotalOps, "n", 0, "Total operations (0 for duration-based)")
	flag.IntVar(&config.KeyRange, "kr", 10000, "Key range")
	flag.DurationVar(&config.RequestTimeout, "timeout", 2*time.Second, "Per-operation timeout")
	flag.StringVar(&config.Output, "output", "", "Write machine-readable JSON result")
	flag.StringVar(&config.ReadyFile, "ready-file", "", "Create this file after warmup, before measurement")
	flag.BoolVar(&config.MissingOnly, "missing-only", false, "Issue only GETs for keys that were not warmed; not-found is expected")

	flag.Parse()
	serverList := *servers
	if *legacyServer != "" {
		serverList = *legacyServer
	}
	for _, addr := range strings.Split(serverList, ",") {
		if addr = strings.TrimSpace(addr); addr != "" {
			config.ServerAddrs = append(config.ServerAddrs, addr)
		}
	}
	if len(config.ServerAddrs) == 0 {
		fmt.Fprintln(os.Stderr, "at least one server is required")
		os.Exit(2)
	}
	return config
}

func warmup(c *client.Client, config *BenchmarkConfig) {
	// Warm through the same public path as real traffic. The server-side L2 path
	// places replicas, so preloading every key independently into every ingress
	// would bypass and overstate the distributed design.
	for i := 0; i < config.KeyRange; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), config.RequestTimeout)
		err := c.Set(ctx, generateKey(config.KeySize, i), generateValue(config.ValueSize), 0)
		cancel()
		if err != nil {
			fmt.Fprintf(os.Stderr, "warmup key %d: %v\n", i, err)
			os.Exit(1)
		}
	}
}

func runBenchmark(c *client.Client, config *BenchmarkConfig) *BenchmarkResult {
	var (
		totalOps    int64
		successOps  int64
		failedOps   int64
		minLatency  = time.Duration(1<<63 - 1)
		maxLatency  time.Duration
		latencies   = make([]time.Duration, 0, 100000)
		latencyLock sync.Mutex
		sampleOps   = make([]int64, int(config.Duration/time.Second)+2)
		sampleOK    = make([]int64, len(sampleOps))
		sampleFail  = make([]int64, len(sampleOps))
		sampleMaxNS = make([]int64, len(sampleOps))
	)

	startTime := time.Now()
	deadline := startTime.Add(config.Duration)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 启动工作协程
	for i := 0; i < config.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			for {
				select {
				case <-stop:
					return
				default:
					if config.TotalOps == 0 && time.Now().After(deadline) {
						return
					}

					if config.TotalOps > 0 && atomic.LoadInt64(&totalOps) >= int64(config.TotalOps) {
						return
					}

					// 决定操作类型
					isRead := config.MissingOnly || rng.Float64() < config.ReadRatio
					keyID := rng.Intn(config.KeyRange)
					key := generateKey(config.KeySize, keyID)
					if config.MissingOnly {
						key = "missing::" + key
					}

					opStart := time.Now()
					var err error
					ctx, cancel := context.WithTimeout(context.Background(), config.RequestTimeout)

					if isRead {
						// 使用 Client.Get(ctx, key)
						_, err = c.Get(ctx, key)
					} else {
						value := generateValue(config.ValueSize)
						// 使用 Client.Set(ctx, key, value, ttl)
						err = c.Set(ctx, key, value, 0)
					}
					cancel()
					if config.MissingOnly && errors.Is(err, client.ErrKeyNotFound) {
						err = nil
					}

					latency := time.Since(opStart)
					bucket := int(opStart.Sub(startTime) / time.Second)
					if bucket >= len(sampleOps) {
						bucket = len(sampleOps) - 1
					}
					atomic.AddInt64(&sampleOps[bucket], 1)
					atomicMax(&sampleMaxNS[bucket], latency.Nanoseconds())

					// 更新统计
					atomic.AddInt64(&totalOps, 1)
					if err != nil {
						atomic.AddInt64(&failedOps, 1)
						atomic.AddInt64(&sampleFail[bucket], 1)
					} else {
						atomic.AddInt64(&successOps, 1)
						atomic.AddInt64(&sampleOK[bucket], 1)
					}

					// 记录延迟
					latencyLock.Lock()
					latencies = append(latencies, latency)
					if latency < minLatency {
						minLatency = latency
					}
					if latency > maxLatency {
						maxLatency = latency
					}
					latencyLock.Unlock()
				}
			}
		}(i)
	}

	// 等待完成
	wg.Wait()
	close(stop)

	totalDuration := time.Since(startTime)

	// 计算结果
	result := &BenchmarkResult{
		TotalOps:          atomic.LoadInt64(&totalOps),
		SuccessOps:        atomic.LoadInt64(&successOps),
		FailedOps:         atomic.LoadInt64(&failedOps),
		TotalDuration:     totalDuration,
		MinLatency:        minLatency,
		MaxLatency:        maxLatency,
		Throughput:        float64(atomic.LoadInt64(&totalOps)) / totalDuration.Seconds(),
		SuccessThroughput: float64(atomic.LoadInt64(&successOps)) / totalDuration.Seconds(),
		Servers:           config.ServerAddrs, Concurrency: config.Concurrency, ValueSize: config.ValueSize,
		ReadRatio: config.ReadRatio, KeyRange: config.KeyRange, MissingOnly: config.MissingOnly,
	}
	for i := range sampleOps {
		ops := atomic.LoadInt64(&sampleOps[i])
		if ops == 0 {
			continue
		}
		ok := atomic.LoadInt64(&sampleOK[i])
		result.Samples = append(result.Samples, TimeSample{Second: i, Operations: ops, Successful: ok,
			Failed: atomic.LoadInt64(&sampleFail[i]), SuccessRate: float64(ok) / float64(ops),
			MaxLatencyMS: float64(atomic.LoadInt64(&sampleMaxNS[i])) / float64(time.Millisecond)})
	}

	if result.TotalOps > 0 {
		result.ErrorRate = float64(result.FailedOps) / float64(result.TotalOps)
	}

	// 计算百分位延迟
	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})

		var totalLatency time.Duration
		for _, lat := range latencies {
			totalLatency += lat
		}
		result.AvgLatency = totalLatency / time.Duration(len(latencies))

		result.P50Latency = percentile(latencies, 0.50)
		result.P95Latency = percentile(latencies, 0.95)
		result.P99Latency = percentile(latencies, 0.99)
	} else {
		result.MinLatency = 0
	}

	return result
}

func atomicMax(dst *int64, value int64) {
	for current := atomic.LoadInt64(dst); value > current; current = atomic.LoadInt64(dst) {
		if atomic.CompareAndSwapInt64(dst, current, value) {
			return
		}
	}
}

func percentile(sorted []time.Duration, quantile float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	index := int(float64(len(sorted)-1) * quantile)
	return sorted[index]
}

func printResults(result *BenchmarkResult) {
	fmt.Println("\n=== Benchmark Results ===")
	fmt.Printf("Total Operations:    %d\n", result.TotalOps)
	fmt.Printf("Successful Ops:      %d\n", result.SuccessOps)
	fmt.Printf("Failed Ops:          %d\n", result.FailedOps)
	fmt.Printf("Error Rate:          %.2f%%\n", result.ErrorRate*100)
	fmt.Printf("Total Duration:      %v\n", result.TotalDuration)
	fmt.Printf("Throughput:          %.2f ops/sec\n", result.Throughput)
	fmt.Printf("Successful throughput: %.2f ops/sec\n", result.SuccessThroughput)
	fmt.Println()
	fmt.Println("=== Latency ===")
	fmt.Printf("Average:             %v\n", result.AvgLatency)
	fmt.Printf("Min:                 %v\n", result.MinLatency)
	fmt.Printf("Max:                 %v\n", result.MaxLatency)
	fmt.Printf("P50:                 %v\n", result.P50Latency)
	fmt.Printf("P95:                 %v\n", result.P95Latency)
	fmt.Printf("P99:                 %v\n", result.P99Latency)
	fmt.Println()
}

func generateKey(size, id int) string {
	if size < 16 {
		size = 16
	}
	key := fmt.Sprintf("key_%010d", id)
	if len(key) < size {
		key += strings.Repeat("x", size-len(key))
	}
	return key[:size]
}

func generateValue(size int) []byte {
	value := make([]byte, size)
	for i := range value {
		value[i] = byte('a' + (i % 26))
	}
	return value
}
