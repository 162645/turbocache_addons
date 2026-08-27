package protection_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"turbocache/internal/protection/circuit"
)

// ===== 基础功能测试 =====

// TestNewCircuitBreaker 测试创建熔断器
func TestNewCircuitBreaker(t *testing.T) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)

	if cb == nil {
		t.Fatal("期望非空熔断器")
	}

	if cb.GetState() != circuit.StateClosed {
		t.Errorf("初始状态应该是Closed, 获得 %v", cb.GetState())
	}
}

// TestCircuitBreaker_SuccessExecution 测试成功执行
func TestCircuitBreaker_SuccessExecution(t *testing.T) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)

	ctx := context.Background()
	executed := false

	operation := func() error {
		executed = true
		return nil
	}

	err := cb.Execute(ctx, operation)
	if err != nil {
		t.Errorf("期望成功执行, 获得错误: %v", err)
	}
	if !executed {
		t.Error("操作应该被执行")
	}

	stats := cb.GetStats()
	if stats.TotalSuccesses != 1 {
		t.Errorf("成功次数应该是1, 获得 %d", stats.TotalSuccesses)
	}
}

// TestCircuitBreaker_FailureExecution 测试失败执行
func TestCircuitBreaker_FailureExecution(t *testing.T) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)

	ctx := context.Background()
	testErr := errors.New("操作失败")

	operation := func() error {
		return testErr
	}

	err := cb.Execute(ctx, operation)
	if err != testErr {
		t.Errorf("期望返回操作错误, 获得 %v", err)
	}

	stats := cb.GetStats()
	if stats.TotalFailures != 1 {
		t.Errorf("失败次数应该是1, 获得 %d", stats.TotalFailures)
	}
}

// ===== 状态转换测试 =====

// TestCircuitBreaker_OpenState 测试打开状态
func TestCircuitBreaker_OpenState(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        3,
		Timeout:          time.Second,
		MaxRequests:      5,
		FailureRatio:     0.5,
		MinRequests:      10,
		SuccessThreshold: 2,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	failOp := func() error {
		return errors.New("失败")
	}

	// 触发失败直到熔断器打开
	for i := 0; i < int(config.Threshold); i++ {
		cb.Execute(ctx, failOp)
	}

	// 现在熔断器应该是打开状态
	if cb.GetState() != circuit.StateOpen {
		t.Errorf("期望状态为Open, 获得 %v", cb.GetState())
	}

	// 尝试执行应该立即失败
	err := cb.Execute(ctx, failOp)
	if err != circuit.ErrCircuitOpen {
		t.Errorf("期望ErrCircuitOpen, 获得 %v", err)
	}
}

// TestCircuitBreaker_HalfOpenState 测试半开状态
func TestCircuitBreaker_HalfOpenState(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        3,
		Timeout:          100 * time.Millisecond,
		MaxRequests:      5,
		FailureRatio:     0.5,
		MinRequests:      10,
		SuccessThreshold: 2,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	failOp := func() error {
		return errors.New("失败")
	}

	// 打开熔断器
	for i := 0; i < int(config.Threshold); i++ {
		cb.Execute(ctx, failOp)
	}

	if cb.GetState() != circuit.StateOpen {
		t.Fatal("熔断器应该处于Open状态")
	}

	// 等待超时
	time.Sleep(150 * time.Millisecond)

	successOp := func() error {
		return nil
	}

	// 下一次执行应该转换到半开状态
	err := cb.Execute(ctx, successOp)
	if err != nil {
		t.Errorf("半开状态下执行失败: %v", err)
	}

	if cb.GetState() != circuit.StateHalfOpen && cb.GetState() != circuit.StateClosed {
		t.Logf("状态: %v", cb.GetState())
	}
}

// TestCircuitBreaker_StateTransitionToClosed 测试转换回关闭状态
func TestCircuitBreaker_StateTransitionToClosed(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        3,
		Timeout:          100 * time.Millisecond,
		MaxRequests:      5,
		FailureRatio:     0.5,
		MinRequests:      10,
		SuccessThreshold: 2,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	// 打开熔断器
	for i := 0; i < int(config.Threshold); i++ {
		cb.Execute(ctx, func() error { return errors.New("失败") })
	}

	// 等待超时进入半开状态
	time.Sleep(150 * time.Millisecond)

	// 执行足够的成功请求以关闭熔断器
	successOp := func() error {
		return nil
	}

	for i := 0; i < int(config.SuccessThreshold); i++ {
		err := cb.Execute(ctx, successOp)
		if err != nil {
			t.Logf("第%d次成功执行出错: %v", i+1, err)
		}
	}

	// 熔断器应该关闭
	if cb.GetState() != circuit.StateClosed {
		t.Errorf("期望状态为Closed, 获得 %v", cb.GetState())
	}
}

// ===== 失败率测试 =====

// TestCircuitBreaker_FailureRatio 测试失败率触发
func TestCircuitBreaker_FailureRatio(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        100, // 设置很高，不通过连续失败触发
		Timeout:          time.Second,
		MaxRequests:      5,
		FailureRatio:     0.5,
		MinRequests:      20,
		SuccessThreshold: 2,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	// 执行20个请求，其中11个失败（失败率55%）
	for i := 0; i < 20; i++ {
		if i < 11 {
			cb.Execute(ctx, func() error { return errors.New("失败") })
		} else {
			cb.Execute(ctx, func() error { return nil })
		}
	}

	// 熔断器应该打开（失败率超过50%）
	if cb.GetState() != circuit.StateOpen {
		stats := cb.GetStats()
		t.Errorf("期望状态为Open, 获得 %v (失败率: %.2f)", cb.GetState(), stats.FailureRate)
	}
}

// ===== 半开状态限流测试 =====

// TestCircuitBreaker_HalfOpenMaxRequests 测试半开状态请求限制
func TestCircuitBreaker_HalfOpenMaxRequests(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        3,
		Timeout:          50 * time.Millisecond,
		MaxRequests:      2,
		FailureRatio:     0.5,
		MinRequests:      10,
		SuccessThreshold: 2,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	// 打开熔断器
	for i := 0; i < int(config.Threshold); i++ {
		cb.Execute(ctx, func() error { return errors.New("失败") })
	}

	// 等待超时
	time.Sleep(100 * time.Millisecond)

	// 在半开状态下，只允许MaxRequests个并发请求
	successOp := func() error {
		time.Sleep(50 * time.Millisecond)
		return nil
	}

	var wg sync.WaitGroup
	errors := make([]error, 5)

	// 启动5个并发请求
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errors[idx] = cb.Execute(ctx, successOp)
		}(i)
	}

	wg.Wait()

	// 应该有一些请求被拒绝
	tooManyRequests := 0
	for _, err := range errors {
		if err == circuit.ErrTooManyRequests {
			tooManyRequests++
		}
	}

	if tooManyRequests == 0 {
		t.Error("期望有请求被限流")
	}

	t.Logf("半开状态限流: %d/%d 请求被拒绝", tooManyRequests, len(errors))
}

// ===== 统计信息测试 =====

// TestCircuitBreaker_Stats 测试统计信息
func TestCircuitBreaker_Stats(t *testing.T) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	// 执行一些成功和失败的操作
	for i := 0; i < 10; i++ {
		if i < 6 {
			cb.Execute(ctx, func() error { return nil })
		} else {
			cb.Execute(ctx, func() error { return errors.New("失败") })
		}
	}

	stats := cb.GetStats()

	if stats.TotalRequests != 10 {
		t.Errorf("总请求数应该是10, 获得 %d", stats.TotalRequests)
	}
	if stats.TotalSuccesses != 6 {
		t.Errorf("成功次数应该是6, 获得 %d", stats.TotalSuccesses)
	}
	if stats.TotalFailures != 4 {
		t.Errorf("失败次数应该是4, 获得 %d", stats.TotalFailures)
	}
	if stats.ConsecutiveFailures != 4 {
		t.Errorf("连续失败次数应该是4, 获得 %d", stats.ConsecutiveFailures)
	}

	expectedFailureRate := 0.4
	if stats.FailureRate < expectedFailureRate-0.01 || stats.FailureRate > expectedFailureRate+0.01 {
		t.Errorf("失败率应该约为%.2f, 获得 %.2f", expectedFailureRate, stats.FailureRate)
	}
}

// ===== Reset测试 =====

// TestCircuitBreaker_Reset 测试重置功能
func TestCircuitBreaker_Reset(t *testing.T) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	// 触发一些失败
	for i := 0; i < 5; i++ {
		cb.Execute(ctx, func() error { return errors.New("失败") })
	}

	stats := cb.GetStats()
	if stats.TotalFailures == 0 {
		t.Fatal("应该有失败记录")
	}

	// 重置
	cb.Reset()

	// 验证已重置
	stats = cb.GetStats()
	if stats.TotalRequests != 0 {
		t.Errorf("重置后总请求数应该是0, 获得 %d", stats.TotalRequests)
	}
	if stats.TotalFailures != 0 {
		t.Errorf("重置后失败次数应该是0, 获得 %d", stats.TotalFailures)
	}
	if cb.GetState() != circuit.StateClosed {
		t.Errorf("重置后状态应该是Closed, 获得 %v", cb.GetState())
	}
}

// ===== 并发安全测试 =====

// TestCircuitBreaker_ConcurrentExecution 测试并发执行
func TestCircuitBreaker_ConcurrentExecution(t *testing.T) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	const goroutines = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)

	successCount := int32(0)
	failureCount := int32(0)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			op := func() error {
				if id%2 == 0 {
					return nil
				}
				return errors.New("失败")
			}

			err := cb.Execute(ctx, op)
			if err == nil {
				atomic.AddInt32(&successCount, 1)
			} else {
				atomic.AddInt32(&failureCount, 1)
			}
		}(i)
	}

	wg.Wait()

	stats := cb.GetStats()
	t.Logf("并发测试: 成功=%d, 失败=%d, 总计=%d",
		successCount, failureCount, stats.TotalRequests)

	if stats.TotalRequests != goroutines {
		t.Errorf("总请求数应该是%d, 获得 %d", goroutines, stats.TotalRequests)
	}
}

// TestCircuitBreaker_RaceCondition 测试竞态条件
func TestCircuitBreaker_RaceCondition(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        10,
		Timeout:          50 * time.Millisecond,
		MaxRequests:      5,
		FailureRatio:     0.5,
		MinRequests:      20,
		SuccessThreshold: 5,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			// 混合操作：读取状态、执行、重置
			for j := 0; j < 10; j++ {
				_ = cb.GetState()
				_ = cb.GetStats()

				op := func() error {
					if (id+j)%3 == 0 {
						return errors.New("失败")
					}
					return nil
				}

				cb.Execute(ctx, op)

				if j == 5 && id == 0 {
					cb.Reset()
				}
			}
		}(i)
	}

	wg.Wait()

	// 应该没有panic或死锁
	t.Log("竞态条件测试通过")
}

// ===== CircuitBreakerManager测试 =====

// TestCircuitBreakerManager_GetOrCreate 测试获取或创建
func TestCircuitBreakerManager_GetOrCreate(t *testing.T) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)

	// 第一次获取，应该创建
	cb1 := manager.GetOrCreate("service1")
	if cb1 == nil {
		t.Fatal("期望非空熔断器")
	}

	// 第二次获取，应该返回同一个实例
	cb2 := manager.GetOrCreate("service1")
	if cb1 != cb2 {
		t.Error("应该返回同一个熔断器实例")
	}

	// 不同名称应该返回不同实例
	cb3 := manager.GetOrCreate("service2")
	if cb1 == cb3 {
		t.Error("不同名称应该返回不同的熔断器实例")
	}
}

// TestCircuitBreakerManager_Get 测试获取
func TestCircuitBreakerManager_Get(t *testing.T) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)

	// 获取不存在的
	_, exists := manager.Get("nonexistent")
	if exists {
		t.Error("不存在的熔断器不应该返回")
	}

	// 创建后再获取
	manager.GetOrCreate("service1")
	cb, exists := manager.Get("service1")
	if !exists {
		t.Error("已创建的熔断器应该存在")
	}
	if cb == nil {
		t.Error("期望非空熔断器")
	}
}

// TestCircuitBreakerManager_Remove 测试移除
func TestCircuitBreakerManager_Remove(t *testing.T) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)

	manager.GetOrCreate("service1")

	// 验证存在
	_, exists := manager.Get("service1")
	if !exists {
		t.Fatal("熔断器应该存在")
	}

	// 移除
	manager.Remove("service1")

	// 验证已移除
	_, exists = manager.Get("service1")
	if exists {
		t.Error("移除后不应该存在")
	}
}

// TestCircuitBreakerManager_GetAll 测试获取所有
func TestCircuitBreakerManager_GetAll(t *testing.T) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)

	// 创建多个熔断器
	services := []string{"service1", "service2", "service3"}
	for _, svc := range services {
		manager.GetOrCreate(svc)
	}

	// 获取所有
	all := manager.GetAll()
	if len(all) != len(services) {
		t.Errorf("期望 %d 个熔断器, 获得 %d", len(services), len(all))
	}

	for _, svc := range services {
		if _, exists := all[svc]; !exists {
			t.Errorf("熔断器 %s 应该存在", svc)
		}
	}
}

// TestCircuitBreakerManager_ResetAll 测试重置所有
func TestCircuitBreakerManager_ResetAll(t *testing.T) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)
	ctx := context.Background()

	// 创建并触发失败
	services := []string{"service1", "service2", "service3"}
	for _, svc := range services {
		cb := manager.GetOrCreate(svc)
		for i := 0; i < 5; i++ {
			cb.Execute(ctx, func() error { return errors.New("失败") })
		}
	}

	// 验证有失败记录
	for _, svc := range services {
		cb, _ := manager.Get(svc)
		stats := cb.GetStats()
		if stats.TotalFailures == 0 {
			t.Errorf("%s 应该有失败记录", svc)
		}
	}

	// 重置所有
	manager.ResetAll()

	// 验证已重置
	for _, svc := range services {
		cb, _ := manager.Get(svc)
		stats := cb.GetStats()
		if stats.TotalFailures != 0 {
			t.Errorf("%s 重置后失败次数应该是0, 获得 %d", svc, stats.TotalFailures)
		}
	}
}

// TestCircuitBreakerManager_Concurrent 测试管理器并发安全
func TestCircuitBreakerManager_Concurrent(t *testing.T) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()

			// 并发创建和访问
			svc := fmt.Sprintf("service%d", id%10)
			cb := manager.GetOrCreate(svc)
			_ = cb.GetState()

			if id%20 == 0 {
				manager.Remove(svc)
			}
		}(i)
	}

	wg.Wait()

	t.Log("管理器并发测试通过")
}

// ===== 禁用熔断器测试 =====

// TestCircuitBreaker_Disabled 测试禁用状态
func TestCircuitBreaker_Disabled(t *testing.T) {
	config := &circuit.Config{
		Enabled: false,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	executed := int32(0)
	op := func() error {
		atomic.AddInt32(&executed, 1)
		return errors.New("失败")
	}

	// 即使失败也应该继续执行
	for i := 0; i < 20; i++ {
		err := cb.Execute(ctx, op)
		if err == nil {
			t.Error("应该返回操作错误")
		}
	}

	if atomic.LoadInt32(&executed) != 20 {
		t.Errorf("禁用时所有操作都应该执行, 获得 %d/20", executed)
	}

	// 状态应该保持关闭
	if cb.GetState() != circuit.StateClosed {
		t.Errorf("禁用时状态应该是Closed, 获得 %v", cb.GetState())
	}
}

// ===== 边界条件测试 =====

// TestCircuitBreaker_ZeroThreshold 测试零阈值
func TestCircuitBreaker_ZeroThreshold(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        0,
		Timeout:          time.Second,
		MaxRequests:      5,
		FailureRatio:     1.0,
		MinRequests:      1,
		SuccessThreshold: 1,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	// 一次失败不应该打开熔断器（阈值为0）
	cb.Execute(ctx, func() error { return errors.New("失败") })

	if cb.GetState() == circuit.StateOpen {
		t.Error("阈值为0时不应该打开熔断器")
	}
}

// TestCircuitBreaker_NilConfig 测试nil配置
func TestCircuitBreaker_NilConfig(t *testing.T) {
	cb := circuit.NewCircuitBreaker("test", nil)

	if cb == nil {
		t.Fatal("nil配置应该使用默认值")
	}

	// 应该能正常工作
	ctx := context.Background()
	err := cb.Execute(ctx, func() error { return nil })
	if err != nil {
		t.Errorf("应该能正常执行, 获得错误: %v", err)
	}
}

// TestCircuitBreaker_QuickTransition 测试快速状态转换
func TestCircuitBreaker_QuickTransition(t *testing.T) {
	config := &circuit.Config{
		Enabled:          true,
		Threshold:        2,
		Timeout:          10 * time.Millisecond,
		MaxRequests:      10,
		FailureRatio:     0.5,
		MinRequests:      5,
		SuccessThreshold: 1,
	}

	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	// 打开熔断器
	for i := 0; i < 2; i++ {
		cb.Execute(ctx, func() error { return errors.New("失败") })
	}

	if cb.GetState() != circuit.StateOpen {
		t.Fatal("熔断器应该打开")
	}

	// 快速转换到半开
	time.Sleep(20 * time.Millisecond)

	// 成功请求应该关闭熔断器
	err := cb.Execute(ctx, func() error { return nil })
	if err != nil {
		t.Errorf("半开状态执行失败: %v", err)
	}

	// 应该转回关闭状态
	if cb.GetState() != circuit.StateClosed {
		t.Logf("当前状态: %v", cb.GetState())
	}
}

// ===== 性能基准测试 =====

// BenchmarkCircuitBreaker_Execute 基准测试执行
func BenchmarkCircuitBreaker_Execute(b *testing.B) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	op := func() error {
		return nil
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Execute(ctx, op)
	}
}

// BenchmarkCircuitBreaker_Concurrent 基准测试并发执行
func BenchmarkCircuitBreaker_Concurrent(b *testing.B) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)
	ctx := context.Background()

	op := func() error {
		return nil
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cb.Execute(ctx, op)
		}
	})
}

// BenchmarkCircuitBreaker_GetState 基准测试状态获取
func BenchmarkCircuitBreaker_GetState(b *testing.B) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.GetState()
	}
}

// BenchmarkCircuitBreaker_GetStats 基准测试统计信息获取
func BenchmarkCircuitBreaker_GetStats(b *testing.B) {
	config := circuit.DefaultConfig()
	cb := circuit.NewCircuitBreaker("test", config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.GetStats()
	}
}

// BenchmarkCircuitBreakerManager_GetOrCreate 基准测试管理器
func BenchmarkCircuitBreakerManager_GetOrCreate(b *testing.B) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		manager.GetOrCreate(fmt.Sprintf("service%d", i%100))
	}
}

// BenchmarkCircuitBreakerManager_Concurrent 基准测试管理器并发
func BenchmarkCircuitBreakerManager_Concurrent(b *testing.B) {
	config := circuit.DefaultConfig()
	manager := circuit.NewCircuitBreakerManager(config)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			manager.GetOrCreate(fmt.Sprintf("service%d", i%100))
			i++
		}
	})
}
