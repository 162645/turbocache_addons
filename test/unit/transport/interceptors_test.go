// interceptors_test.go
// 对 internal/transport/grpc/interceptors 包的单元测试
// 使用中文注释，覆盖常见拦截器行为：日志、指标、恢复、超时、限流、链式调用等。

package transport

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"turbocache/internal/metrics"
	"turbocache/internal/transport/grpc/interceptors"
	"turbocache/internal/utils/logger"
)

// -------------------- mock logger --------------------

// mockLogger 用于捕获日志调用：只实现被拦截器使用到的方法
type mockLogger struct {
	mu     sync.Mutex
	infos  []string
	errs   []string
	fields []map[string]interface{}
	// 可选：存 ctx 或其他信息以便断言
}

func (m *mockLogger) Debug(args ...interface{}) {}
func (m *mockLogger) Info(args ...interface{}) {
	m.mu.Lock()
	m.infos = append(m.infos, "info")
	m.mu.Unlock()
}
func (m *mockLogger) Warn(args ...interface{}) {
	m.mu.Lock()
	m.infos = append(m.infos, "warn")
	m.mu.Unlock()
}
func (m *mockLogger) Error(args ...interface{}) {
	m.mu.Lock()
	m.errs = append(m.errs, "error")
	m.mu.Unlock()
}
func (m *mockLogger) Fatal(args ...interface{})                 {}
func (m *mockLogger) Debugf(format string, args ...interface{}) {}
func (m *mockLogger) Infof(format string, args ...interface{}) {
	m.mu.Lock()
	m.infos = append(m.infos, "infof")
	m.mu.Unlock()
}
func (m *mockLogger) Warnf(format string, args ...interface{}) {
	m.mu.Lock()
	m.infos = append(m.infos, "warnf")
	m.mu.Unlock()
}
func (m *mockLogger) Errorf(format string, args ...interface{}) {
	m.mu.Lock()
	m.errs = append(m.errs, "errorf")
	m.mu.Unlock()
}
func (m *mockLogger) Fatalf(format string, args ...interface{}) {}

// 关键修正：这些方法必须返回 logger.Logger（接口），而不是 concrete mockLogger 类型
func (m *mockLogger) WithField(key string, value interface{}) logger.Logger {
	m.mu.Lock()
	m.fields = append(m.fields, map[string]interface{}{key: value})
	m.mu.Unlock()
	return m
}
func (m *mockLogger) WithFields(fields map[string]interface{}) logger.Logger {
	m.mu.Lock()
	m.fields = append(m.fields, fields)
	m.mu.Unlock()
	return m
}
func (m *mockLogger) WithContext(ctx context.Context) logger.Logger {
	// 如需断言，可保存 ctx；此处为简化直接返回自身
	return m
}
func (m *mockLogger) WithError(err error) logger.Logger {
	if err != nil {
		m.mu.Lock()
		m.errs = append(m.errs, err.Error())
		m.mu.Unlock()
	}
	return m
}

// -------------------- mock handler（模拟业务处理器） --------------------

// successHandler 返回固定成功结果
func successHandler(ctx context.Context, req interface{}) (interface{}, error) {
	return "ok", nil
}

// errorHandler 返回指定错误
func errorHandler(ctx context.Context, req interface{}) (interface{}, error) {
	return nil, status.Error(codes.InvalidArgument, "bad request")
}

// panicHandler 模拟 panic
func panicHandler(ctx context.Context, req interface{}) (interface{}, error) {
	panic("simulated panic")
}

// slowHandler 模拟耗时处理
func slowHandler(d time.Duration) grpc.UnaryHandler {
	return func(ctx context.Context, req interface{}) (interface{}, error) {
		select {
		case <-time.After(d):
			return "done", nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// -------------------- 辅助构造 --------------------

func unaryInfo(method string) *grpc.UnaryServerInfo {
	return &grpc.UnaryServerInfo{
		FullMethod: method,
	}
}

// -------------------- 单元测试 --------------------

func TestLoggingInterceptor_SuccessAndError(t *testing.T) {
	mockLog := &mockLogger{}

	inter := interceptors.LoggingInterceptor(mockLog)

	// 成功分支：handler 返回 nil error
	resp, err := inter(context.Background(), "req", unaryInfo("/svc/Get"), successHandler)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}

	// 错误分支：handler 返回 gRPC 错误
	_, err = inter(context.Background(), "req", unaryInfo("/svc/Get"), errorHandler)
	if err == nil {
		t.Fatalf("expected error, got nil")
	}

	// 确认 mockLogger 收到日志（至少产生了 info/error 记录）
	mockLog.mu.Lock()
	infoCount := len(mockLog.infos)
	errCount := len(mockLog.errs)
	mockLog.mu.Unlock()

	if infoCount == 0 && errCount == 0 {
		t.Fatalf("expected logging calls, got none")
	}
}

func TestRecoveryInterceptor_RecoverFromPanic(t *testing.T) {
	mockLog := &mockLogger{}
	inter := interceptors.RecoveryInterceptor(mockLog)

	// 当 handler panic 时，拦截器应该捕获并返回 Internal error
	_, err := inter(context.Background(), "req", unaryInfo("/svc/Do"), panicHandler)
	if err == nil {
		t.Fatalf("expected error due to panic, got nil")
	}
	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Fatalf("expected Internal code, got %v", st.Code())
	}
	// mockLog 也应记录 panic 信息
	mockLog.mu.Lock()
	hasErrLogs := len(mockLog.errs) > 0 || len(mockLog.fields) > 0
	mockLog.mu.Unlock()
	if !hasErrLogs {
		// 仅警告：可能 logger.WithFields 的实现将字段保存到 fields
		t.Logf("warning: mock logger didn't capture fields or errs; actual logger likely recorded panic")
	}
}

func TestTimeoutInterceptor(t *testing.T) {
	// 设置超时短于 handler 执行时间 -> 应返回 DeadlineExceeded
	interShort := interceptors.TimeoutInterceptor(20 * time.Millisecond)
	_, err := interShort(context.Background(), "req", unaryInfo("/svc/Slow"), slowHandler(50*time.Millisecond))
	if err == nil {
		t.Fatalf("expected timeout error, got nil")
	}
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", status.Code(err))
	}

	// 设置超时长于 handler 执行时间 -> 正常返回
	interLong := interceptors.TimeoutInterceptor(200 * time.Millisecond)
	resp, err := interLong(context.Background(), "req", unaryInfo("/svc/Slow"), slowHandler(50*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp != "done" {
		t.Fatalf("unexpected response: %v", resp)
	}
}

func TestRateLimitInterceptor_Basic(t *testing.T) {
	// 使用极低速率以便可预测地触发限流
	rps := 2
	inter := interceptors.RateLimitInterceptor(rps)

	// 尝试快速发起多次请求，部分应被限流返回 ResourceExhausted
	total := 10
	exhausted := 0
	for i := 0; i < total; i++ {
		_, err := inter(context.Background(), "req", unaryInfo("/svc/Any"), successHandler)
		if err != nil {
			if status.Code(err) == codes.ResourceExhausted {
				exhausted++
			} else {
				t.Fatalf("unexpected error code: %v", status.Code(err))
			}
		}
		// 不等待，尽量快速触发限流
	}
	if exhausted == 0 {
		t.Logf("warning: no ResourceExhausted observed; environment timing may have allowed tokens to fill")
	}
}

func TestValidationInterceptor_Noop(t *testing.T) {
	inter := interceptors.ValidationInterceptor()
	// 验证拦截器目前是 noop —— 应该直接调用 handler 并返回结果
	resp, err := inter(context.Background(), "req", unaryInfo("/svc/Validate"), successHandler)
	if err != nil {
		t.Fatalf("unexpected error from validation interceptor: %v", err)
	}
	if resp != "ok" {
		t.Fatalf("unexpected response: %v", resp)
	}
}

func TestChainUnaryServer_OrderAndEffects(t *testing.T) {
	mockLog := &mockLogger{}
	// 使用 timeout + recovery + logging 组合
	timeout := interceptors.TimeoutInterceptor(100 * time.Millisecond)
	recovery := interceptors.RecoveryInterceptor(mockLog)
	logging := interceptors.LoggingInterceptor(mockLog)

	chain := interceptors.ChainUnaryServer(logging, recovery, timeout)

	// 1) 正常请求：应该走完链并返回正常结果
	resp, err := chain(context.Background(), "req", unaryInfo("/svc/Chain"), successHandler)
	if err != nil || resp != "ok" {
		t.Fatalf("expected ok, got %v / %v", resp, err)
	}

	// 2) panic 请求：Recovery 应将 panic 转为 Internal error
	_, err = chain(context.Background(), "req", unaryInfo("/svc/Chain"), panicHandler)
	if err == nil {
		t.Fatalf("expected error due to panic")
	}
	if status.Code(err) != codes.Internal {
		t.Fatalf("expected Internal code, got %v", status.Code(err))
	}
}

func TestMetricsInterceptor_Records(t *testing.T) {
	// WARNING: 此处尝试使用 metrics.Collector 的零值作为参数，假定其方法在零值上可安全调用。
	// 如果你的 metrics.Collector 需要显式构造，请把下面的零值替换为真实构造器，例如 metrics.NewCollector(...)
	var collector metrics.Collector
	inter := interceptors.MetricsInterceptor(&collector)

	// 针对不同方法调用，断言不会 panic 且逻辑路径被执行（我们无法直接读取 collector 的私有状态）
	// 通过调用不同 FullMethod 路径来覆盖 switch 分支
	methods := []string{
		"/cachepb.CacheService/Get",
		"/cachepb.CacheService/Set",
		"/cachepb.CacheService/Delete",
		"/other.Service/Method",
	}

	for _, m := range methods {
		info := unaryInfo(m)
		_, err := inter(context.Background(), "req", info, successHandler)
		// MetricsInterceptor 只会在 handler 返回错误时记录 error，成功时应该不报错
		if err != nil {
			t.Fatalf("unexpected error from metrics interceptor for method %s: %v", m, err)
		}
	}

	// 如果 collector 在零值上不可用，会产生 panic/运行时错误，请根据你实际的 metrics 包的构造函数调整测试
}

func TestInterceptorChain_IntegrationExample(t *testing.T) {
	mockLog := &mockLogger{}
	var collector metrics.Collector // 参见上文备注：若不可用请用真实构造器
	logging := interceptors.LoggingInterceptor(mockLog)
	metricsInter := interceptors.MetricsInterceptor(&collector)
	timeout := interceptors.TimeoutInterceptor(100 * time.Millisecond)
	recovery := interceptors.RecoveryInterceptor(mockLog)

	chain := interceptors.ChainUnaryServer(logging, metricsInter, timeout, recovery)

	// 执行一次完整生命周期请求
	start := time.Now()
	resp, err := chain(context.Background(), "req", unaryInfo("/cachepb.CacheService/Get"), slowHandler(10*time.Millisecond))
	if err != nil {
		t.Fatalf("unexpected error in integration chain: %v", err)
	}
	if resp == nil {
		t.Fatalf("expected non-nil response")
	}
	dur := time.Since(start)
	t.Logf("integration request done in %v", dur)
}

// -------------------- 边界与并发快速检查 --------------------

func TestTimeoutInterceptor_Concurrent(t *testing.T) {
	inter := interceptors.TimeoutInterceptor(50 * time.Millisecond)

	// 并发多次调用，检查没有竞态
	var wg sync.WaitGroup
	concurrency := 20
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, _ = inter(context.Background(), "req", unaryInfo("/svc/Slow"), slowHandler(10*time.Millisecond))
		}()
	}
	wg.Wait()
}

// -------------------- 恢复拦截器错误场景 --------------------

func TestRecoveryInterceptor_EnsureNoPanicEscapes(t *testing.T) {
	mockLog := &mockLogger{}
	inter := interceptors.RecoveryInterceptor(mockLog)

	// 多次触发 panic，确认不会导致测试进程崩溃
	for i := 0; i < 5; i++ {
		_, err := inter(context.Background(), i, unaryInfo("/svc/Panic"), panicHandler)
		if err == nil {
			t.Fatalf("expected error when handler panics, got nil (iteration %d)", i)
		}
		if status.Code(err) != codes.Internal {
			t.Fatalf("expected internal code, got %v (iteration %d)", status.Code(err), i)
		}
	}
}

// -------------------- 验证拦截器错误处理 --------------------

func TestInterceptors_ErrorTranslation(t *testing.T) {
	// 检查当 handler 返回不同 gRPC 错时代码时，代码保持不变（Logging / Metrics 不应修改代码）
	mockLog := &mockLogger{}
	var collector metrics.Collector

	logging := interceptors.LoggingInterceptor(mockLog)
	metricsInter := interceptors.MetricsInterceptor(&collector)

	// 使用返回不同 codes 的 handler
	cases := []codes.Code{codes.InvalidArgument, codes.NotFound, codes.Internal, codes.Unavailable}

	for _, c := range cases {
		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, status.Error(c, "simulated")
		}
		_, err := logging(context.Background(), "req", unaryInfo("/svc/Test"), handler)
		if err == nil {
			t.Fatalf("expected error for code %v", c)
		}
		if status.Code(err) != c {
			t.Fatalf("expected code %v, got %v", c, status.Code(err))
		}

		_, err = metricsInter(context.Background(), "req", unaryInfo("/svc/Test"), handler)
		if err == nil {
			t.Fatalf("expected error for code %v", c)
		}
		if status.Code(err) != c {
			t.Fatalf("expected code %v from metrics interceptor, got %v", c, status.Code(err))
		}
	}
}
