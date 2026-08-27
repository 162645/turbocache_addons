package interceptors

import (
	"context"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"turbocache/internal/metrics"
	"turbocache/internal/utils/logger"
)

// LoggingInterceptor 日志拦截器
func LoggingInterceptor(log logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		// 调用处理器
		resp, err := handler(ctx, req)

		duration := time.Since(start)

		// 记录日志
		fields := logger.Fields{
			"method":   info.FullMethod,
			"duration": duration.String(),
		}

		if err != nil {
			st, _ := status.FromError(err)
			fields["error"] = st.Message()
			fields["code"] = st.Code().String()
			log.WithFields(fields).Error("gRPC request failed")
		} else {
			log.WithFields(fields).Info("gRPC request")
		}

		return resp, err
	}
}

// MetricsInterceptor 指标拦截器
func MetricsInterceptor(collector *metrics.Collector) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()

		resp, err := handler(ctx, req)

		// 如果 collector 为 nil，直接返回，不记录指标
		if collector == nil {
			return resp, err
		}

		duration := time.Since(start)

		// 根据方法名记录指标
		switch info.FullMethod {
		case "/cachepb.CacheService/Get":
			collector.RecordGetLatency(duration)
			if err != nil {
				collector.RecordMiss()
			} else {
				collector.RecordHit()
			}
		case "/cachepb.CacheService/Set":
			collector.RecordSetLatency(duration)
			collector.RecordSet()
		case "/cachepb.CacheService/Delete":
			collector.RecordDeleteLatency(duration)
			collector.RecordDelete()
		}

		if err != nil {
			collector.RecordError()
		}

		return resp, err
	}
}

// RecoveryInterceptor 恢复拦截器
func RecoveryInterceptor(log logger.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.WithFields(logger.Fields{
					"method": info.FullMethod,
					"panic":  r,
					"stack":  string(debug.Stack()),
				}).Error("Panic recovered in gRPC handler")

				err = status.Errorf(codes.Internal, "Internal server error: %v", r)
			}
		}()

		return handler(ctx, req)
	}
}

// TimeoutInterceptor 超时拦截器
func TimeoutInterceptor(timeout time.Duration) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		type result struct {
			resp interface{}
			err  error
		}

		done := make(chan result, 1)

		go func() {
			// 在 goroutine 内部捕获 panic
			defer func() {
				if r := recover(); r != nil {
					done <- result{
						resp: nil,
						err:  status.Errorf(codes.Internal, "panic in handler: %v", r),
					}
				}
			}()

			resp, err := handler(ctx, req)
			done <- result{resp: resp, err: err}
		}()

		select {
		case res := <-done:
			return res.resp, res.err
		case <-ctx.Done():
			return nil, status.Errorf(codes.DeadlineExceeded, "Request timeout")
		}
	}
}

// RateLimitInterceptor 限流拦截器
func RateLimitInterceptor(requestsPerSecond int) grpc.UnaryServerInterceptor {
	limiter := make(chan struct{}, requestsPerSecond)

	// 填充令牌
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(requestsPerSecond))
		defer ticker.Stop()

		for range ticker.C {
			select {
			case limiter <- struct{}{}:
			default:
			}
		}
	}()

	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		select {
		case <-limiter:
			return handler(ctx, req)
		default:
			return nil, status.Errorf(codes.ResourceExhausted, "Rate limit exceeded")
		}
	}
}

// ValidationInterceptor 验证拦截器
func ValidationInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		// 可以在这里添加请求验证逻辑
		// 例如检查必填字段、格式验证等

		return handler(ctx, req)
	}
}

// ChainUnaryServer 链接多个拦截器
func ChainUnaryServer(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		chained := handler
		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			next := chained
			chained = func(currentCtx context.Context, currentReq interface{}) (interface{}, error) {
				return interceptor(currentCtx, currentReq, info, next)
			}
		}
		return chained(ctx, req)
	}
}
