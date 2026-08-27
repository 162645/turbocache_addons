package middleware

import (
	"context"
	"net/http"
	"sync"
	"time"

	"turbocache/internal/metrics"
	"turbocache/internal/utils/logger"
)

// Logger 日志中间件
func Logger(log logger.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// 包装 ResponseWriter 以捕获状态码
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)
			log.WithFields(map[string]interface{}{
				"method":      r.Method,
				"path":        r.URL.Path,
				"status":      wrapped.statusCode,
				"duration":    duration.String(),
				"remote_addr": r.RemoteAddr,
				"user_agent":  r.UserAgent(),
			}).Info("HTTP Request")
		})
	}
}

// Metrics 指标中间件
func Metrics(collector *metrics.Collector) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			wrapped := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

			next.ServeHTTP(wrapped, r)

			duration := time.Since(start)

			// 根据路径记录不同的指标
			if r.URL.Path == "/cache/" {
				switch r.Method {
				case http.MethodGet:
					collector.RecordGetLatency(duration)
				case http.MethodPost, http.MethodPut:
					collector.RecordSetLatency(duration)
				case http.MethodDelete:
					collector.RecordDeleteLatency(duration)
				}
			}
		})
	}
}

// Recovery 恢复中间件
func Recovery(log logger.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					log.WithFields(map[string]interface{}{
						"error":  err,
						"method": r.Method,
						"path":   r.URL.Path,
					}).Error("Panic recovered")

					w.WriteHeader(http.StatusInternalServerError)
					w.Write([]byte("Internal Server Error"))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

// RateLimit 限流中间件（简化版）
func RateLimit(requestsPerSecond int) func(http.Handler) http.Handler {
	limiter := &rateLimiter{
		rate:   requestsPerSecond,
		tokens: requestsPerSecond,
		mu:     sync.Mutex{},
	}

	// 启动令牌补充协程
	go limiter.refill()

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !limiter.allow() {
				w.WriteHeader(http.StatusTooManyRequests)
				w.Write([]byte("Rate limit exceeded"))
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// Timeout 超时中间件
func Timeout(timeout time.Duration) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			r = r.WithContext(ctx)

			done := make(chan struct{})
			go func() {
				next.ServeHTTP(w, r)
				close(done)
			}()

			select {
			case <-done:
				// 请求完成
			case <-ctx.Done():
				// 超时
				w.WriteHeader(http.StatusGatewayTimeout)
				w.Write([]byte("Request timeout"))
			}
		})
	}
}

// CORS CORS 中间件
func CORS(allowOrigins []string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			if origin == "" {
				origin = "*"
			}

			// 设置 CORS 头
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-TTL")
			w.Header().Set("Access-Control-Max-Age", "86400")

			// 处理 OPTIONS 请求
			if r.Method == http.MethodOptions {
				w.WriteHeader(http.StatusOK)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// Chain 中间件链
func Chain(middlewares ...func(http.Handler) http.Handler) func(http.Handler) http.Handler {
	return func(final http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			final = middlewares[i](final)
		}
		return final
	}
}

// responseWriter 包装的 ResponseWriter
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

// rateLimiter 简单的令牌桶限流器
type rateLimiter struct {
	rate   int
	tokens int
	mu     sync.Mutex
}

func (rl *rateLimiter) allow() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if rl.tokens > 0 {
		rl.tokens--
		return true
	}
	return false
}

func (rl *rateLimiter) refill() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		rl.mu.Lock()
		rl.tokens = rl.rate
		rl.mu.Unlock()
	}
}
