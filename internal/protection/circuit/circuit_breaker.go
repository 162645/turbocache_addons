package circuit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"turbocache/internal/utils/logger"
)

var (
	ErrCircuitOpen     = errors.New("circuit breaker is open")
	ErrTooManyRequests = errors.New("too many requests")
)

// State 熔断器状态
type State int32

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

// CircuitBreaker 熔断器
type CircuitBreaker struct {
	mu     sync.RWMutex
	state  int32 // 使用atomic操作
	config *Config
	logger logger.Logger

	// 计数器（使用 atomic 操作）
	consecutiveFailures  int64
	consecutiveSuccesses int64
	totalRequests        int64
	totalFailures        int64
	totalSuccesses       int64

	// 时间控制
	lastStateChange time.Time
	lastFailureTime time.Time

	// 半开状态下的请求控制
	halfOpenRequests int32
}

// Config 熔断器配置
type Config struct {
	Enabled          bool          `yaml:"enabled"`
	Threshold        int64         `yaml:"threshold"`         // 失败阈值，<=0 表示禁用自动打开（包括失败率）
	Timeout          time.Duration `yaml:"timeout"`           // 熔断超时时间
	MaxRequests      int           `yaml:"max_requests"`      // 半开状态最大请求数
	FailureRatio     float64       `yaml:"failure_ratio"`     // 失败率阈值
	MinRequests      int64         `yaml:"min_requests"`      // 最小请求数（用于计算失败率）
	SuccessThreshold int64         `yaml:"success_threshold"` // 半开状态成功阈值
}

// DefaultConfig 默认配置
func DefaultConfig() *Config {
	return &Config{
		Enabled:          true,
		Threshold:        10,
		Timeout:          30 * time.Second,
		MaxRequests:      5,
		FailureRatio:     0.5,
		MinRequests:      20,
		SuccessThreshold: 5,
	}
}

// NewCircuitBreaker 创建熔断器
func NewCircuitBreaker(name string, config *Config) *CircuitBreaker {
	if config == nil {
		config = DefaultConfig()
	}

	cb := &CircuitBreaker{
		config:          config,
		logger:          logger.WithField("component", fmt.Sprintf("circuit_breaker_%s", name)),
		lastStateChange: time.Now(),
	}

	atomic.StoreInt32(&cb.state, int32(StateClosed))
	return cb
}

// Execute 执行操作，外部把要调用的网络操作传入 Execute。如果熔断功能禁用（config.Enabled == false），直接 operation()。
func (cb *CircuitBreaker) Execute(ctx context.Context, operation func() error) error {
	// 如果禁用熔断器，直接执行（不计入 totalRequests）
	if !cb.config.Enabled {
		return operation()
	}

	// 计数尝试次数（包括被熔断器拒绝的尝试）
	atomic.AddInt64(&cb.totalRequests, 1)

	// 检查是否可以执行
	if err := cb.beforeRequest(); err != nil {
		// 被拒绝的情况（ErrCircuitOpen / ErrTooManyRequests）直接返回
		return err
	}

	// 执行操作
	err := operation()

	// 记录结果（注意 totalRequests 已在入口处计数）
	cb.afterRequest(err)

	return err
}

// beforeRequest 请求前检查
func (cb *CircuitBreaker) beforeRequest() error {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	state := cb.GetState()

	switch state {
	case StateClosed:
		// 关闭状态，允许请求
		return nil

	case StateOpen:
		// 开启状态，检查是否可以转换到半开状态
		if time.Since(cb.lastStateChange) >= cb.config.Timeout {
			cb.setState(StateHalfOpen)
			// The request that performs the transition is the first half-open probe.
			atomic.StoreInt32(&cb.halfOpenRequests, 1)
			return nil
		}
		return ErrCircuitOpen

	case StateHalfOpen:
		// 半开状态，检查请求数限制
		if atomic.LoadInt32(&cb.halfOpenRequests) >= int32(cb.config.MaxRequests) {
			return ErrTooManyRequests
		}
		atomic.AddInt32(&cb.halfOpenRequests, 1)
		return nil

	default:
		return fmt.Errorf("unknown circuit breaker state: %v", state)
	}
}

// afterRequest 请求后处理
func (cb *CircuitBreaker) afterRequest(err error) {
	state := cb.GetState()

	if err != nil {
		cb.onFailure(state)
	} else {
		cb.onSuccess(state)
	}

	// 无论成功或失败，都要检查“按失败率”是否应当打开（仅当 Threshold>0 才允许自动打开）
	// 之所以放在这里：是为了确保在请求之后（即使最后一次是成功），也能依据累计的失败率决定是否打开。
	if cb.GetState() == StateClosed && cb.config != nil && cb.config.Threshold > 0 {
		// 只有当配置启用失败率判断（MinRequests>0 && FailureRatio>0）并且达到最小样本数时才检查
		if cb.config.MinRequests > 0 && cb.config.FailureRatio > 0 {
			totalReq := atomic.LoadInt64(&cb.totalRequests)
			if totalReq >= cb.config.MinRequests {
				totalFail := atomic.LoadInt64(&cb.totalFailures)
				failureRatio := float64(totalFail) / float64(totalReq)
				if failureRatio >= cb.config.FailureRatio {
					// 需要把状态变更放在锁内以保证一致性和日志顺序
					cb.mu.Lock()
					// 再次确认仍为 closed（避免竞态）
					if cb.GetState() == StateClosed {
						cb.logger.Warnf("Circuit breaker opening due to failure ratio %.2f (failures=%d, requests=%d)",
							failureRatio, totalFail, totalReq)
						cb.setState(StateOpen)
					}
					cb.mu.Unlock()
				}
			}
		}
	}
}

// onSuccess 处理成功
func (cb *CircuitBreaker) onSuccess(state State) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.totalSuccesses, 1)
	atomic.AddInt64(&cb.consecutiveSuccesses, 1)
	atomic.StoreInt64(&cb.consecutiveFailures, 0)

	switch state {
	case StateHalfOpen:
		// 成功后减少半开计数
		atomic.AddInt32(&cb.halfOpenRequests, -1)

		// 检查是否达到成功阈值（进入 Closed）
		if atomic.LoadInt64(&cb.consecutiveSuccesses) >= cb.config.SuccessThreshold {
			cb.logger.Info("Circuit breaker transitioning to closed state after successful requests")
			cb.setState(StateClosed)
			cb.resetCounters()
		}

	case StateClosed:
		// 关闭状态：无额外动作
	}
}

// onFailure 处理失败
func (cb *CircuitBreaker) onFailure(state State) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt64(&cb.totalFailures, 1)
	atomic.AddInt64(&cb.consecutiveFailures, 1)
	atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
	cb.lastFailureTime = time.Now()

	switch state {
	case StateClosed:
		// 连续失败导致立即打开（仅当 Threshold>0）
		if cb.config != nil && cb.config.Threshold > 0 {
			if atomic.LoadInt64(&cb.consecutiveFailures) >= cb.config.Threshold {
				cb.logger.Warnf("Circuit breaker opening after %d consecutive failures",
					atomic.LoadInt64(&cb.consecutiveFailures))
				cb.setState(StateOpen)
			}
		}

	case StateHalfOpen:
		// 半开失败 -> 重新打开
		atomic.AddInt32(&cb.halfOpenRequests, -1)
		cb.logger.Warn("Circuit breaker reopening after failure in half-open state")
		cb.setState(StateOpen)
		cb.resetCounters()
	}
}

// shouldTransitionToHalfOpen 判断是否应该转换到半开状态
func (cb *CircuitBreaker) shouldTransitionToHalfOpen() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return time.Since(cb.lastStateChange) >= cb.config.Timeout
}

// setState 设置状态
func (cb *CircuitBreaker) setState(newState State) {
	oldState := State(atomic.LoadInt32(&cb.state))
	atomic.StoreInt32(&cb.state, int32(newState))
	cb.lastStateChange = time.Now()

	if newState == StateHalfOpen {
		atomic.StoreInt32(&cb.halfOpenRequests, 0)
	}

	cb.logger.Infof("Circuit breaker state changed: %s -> %s", oldState, newState)
}

// GetState 获取当前状态
func (cb *CircuitBreaker) GetState() State {
	return State(atomic.LoadInt32(&cb.state))
}

// resetCounters 重置计数器
func (cb *CircuitBreaker) resetCounters() {
	atomic.StoreInt64(&cb.consecutiveFailures, 0)
	atomic.StoreInt64(&cb.consecutiveSuccesses, 0)
	atomic.StoreInt64(&cb.totalRequests, 0)
	atomic.StoreInt64(&cb.totalFailures, 0)
	atomic.StoreInt64(&cb.totalSuccesses, 0)
}

// Reset 重置熔断器
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.setState(StateClosed)
	cb.resetCounters()
	cb.lastStateChange = time.Now()
	cb.lastFailureTime = time.Time{}

	cb.logger.Info("Circuit breaker reset")
}

// GetStats 获取统计信息
func (cb *CircuitBreaker) GetStats() *Stats {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	totalRequests := atomic.LoadInt64(&cb.totalRequests)
	totalFailures := atomic.LoadInt64(&cb.totalFailures)
	var failureRate float64
	if totalRequests > 0 {
		failureRate = float64(totalFailures) / float64(totalRequests)
	}

	return &Stats{
		State:                cb.GetState(),
		ConsecutiveFailures:  atomic.LoadInt64(&cb.consecutiveFailures),
		ConsecutiveSuccesses: atomic.LoadInt64(&cb.consecutiveSuccesses),
		TotalRequests:        totalRequests,
		TotalFailures:        totalFailures,
		TotalSuccesses:       atomic.LoadInt64(&cb.totalSuccesses),
		FailureRate:          failureRate,
		LastStateChange:      cb.lastStateChange,
		LastFailureTime:      cb.lastFailureTime,
	}
}

// Stats 统计信息
type Stats struct {
	State                State     `json:"state"`
	ConsecutiveFailures  int64     `json:"consecutive_failures"`
	ConsecutiveSuccesses int64     `json:"consecutive_successes"`
	TotalRequests        int64     `json:"total_requests"`
	TotalFailures        int64     `json:"total_failures"`
	TotalSuccesses       int64     `json:"total_successes"`
	FailureRate          float64   `json:"failure_rate"`
	LastStateChange      time.Time `json:"last_state_change"`
	LastFailureTime      time.Time `json:"last_failure_time"`
}

// CircuitBreakerManager 熔断器管理器，是一个按 name（通常是后端地址或服务名）管理多个 CircuitBreaker 实例的并发安全容器
type CircuitBreakerManager struct {
	mu       sync.RWMutex
	breakers map[string]*CircuitBreaker
	config   *Config
}

// NewCircuitBreakerManager 创建熔断器管理器
func NewCircuitBreakerManager(config *Config) *CircuitBreakerManager {
	if config == nil {
		config = DefaultConfig()
	}

	return &CircuitBreakerManager{
		breakers: make(map[string]*CircuitBreaker),
		config:   config,
	}
}

// GetOrCreate 获取或创建熔断器
func (m *CircuitBreakerManager) GetOrCreate(name string) *CircuitBreaker {
	m.mu.RLock()
	breaker, exists := m.breakers[name]
	m.mu.RUnlock()

	if exists {
		return breaker
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// 双重检查
	if breaker, exists := m.breakers[name]; exists {
		return breaker
	}

	breaker = NewCircuitBreaker(name, m.config)
	m.breakers[name] = breaker
	return breaker
}

// Get 获取熔断器
func (m *CircuitBreakerManager) Get(name string) (*CircuitBreaker, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	breaker, exists := m.breakers[name]
	return breaker, exists
}

// Remove 移除熔断器
func (m *CircuitBreakerManager) Remove(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.breakers, name)
}

// GetAll 获取所有熔断器
func (m *CircuitBreakerManager) GetAll() map[string]*CircuitBreaker {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]*CircuitBreaker, len(m.breakers))
	for k, v := range m.breakers {
		result[k] = v
	}
	return result
}

// ResetAll 重置所有熔断器
func (m *CircuitBreakerManager) ResetAll() {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, breaker := range m.breakers {
		breaker.Reset()
	}
}
