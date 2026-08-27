package storage

import (
	"errors"
	"fmt"
)

// 预定义错误
var (
	// 通用错误
	ErrKeyNotFound   = errors.New("key not found")
	ErrKeyExists     = errors.New("key already exists")
	ErrInvalidKey    = errors.New("invalid key")
	ErrInvalidValue  = errors.New("invalid value")
	ErrKeyTooLarge   = errors.New("key too large")
	ErrValueTooLarge = errors.New("value too large")
	ErrStorageFull   = errors.New("storage full")
	ErrStorageClosed = errors.New("storage closed")

	// 操作错误
	ErrOperationTimeout      = errors.New("operation timeout")
	ErrOperationCanceled     = errors.New("operation canceled")
	ErrOperationNotSupported = errors.New("operation not supported")
	ErrBatchTooLarge         = errors.New("batch too large")
	ErrBatchEmpty            = errors.New("batch empty")

	// 事务错误
	ErrTransactionNotActive = errors.New("transaction not active")
	ErrTransactionConflict  = errors.New("transaction conflict")
	ErrTransactionReadOnly  = errors.New("transaction read-only")

	// 配置错误
	ErrInvalidConfig          = errors.New("invalid config")
	ErrUnsupportedStorageType = errors.New("unsupported storage type")
	ErrConfigMissing          = errors.New("config missing")

	// 备份错误
	ErrBackupNotFound  = errors.New("backup not found")
	ErrBackupCorrupted = errors.New("backup corrupted")
	ErrRestoreFailed   = errors.New("restore failed")

	// 网络错误
	ErrConnectionFailed   = errors.New("connection failed")
	ErrNetworkTimeout     = errors.New("network timeout")
	ErrNetworkUnavailable = errors.New("network unavailable")

	// 持久化错误
	ErrPersistenceFailed = errors.New("persistence failed")
	ErrDataCorrupted     = errors.New("data corrupted")
	ErrChecksumMismatch  = errors.New("checksum mismatch")
	ErrVersionMismatch   = errors.New("version mismatch")

	// 资源错误
	ErrResourceExhausted  = errors.New("resource exhausted")
	ErrMemoryExhausted    = errors.New("memory exhausted")
	ErrDiskSpaceExhausted = errors.New("disk space exhausted")

	// 并发错误
	ErrConcurrentModification = errors.New("concurrent modification")
	ErrLockTimeout            = errors.New("lock timeout")
	ErrDeadlock               = errors.New("deadlock detected")
)

// StorageError 存储错误类型
type StorageError struct {
	Code      ErrorCode         `json:"code"`
	Message   string            `json:"message"`
	Operation string            `json:"operation,omitempty"`
	Key       string            `json:"key,omitempty"`
	Cause     error             `json:"cause,omitempty"`
	Context   map[string]string `json:"context,omitempty"`
}

// ErrorCode 错误码
type ErrorCode int

const (
	// 成功
	CodeOK ErrorCode = 0

	// 客户端错误 (4xx)
	CodeInvalidArgument       ErrorCode = 400
	CodeKeyNotFound           ErrorCode = 404
	CodeKeyExists             ErrorCode = 409
	CodeKeyTooLarge           ErrorCode = 413
	CodeValueTooLarge         ErrorCode = 414
	CodeOperationNotSupported ErrorCode = 415

	// 服务端错误 (5xx)
	CodeInternalError      ErrorCode = 500
	CodeStorageFull        ErrorCode = 507
	CodeOperationTimeout   ErrorCode = 504
	CodeServiceUnavailable ErrorCode = 503
	CodeResourceExhausted  ErrorCode = 509

	// 事务错误 (6xx)
	CodeTransactionConflict  ErrorCode = 601
	CodeTransactionNotActive ErrorCode = 602
	CodeDeadlock             ErrorCode = 603

	// 网络错误 (7xx)
	CodeConnectionFailed   ErrorCode = 701
	CodeNetworkTimeout     ErrorCode = 702
	CodeNetworkUnavailable ErrorCode = 703

	// 数据错误 (8xx)
	CodeDataCorrupted    ErrorCode = 801
	CodeChecksumMismatch ErrorCode = 802
	CodeVersionMismatch  ErrorCode = 803
	CodeBackupCorrupted  ErrorCode = 804
)

// Error 实现error接口
func (e *StorageError) Error() string {
	msg := e.Message
	if e.Operation != "" {
		msg = fmt.Sprintf("%s (operation: %s)", msg, e.Operation)
	}
	if e.Key != "" {
		msg = fmt.Sprintf("%s (key: %s)", msg, e.Key)
	}
	if e.Cause != nil {
		msg = fmt.Sprintf("%s: %v", msg, e.Cause)
	}
	return msg
}

// Unwrap 实现errors.Unwrap
func (e *StorageError) Unwrap() error {
	return e.Cause
}

// Is 实现errors.Is
func (e *StorageError) Is(target error) bool {
	if target == nil {
		return false
	}

	// 检查是否是同类型的StorageError
	if se, ok := target.(*StorageError); ok {
		return e.Code == se.Code
	}

	// 检查是否是预定义错误
	switch target {
	case ErrKeyNotFound:
		return e.Code == CodeKeyNotFound
	case ErrKeyExists:
		return e.Code == CodeKeyExists
	case ErrKeyTooLarge:
		return e.Code == CodeKeyTooLarge
	case ErrValueTooLarge:
		return e.Code == CodeValueTooLarge
	case ErrStorageFull:
		return e.Code == CodeStorageFull
	case ErrOperationTimeout:
		return e.Code == CodeOperationTimeout
	case ErrOperationNotSupported:
		return e.Code == CodeOperationNotSupported
	case ErrTransactionConflict:
		return e.Code == CodeTransactionConflict
	case ErrTransactionNotActive:
		return e.Code == CodeTransactionNotActive
	case ErrDeadlock:
		return e.Code == CodeDeadlock
	case ErrConnectionFailed:
		return e.Code == CodeConnectionFailed
	case ErrNetworkTimeout:
		return e.Code == CodeNetworkTimeout
	case ErrDataCorrupted:
		return e.Code == CodeDataCorrupted
	case ErrChecksumMismatch:
		return e.Code == CodeChecksumMismatch
	case ErrVersionMismatch:
		return e.Code == CodeVersionMismatch
	case ErrResourceExhausted:
		return e.Code == CodeResourceExhausted
	default:
		return false
	}
}

// WithContext 添加上下文信息
func (e *StorageError) WithContext(key, value string) *StorageError {
	if e.Context == nil {
		e.Context = make(map[string]string)
	}
	e.Context[key] = value
	return e
}

// WithKey 添加键信息
func (e *StorageError) WithKey(key string) *StorageError {
	e.Key = key
	return e
}

// WithOperation 添加操作信息
func (e *StorageError) WithOperation(operation string) *StorageError {
	e.Operation = operation
	return e
}

// WithCause 添加原因
func (e *StorageError) WithCause(cause error) *StorageError {
	e.Cause = cause
	return e
}

// NewStorageError 创建存储错误
func NewStorageError(code ErrorCode, message string) *StorageError {
	return &StorageError{
		Code:    code,
		Message: message,
		Context: make(map[string]string),
	}
}

// 错误构造函数

// NewKeyNotFoundError 键不存在错误
func NewKeyNotFoundError(key string) *StorageError {
	return &StorageError{
		Code:    CodeKeyNotFound,
		Message: "key not found",
		Key:     key,
	}
}

// NewKeyExistsError 键已存在错误
func NewKeyExistsError(key string) *StorageError {
	return &StorageError{
		Code:    CodeKeyExists,
		Message: "key already exists",
		Key:     key,
	}
}

// NewKeyTooLargeError 键过大错误
func NewKeyTooLargeError(key string, size, maxSize int64) *StorageError {
	return &StorageError{
		Code:    CodeKeyTooLarge,
		Message: fmt.Sprintf("key too large: %d bytes (max: %d)", size, maxSize),
		Key:     key,
		Context: map[string]string{
			"size":     fmt.Sprintf("%d", size),
			"max_size": fmt.Sprintf("%d", maxSize),
		},
	}
}

// NewValueTooLargeError 值过大错误
func NewValueTooLargeError(key string, size, maxSize int64) *StorageError {
	return &StorageError{
		Code:    CodeValueTooLarge,
		Message: fmt.Sprintf("value too large: %d bytes (max: %d)", size, maxSize),
		Key:     key,
		Context: map[string]string{
			"size":     fmt.Sprintf("%d", size),
			"max_size": fmt.Sprintf("%d", maxSize),
		},
	}
}

// NewStorageFullError 存储满错误
func NewStorageFullError(used, total int64) *StorageError {
	return &StorageError{
		Code:    CodeStorageFull,
		Message: fmt.Sprintf("storage full: %d/%d bytes used", used, total),
		Context: map[string]string{
			"used_bytes":  fmt.Sprintf("%d", used),
			"total_bytes": fmt.Sprintf("%d", total),
		},
	}
}

// NewOperationTimeoutError 操作超时错误
func NewOperationTimeoutError(operation string, timeout string) *StorageError {
	return &StorageError{
		Code:      CodeOperationTimeout,
		Message:   fmt.Sprintf("operation timeout: %s", timeout),
		Operation: operation,
		Context: map[string]string{
			"timeout": timeout,
		},
	}
}

// NewTransactionConflictError 事务冲突错误
func NewTransactionConflictError(key string) *StorageError {
	return &StorageError{
		Code:    CodeTransactionConflict,
		Message: "transaction conflict detected",
		Key:     key,
	}
}

// NewConnectionFailedError 连接失败错误
func NewConnectionFailedError(endpoint string, cause error) *StorageError {
	return &StorageError{
		Code:    CodeConnectionFailed,
		Message: fmt.Sprintf("failed to connect to %s", endpoint),
		Cause:   cause,
		Context: map[string]string{
			"endpoint": endpoint,
		},
	}
}

// NewDataCorruptedError 数据损坏错误
func NewDataCorruptedError(key string, cause error) *StorageError {
	return &StorageError{
		Code:    CodeDataCorrupted,
		Message: "data corrupted",
		Key:     key,
		Cause:   cause,
	}
}

// IsRetryable 检查错误是否可重试
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	var se *StorageError
	if errors.As(err, &se) {
		switch se.Code {
		case CodeOperationTimeout, CodeServiceUnavailable, CodeConnectionFailed,
			CodeNetworkTimeout, CodeResourceExhausted:
			return true
		default:
			return false
		}
	}

	// 检查特定的预定义错误
	switch {
	case errors.Is(err, ErrOperationTimeout),
		errors.Is(err, ErrConnectionFailed),
		errors.Is(err, ErrNetworkTimeout),
		errors.Is(err, ErrResourceExhausted):
		return true
	default:
		return false
	}
}

// IsPermanent 检查错误是否是永久性的
func IsPermanent(err error) bool {
	if err == nil {
		return false
	}

	var se *StorageError
	if errors.As(err, &se) {
		switch se.Code {
		case CodeInvalidArgument, CodeKeyTooLarge, CodeValueTooLarge,
			CodeOperationNotSupported, CodeVersionMismatch, CodeDataCorrupted:
			return true
		default:
			return false
		}
	}

	// 检查特定的预定义错误
	switch {
	case errors.Is(err, ErrInvalidKey),
		errors.Is(err, ErrInvalidValue),
		errors.Is(err, ErrKeyTooLarge),
		errors.Is(err, ErrValueTooLarge),
		errors.Is(err, ErrOperationNotSupported),
		errors.Is(err, ErrDataCorrupted),
		errors.Is(err, ErrVersionMismatch):
		return true
	default:
		return false
	}
}

// IsNotFound 检查是否是"未找到"错误
func IsNotFound(err error) bool {
	if err == nil {
		return false
	}

	var se *StorageError
	if errors.As(err, &se) {
		return se.Code == CodeKeyNotFound
	}

	return errors.Is(err, ErrKeyNotFound)
}

// IsConflict 检查是否是冲突错误
func IsConflict(err error) bool {
	if err == nil {
		return false
	}

	var se *StorageError
	if errors.As(err, &se) {
		return se.Code == CodeKeyExists || se.Code == CodeTransactionConflict
	}

	return errors.Is(err, ErrKeyExists) || errors.Is(err, ErrTransactionConflict)
}

// GetErrorCode 获取错误码
func GetErrorCode(err error) ErrorCode {
	if err == nil {
		return CodeOK
	}

	var se *StorageError
	if errors.As(err, &se) {
		return se.Code
	}

	// 映射预定义错误到错误码
	switch {
	case errors.Is(err, ErrKeyNotFound):
		return CodeKeyNotFound
	case errors.Is(err, ErrKeyExists):
		return CodeKeyExists
	case errors.Is(err, ErrKeyTooLarge):
		return CodeKeyTooLarge
	case errors.Is(err, ErrValueTooLarge):
		return CodeValueTooLarge
	case errors.Is(err, ErrStorageFull):
		return CodeStorageFull
	case errors.Is(err, ErrOperationTimeout):
		return CodeOperationTimeout
	case errors.Is(err, ErrOperationNotSupported):
		return CodeOperationNotSupported
	case errors.Is(err, ErrTransactionConflict):
		return CodeTransactionConflict
	case errors.Is(err, ErrTransactionNotActive):
		return CodeTransactionNotActive
	case errors.Is(err, ErrDeadlock):
		return CodeDeadlock
	case errors.Is(err, ErrConnectionFailed):
		return CodeConnectionFailed
	case errors.Is(err, ErrNetworkTimeout):
		return CodeNetworkTimeout
	case errors.Is(err, ErrDataCorrupted):
		return CodeDataCorrupted
	case errors.Is(err, ErrChecksumMismatch):
		return CodeChecksumMismatch
	case errors.Is(err, ErrVersionMismatch):
		return CodeVersionMismatch
	case errors.Is(err, ErrResourceExhausted):
		return CodeResourceExhausted
	default:
		return CodeInternalError
	}
}

// ErrorCodeToHTTPStatus 错误码转HTTP状态码
func ErrorCodeToHTTPStatus(code ErrorCode) int {
	switch code {
	case CodeOK:
		return 200
	case CodeInvalidArgument:
		return 400
	case CodeKeyNotFound:
		return 404
	case CodeKeyExists:
		return 409
	case CodeKeyTooLarge, CodeValueTooLarge:
		return 413
	case CodeOperationNotSupported:
		return 415
	case CodeInternalError:
		return 500
	case CodeServiceUnavailable:
		return 503
	case CodeOperationTimeout:
		return 504
	case CodeStorageFull, CodeResourceExhausted:
		return 507
	default:
		return 500
	}
}

// WrapError 包装错误
func WrapError(err error, operation string) error {
	if err == nil {
		return nil
	}

	var se *StorageError
	if errors.As(err, &se) {
		// 如果已经是StorageError，添加操作信息
		if se.Operation == "" {
			se.Operation = operation
		}
		return se
	}

	// 创建新的StorageError
	return &StorageError{
		Code:      GetErrorCode(err),
		Message:   err.Error(),
		Operation: operation,
		Cause:     err,
	}
}
