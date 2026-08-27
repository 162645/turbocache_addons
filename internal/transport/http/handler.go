package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"embed"
	"turbocache/internal/cache/value"
	"turbocache/internal/utils/logger"
)

//go:embed static/*
var staticFS embed.FS

// ------------------------ 关键改动：定义 metrics 接口 ------------------------

// MetricsCollector 是 handler 所需的指标收集接口（只包含 handler 使用的方法）
// 任何实现了下面方法集的类型（包括你的 mock 与真实 *metrics.Collector）都可以注入
type MetricsCollector interface {
	RecordGetLatency(d time.Duration)
	RecordSetLatency(d time.Duration)
	RecordDeleteLatency(d time.Duration)
	RecordHit()
	RecordMiss()
	RecordSet()
	RecordDelete()
	RecordError()
}

// ------------------------ 其余保持原有实现 ------------------------

// CacheOperator 缓存操作接口（由外部注入具体实现）
type CacheOperator interface {
	Get(ctx context.Context, key string) (*value.Entry, error)
	Set(ctx context.Context, key string, entry *value.Entry) error
	Delete(ctx context.Context, key string) error
	Clear(ctx context.Context) error
	Stats() (interface{}, error)
}

// Handler HTTP处理器
type Handler struct {
	cache   CacheOperator
	metrics MetricsCollector // <- 改为接口
	logger  logger.Logger
}

// NewHandler 创建HTTP处理器（接收 MetricsCollector 接口，方便 mock 注入）
func NewHandler(cache CacheOperator, metricsCollector MetricsCollector) *Handler {
	return &Handler{
		cache:   cache,
		metrics: metricsCollector,
		logger:  logger.WithField("component", "http_handler"),
	}
}

// RegisterRoutes 注册路由
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	// 缓存操作
	mux.HandleFunc("/cache/", h.cacheHandler)

	// 批量操作
	mux.HandleFunc("/batch/get", h.batchGetHandler)
	mux.HandleFunc("/batch/set", h.batchSetHandler)
	mux.HandleFunc("/batch/delete", h.batchDeleteHandler)

	// 管理接口
	mux.HandleFunc("/stats", h.statsHandler)
	mux.HandleFunc("/health", h.healthHandler)
	mux.HandleFunc("/clear", h.clearHandler)

	// Web 管理界面
	staticServer := http.FileServer(http.FS(staticFS))
	mux.Handle("/admin/", http.StripPrefix("/admin/", staticServer))

	// 如果访问根路径，重定向到 admin
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/admin/", http.StatusFound)
			return
		}
		// 如果不是根路径且没匹配到，返回 404
		http.NotFound(w, r)
	})
}

// cacheHandler 处理单个缓存操作
func (h *Handler) cacheHandler(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// 解析路径 /cache/{key}
	path := strings.TrimPrefix(r.URL.Path, "/cache/")
	key := path

	if key == "" {
		h.writeError(w, http.StatusBadRequest, "key cannot be empty")
		return
	}

	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r, key, start)
	case http.MethodPost, http.MethodPut:
		h.handleSet(w, r, key, start)
	case http.MethodDelete:
		h.handleDelete(w, r, key, start)
	default:
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

// handleGet 处理GET请求
func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request, key string, start time.Time) {
	defer func() {
		if h.metrics != nil {
			h.metrics.RecordGetLatency(time.Since(start))
		}
	}()

	entry, err := h.cache.Get(r.Context(), key)
	if err != nil {
		if h.metrics != nil {
			h.metrics.RecordError()
			h.metrics.RecordMiss()
		}
		h.logger.Errorf("Failed to get key %s: %v", key, err)
		h.writeError(w, http.StatusNotFound, "key not found")
		return
	}

	if h.metrics != nil {
		h.metrics.RecordHit()
	}

	// 设置响应头
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "private")
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(entry.Value.ByteSlice()); err != nil {
		h.logger.Errorf("Failed to write response: %v", err)
	}
}

// handleSet 处理SET请求
func (h *Handler) handleSet(w http.ResponseWriter, r *http.Request, key string, start time.Time) {
	defer func() {
		if h.metrics != nil {
			h.metrics.RecordSetLatency(time.Since(start))
		}
	}()

	// 读取请求体
	body, err := io.ReadAll(r.Body)
	if err != nil {
		if h.metrics != nil {
			h.metrics.RecordError()
		}
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("failed to read body: %v", err))
		return
	}
	defer r.Body.Close()

	// 解析TTL
	var ttl time.Duration = time.Hour // 默认TTL
	if ttlStr := r.Header.Get("X-TTL"); ttlStr != "" {
		if ttlSeconds, err := strconv.Atoi(ttlStr); err == nil {
			ttl = time.Duration(ttlSeconds) * time.Second
		}
	}

	// 创建缓存条目
	entry := value.NewEntry(key, value.NewByteView(body), ttl)

	err = h.cache.Set(r.Context(), key, entry)
	if err != nil {
		if h.metrics != nil {
			h.metrics.RecordError()
		}
		h.logger.Errorf("Failed to set key %s: %v", key, err)
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to set key: %v", err))
		return
	}

	if h.metrics != nil {
		h.metrics.RecordSet()
	}
	h.writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "key set successfully",
	})
}

// handleDelete 处理DELETE请求
func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request, key string, start time.Time) {
	defer func() {
		if h.metrics != nil {
			h.metrics.RecordDeleteLatency(time.Since(start))
		}
	}()

	err := h.cache.Delete(r.Context(), key)
	found := err == nil

	if err != nil && err.Error() != "key not found" {
		if h.metrics != nil {
			h.metrics.RecordError()
		}
		h.logger.Errorf("Failed to delete key %s: %v", key, err)
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to delete key: %v", err))
		return
	}

	if h.metrics != nil {
		h.metrics.RecordDelete()
	}

	status := http.StatusOK
	if !found {
		status = http.StatusNotFound
	}

	w.WriteHeader(status)
	h.writeJSON(w, map[string]interface{}{
		"success": true,
		"found":   found,
		"message": "delete operation completed",
	})
}

// batchGetHandler 批量获取处理器
func (h *Handler) batchGetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req BatchGetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if len(req.Keys) == 0 {
		h.writeError(w, http.StatusBadRequest, "keys array cannot be empty")
		return
	}

	results := make(map[string]BatchResult)
	for _, key := range req.Keys {
		entry, err := h.cache.Get(r.Context(), key)

		result := BatchResult{
			Found: err == nil,
		}

		if err != nil {
			result.Error = err.Error()
			if h.metrics != nil {
				h.metrics.RecordError()
				h.metrics.RecordMiss()
			}
		} else {
			result.Value = entry.Value.ByteSlice()
			if h.metrics != nil {
				h.metrics.RecordHit()
			}
		}

		results[key] = result
	}

	h.writeJSON(w, BatchGetResponse{
		Results: results,
		Success: true,
	})
}

// batchSetHandler 批量设置处理器
func (h *Handler) batchSetHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req BatchSetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if len(req.Items) == 0 {
		h.writeError(w, http.StatusBadRequest, "items array cannot be empty")
		return
	}

	results := make(map[string]BatchResult)
	for _, item := range req.Items {
		var ttl time.Duration = time.Hour // 默认TTL
		if item.TTLSeconds > 0 {
			ttl = time.Duration(item.TTLSeconds) * time.Second
		}

		entry := value.NewEntry(item.Key, value.NewByteView(item.Value), ttl)
		err := h.cache.Set(r.Context(), item.Key, entry)

		result := BatchResult{
			Found: err == nil,
		}

		if err != nil {
			result.Error = err.Error()
			if h.metrics != nil {
				h.metrics.RecordError()
			}
		} else {
			if h.metrics != nil {
				h.metrics.RecordSet()
			}
		}

		results[item.Key] = result
	}

	h.writeJSON(w, BatchSetResponse{
		Results: results,
		Success: true,
	})
}

// batchDeleteHandler 批量删除处理器
func (h *Handler) batchDeleteHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	var req BatchDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON: %v", err))
		return
	}

	if len(req.Keys) == 0 {
		h.writeError(w, http.StatusBadRequest, "keys array cannot be empty")
		return
	}

	results := make(map[string]BatchResult)
	for _, key := range req.Keys {
		err := h.cache.Delete(r.Context(), key)
		found := err == nil

		result := BatchResult{
			Found: found,
		}

		if err != nil && err.Error() != "key not found" {
			result.Error = err.Error()
			if h.metrics != nil {
				h.metrics.RecordError()
			}
		} else {
			if h.metrics != nil {
				h.metrics.RecordDelete()
			}
		}

		results[key] = result
	}

	h.writeJSON(w, BatchDeleteResponse{
		Results: results,
		Success: true,
	})
}

// statsHandler 统计信息处理器
func (h *Handler) statsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats, err := h.cache.Stats()
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to get stats: %v", err))
		return
	}

	h.writeJSON(w, stats)
}

// healthHandler 健康检查处理器
func (h *Handler) healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	h.writeJSON(w, map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
	})
}

// clearHandler 清空缓存处理器
func (h *Handler) clearHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	err := h.cache.Clear(r.Context())
	if err != nil {
		if h.metrics != nil {
			h.metrics.RecordError()
		}
		h.logger.Errorf("Failed to clear cache: %v", err)
		h.writeError(w, http.StatusInternalServerError, fmt.Sprintf("failed to clear cache: %v", err))
		return
	}

	h.writeJSON(w, map[string]interface{}{
		"success": true,
		"message": "cache cleared successfully",
	})
}

// writeJSON 写入JSON响应
func (h *Handler) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.logger.Errorf("Failed to encode JSON response: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

// writeError 写入错误响应
func (h *Handler) writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	response := map[string]interface{}{
		"success": false,
		"error":   message,
		"code":    status,
	}

	json.NewEncoder(w).Encode(response)
}

// 请求和响应结构（保持原有定义）
type BatchGetRequest struct {
	Keys []string `json:"keys"`
}
type BatchSetRequest struct {
	Items []BatchSetItem `json:"items"`
}
type BatchSetItem struct {
	Key        string `json:"key"`
	Value      []byte `json:"value"`
	TTLSeconds int    `json:"ttl_seconds,omitempty"`
}
type BatchDeleteRequest struct {
	Keys []string `json:"keys"`
}
type BatchResult struct {
	Found bool   `json:"found"`
	Value []byte `json:"value,omitempty"`
	Error string `json:"error,omitempty"`
}
type BatchGetResponse struct {
	Results map[string]BatchResult `json:"results"`
	Success bool                   `json:"success"`
}
type BatchSetResponse struct {
	Results map[string]BatchResult `json:"results"`
	Success bool                   `json:"success"`
}
type BatchDeleteResponse struct {
	Results map[string]BatchResult `json:"results"`
	Success bool                   `json:"success"`
}
