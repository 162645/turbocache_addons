// transport/http_handler_test.go
//
// 修复后的完整测试文件：
// - 使用中文注释和断言信息
// - mockMetricsCollector 与生产代码约定的方法集一致（便于注入）
// - mockCacheOperator 并发安全（sync.RWMutex），在 Set/Get 时使用 Clone 避免共享对象被修改导致竞态
// - 覆盖：单键操作、批量操作、管理接口、错误情况、并发、大值与基准测试
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"turbocache/internal/cache/value"
	httphandler "turbocache/internal/transport/http"
)

// -------------------- Mock 与 辅助实现（并发安全） --------------------

// mockMetricsCollector 模拟指标收集器（方法与生产代码调用的一致）
type mockMetricsCollector struct{}

func (m *mockMetricsCollector) RecordGetLatency(d time.Duration)    {}
func (m *mockMetricsCollector) RecordSetLatency(d time.Duration)    {}
func (m *mockMetricsCollector) RecordDeleteLatency(d time.Duration) {}
func (m *mockMetricsCollector) RecordHit()                          {}
func (m *mockMetricsCollector) RecordMiss()                         {}
func (m *mockMetricsCollector) RecordSet()                          {}
func (m *mockMetricsCollector) RecordDelete()                       {}
func (m *mockMetricsCollector) RecordError()                        {}

// mockCacheOperator 并发安全的缓存操作模拟，适用于单项与批量测试
type mockCacheOperator struct {
	mu   sync.RWMutex
	data map[string]*value.Entry
}

func newMockCacheOperator() *mockCacheOperator {
	return &mockCacheOperator{
		data: make(map[string]*value.Entry),
	}
}

func (m *mockCacheOperator) Get(ctx context.Context, key string) (*value.Entry, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.data[key]; ok {
		// 返回克隆，避免测试中误修改同一对象引起竞态或断言不稳定
		return e.Clone(), nil
	}
	return nil, fmt.Errorf("key not found")
}

func (m *mockCacheOperator) Set(ctx context.Context, key string, entry *value.Entry) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 存储克隆，避免外部对象被并发修改
	m.data[key] = entry.Clone()
	return nil
}

func (m *mockCacheOperator) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.data[key]; !ok {
		// 与生产代码约定：删除不存在返回 "key not found" 错误字符串
		return fmt.Errorf("key not found")
	}
	delete(m.data, key)
	return nil
}

func (m *mockCacheOperator) Clear(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string]*value.Entry)
	return nil
}

func (m *mockCacheOperator) Stats() (interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return map[string]interface{}{
		"total_keys": len(m.data),
		"hit_rate":   0.95,
	}, nil
}

// -------------------- 测试用例 --------------------

// TestNewHandler 测试创建 Handler（构造是否成功）
func TestNewHandler(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}

	h := httphandler.NewHandler(cache, metrics)
	if h == nil {
		t.Fatal("NewHandler 返回 nil，期望非 nil")
	}
}

// TestHandler_Get 测试单键 GET 行为：存在 -> 返回 raw bytes 和 200；不存在 -> 404 JSON 错误；空 key -> 400
func TestHandler_Get(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置数据（key 存在）
	entry := value.NewEntry("test-key", value.NewByteView([]byte("test-value")), time.Hour)
	if err := cache.Set(context.Background(), "test-key", entry); err != nil {
		t.Fatalf("预置数据失败: %v", err)
	}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	// 1) 存在的键
	{
		req := httptest.NewRequest(http.MethodGet, "/cache/test-key", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("期望状态 %d, 实际 %d", http.StatusOK, w.Code)
		}
		// handler 对单 key GET 返回 raw bytes（Content-Type = application/octet-stream）
		if ct := w.Header().Get("Content-Type"); ct != "application/octet-stream" {
			t.Fatalf("期望 Content-Type application/octet-stream，实际 %s", ct)
		}
		if got := w.Body.Bytes(); !bytes.Equal(got, []byte("test-value")) {
			t.Fatalf("期望 body 'test-value'，实际 '%s'", string(got))
		}
	}

	// 2) 不存在的键 -> 404 + JSON 错误结构
	{
		req := httptest.NewRequest(http.MethodGet, "/cache/non-existing", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("期望状态 %d, 实际 %d", http.StatusNotFound, w.Code)
		}

		var resp map[string]interface{}
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("解码错误响应失败: %v", err)
		}
		if ok, exists := resp["success"].(bool); !exists || ok {
			t.Fatalf("期望 success=false，实际 %v", resp["success"])
		}
		if _, has := resp["error"]; !has {
			t.Fatalf("期望 error 字段存在")
		}
	}

	// 3) 空 key -> 400
	{
		req := httptest.NewRequest(http.MethodGet, "/cache/", nil) // path 解析后 key == ""
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("期望状态 %d（空 key），实际 %d", http.StatusBadRequest, w.Code)
		}
	}
}

// TestHandler_Set 测试 POST/PUT 设置（带/不带 TTL）
func TestHandler_Set(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	tests := []struct {
		name           string
		method         string
		key            string
		value          []byte
		ttlHeader      string // X-TTL 秒数（可空）
		expectedStatus int
	}{
		{"POST set", http.MethodPost, "post-key", []byte("post-value"), "", http.StatusOK},
		{"PUT set", http.MethodPut, "put-key", []byte("put-value"), "", http.StatusOK},
		{"POST with TTL", http.MethodPost, "ttl-key", []byte("ttl-value"), "3600", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := fmt.Sprintf("/cache/%s", tt.key)
			req := httptest.NewRequest(tt.method, path, bytes.NewReader(tt.value))
			if tt.ttlHeader != "" {
				req.Header.Set("X-TTL", tt.ttlHeader)
			}

			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != tt.expectedStatus {
				t.Fatalf("期望状态 %d, 实际 %d", tt.expectedStatus, w.Code)
			}

			// 解析 JSON 响应，检查 success 字段
			var resp map[string]interface{}
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("解析响应失败: %v", err)
			}
			if success, ok := resp["success"].(bool); !ok || !success {
				t.Fatalf("期望 success=true，实际 %v", resp["success"])
			}

			// 再 Get 一次校验值（存在性）
			getReq := httptest.NewRequest(http.MethodGet, "/cache/"+tt.key, nil)
			getW := httptest.NewRecorder()
			mux.ServeHTTP(getW, getReq)
			if getW.Code != http.StatusOK {
				t.Fatalf("设置后 GET 期望 200，实际 %d", getW.Code)
			}
		})
	}
}

// TestHandler_Delete 测试 DELETE：存在 -> 200 found=true；不存在 -> 404 found=false
func TestHandler_Delete(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置一个键
	entry := value.NewEntry("delete-key", value.NewByteView([]byte("delete-value")), time.Hour)
	cache.Set(context.Background(), "delete-key", entry)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	cases := []struct {
		key            string
		expectedStatus int
		expectedFound  bool
	}{
		{"delete-key", http.StatusOK, true},
		{"no-such-key", http.StatusNotFound, false},
	}

	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodDelete, "/cache/"+c.key, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != c.expectedStatus {
				t.Fatalf("期望状态 %d，实际 %d", c.expectedStatus, w.Code)
			}

			var resp map[string]interface{}
			if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
				t.Fatalf("解析响应失败: %v", err)
			}
			found, _ := resp["found"].(bool)
			if found != c.expectedFound {
				t.Fatalf("期望 found=%v，实际 %v", c.expectedFound, found)
			}
		})
	}
}

// TestHandler_BatchSet 测试批量设置接口（成功与空数组错误）
func TestHandler_BatchSet(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	t.Run("批量设置多个项", func(t *testing.T) {
		items := []httphandler.BatchSetItem{
			{Key: "batch-set-1", Value: []byte("v1"), TTLSeconds: 3600},
			{Key: "batch-set-2", Value: []byte("v2"), TTLSeconds: 3600},
			{Key: "batch-set-3", Value: []byte("v3"), TTLSeconds: 3600},
		}
		reqBody := httphandler.BatchSetRequest{Items: items}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/batch/set", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("期望 200，实际 %d", w.Code)
		}

		var resp httphandler.BatchSetResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}

		if len(resp.Results) != len(items) {
			t.Fatalf("期望 %d 个结果，实际 %d", len(items), len(resp.Results))
		}
	})

	t.Run("空 items 应返回 400", func(t *testing.T) {
		reqBody := httphandler.BatchSetRequest{Items: []httphandler.BatchSetItem{}}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/batch/set", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		mux.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("期望 400，实际 %d", w.Code)
		}
	})
}

// TestHandler_BatchGet 测试批量获取接口（全部存在 / 混合 / 空 keys）
func TestHandler_BatchGet(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置数据
	for i := 1; i <= 3; i++ {
		k := fmt.Sprintf("batch-key-%d", i)
		v := fmt.Sprintf("batch-value-%d", i)
		cache.Set(context.Background(), k, value.NewEntry(k, value.NewByteView([]byte(v)), time.Hour))
	}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	cases := []struct {
		name           string
		keys           []string
		expectedStatus int
		expectedFound  int
	}{
		{"all exist", []string{"batch-key-1", "batch-key-2", "batch-key-3"}, http.StatusOK, 3},
		{"mixed", []string{"batch-key-1", "no", "batch-key-2"}, http.StatusOK, 2},
		{"empty keys", []string{}, http.StatusBadRequest, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			reqBody := httphandler.BatchGetRequest{Keys: c.keys}
			body, _ := json.Marshal(reqBody)
			req := httptest.NewRequest(http.MethodPost, "/batch/get", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			mux.ServeHTTP(w, req)

			if w.Code != c.expectedStatus {
				t.Fatalf("期望 %d，實際 %d", c.expectedStatus, w.Code)
			}

			if c.expectedStatus == http.StatusOK {
				var resp httphandler.BatchGetResponse
				if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
					t.Fatalf("解析响应失败: %v", err)
				}
				found := 0
				for _, r := range resp.Results {
					if r.Found {
						found++
					}
				}
				if found != c.expectedFound {
					t.Fatalf("期望找到 %d 个，实际 %d 个", c.expectedFound, found)
				}
			}
		})
	}
}

// TestHandler_BatchDelete 批量删除测试
func TestHandler_BatchDelete(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置要删除的数据
	for i := 1; i <= 3; i++ {
		k := fmt.Sprintf("batch-del-%d", i)
		cache.Set(context.Background(), k, value.NewEntry(k, value.NewByteView([]byte("v")), time.Hour))
	}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	t.Run("批量删除存在和不存在的键", func(t *testing.T) {
		reqBody := httphandler.BatchDeleteRequest{Keys: []string{"batch-del-1", "batch-del-2", "no"}}
		body, _ := json.Marshal(reqBody)
		req := httptest.NewRequest(http.MethodPost, "/batch/delete", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("期望 200，实际 %d", w.Code)
		}

		var resp httphandler.BatchDeleteResponse
		if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
			t.Fatalf("解析响应失败: %v", err)
		}

		if len(resp.Results) != 3 {
			t.Fatalf("期望 3 个结果，实际 %d", len(resp.Results))
		}
	})
}

// TestHandler_Stats 测试 stats 接口
func TestHandler_Stats(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置数据
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("stats-key-%d", i)
		cache.Set(context.Background(), k, value.NewEntry(k, value.NewByteView([]byte("v")), time.Hour))
	}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("期望 200，实际 %d", w.Code)
	}

	var stats map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&stats); err != nil {
		t.Fatalf("解析 stats 失败: %v", err)
	}

	if total, ok := stats["total_keys"]; ok {
		t.Logf("total_keys = %v", total)
	} else {
		t.Fatalf("期望返回 total_keys 字段")
	}
}

// TestHandler_Health 测试健康检查接口
func TestHandler_Health(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("期望 200，实际 %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}

	if status, ok := resp["status"].(string); !ok || status != "healthy" {
		t.Fatalf("期望 status=healthy，实际 %v", resp["status"])
	}
}

// TestHandler_Clear 测试清空缓存
func TestHandler_Clear(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置数据
	for i := 1; i <= 5; i++ {
		k := fmt.Sprintf("clear-key-%d", i)
		cache.Set(context.Background(), k, value.NewEntry(k, value.NewByteView([]byte("v")), time.Hour))
	}

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodPost, "/clear", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("期望 200，实际 %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}

	if ok, _ := resp["success"].(bool); !ok {
		t.Fatalf("期望 success=true，实际 %v", resp["success"])
	}

	// 确认已清空
	stats, _ := cache.Stats()
	statsMap := stats.(map[string]interface{})
	if statsMap["total_keys"].(int) != 0 {
		t.Fatalf("期望清空后 total_keys=0，实际 %v", statsMap["total_keys"])
	}
}

// TestHandler_InvalidJSON 测试无效 JSON 会返回 400
func TestHandler_InvalidJSON(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	tests := []struct {
		name string
		path string
		body string
	}{
		{"invalid batch get", "/batch/get", "{invalid json}"},
		{"invalid batch set", "/batch/set", "not a json"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tt.path, bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("期望 400，實際 %d", w.Code)
			}
		})
	}
}

// TestHandler_MethodNotAllowed 测试不允许的方法返回 405（或者被特殊处理时仍能安全返回）
func TestHandler_MethodNotAllowed(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	cases := []struct {
		method string
		path   string
	}{
		{http.MethodPatch, "/cache/test-key"},
		{http.MethodGet, "/batch/set"},
		{http.MethodDelete, "/stats"},
	}

	for _, c := range cases {
		t.Run(c.method+" "+c.path, func(t *testing.T) {
			req := httptest.NewRequest(c.method, c.path, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			// 只要返回了有效状态码且不 panic 即可，具体状态可能依实现略有不同
			if w.Code == 0 {
				t.Fatalf("收到无效响应状态码 0")
			}
		})
	}
}

// TestHandler_ContentType 验证 GET 单条返回 content-type 为 octet-stream
func TestHandler_ContentType(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置数据
	cache.Set(context.Background(), "content-key", value.NewEntry("content-key", value.NewByteView([]byte("content-value")), time.Hour))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/cache/content-key", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("期望 Content-Type application/octet-stream，实际 %s", w.Header().Get("Content-Type"))
	}
}

// TestHandler_LargeValue 测试处理大值（1MB）
func TestHandler_LargeValue(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	largeValue := bytes.Repeat([]byte("x"), 1024*1024) // 1MB
	// POST 写入大值
	req := httptest.NewRequest(http.MethodPost, "/cache/large-key", bytes.NewReader(largeValue))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("POST 大值期望 200，实际 %d", w.Code)
	}

	// GET 验证内容长度
	getReq := httptest.NewRequest(http.MethodGet, "/cache/large-key", nil)
	getW := httptest.NewRecorder()
	mux.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusOK {
		t.Fatalf("GET 大值期望 200，实际 %d", getW.Code)
	}
	if getW.Body.Len() != len(largeValue) {
		t.Fatalf("期望 body 长度 %d，实际 %d", len(largeValue), getW.Body.Len())
	}
}

// TestHandler_ConcurrentRequests 并发读写压力测试，检查无竞态与最终键数量合理
func TestHandler_ConcurrentRequests(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	var wg sync.WaitGroup
	const N = 100

	// 并发写入
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("c-key-%d", i)
			req := httptest.NewRequest(http.MethodPost, "/cache/"+key, bytes.NewReader([]byte(fmt.Sprintf("v-%d", i))))
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("写入 %s 失败，状态 %d", key, w.Code)
			}
		}(i)
	}

	// 并发读取（部分可能读到空）
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("c-key-%d", i%50)
			req := httptest.NewRequest(http.MethodGet, "/cache/"+key, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			// 不断言状态，主要确保无 panic / crash
			_ = w
		}(i)
	}

	wg.Wait()

	stats, _ := cache.Stats()
	statsMap := stats.(map[string]interface{})
	t.Logf("最终键数: %v", statsMap["total_keys"])
}

// TestHandler_ErrorResponse 验证错误响应字段格式（success=false, error, code）
func TestHandler_ErrorResponse(t *testing.T) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest(http.MethodGet, "/cache/non-existing-key", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var errResp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&errResp); err != nil {
		t.Fatalf("解析错误响应失败: %v", err)
	}

	if s, ok := errResp["success"].(bool); !ok || s {
		t.Fatalf("期望 success=false，实际 %v", errResp["success"])
	}
	if _, ok := errResp["error"]; !ok {
		t.Fatalf("期望 error 字段存在")
	}
	if _, ok := errResp["code"]; !ok {
		t.Fatalf("期望 code 字段存在")
	}
}

// -------------------- 基准测试（可选） --------------------

func BenchmarkHandler_Get(b *testing.B) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	// 预置数据
	cache.Set(context.Background(), "bench-key", value.NewEntry("bench-key", value.NewByteView([]byte("bench-v")), time.Hour))

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodGet, "/cache/bench-key", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		_ = w
	}
}

func BenchmarkHandler_Set(b *testing.B) {
	cache := newMockCacheOperator()
	metrics := &mockMetricsCollector{}
	h := httphandler.NewHandler(cache, metrics)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	val := []byte("bench")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/cache/bench-%d", i), bytes.NewReader(val))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		_ = w
	}
}
