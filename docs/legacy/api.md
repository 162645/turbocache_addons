> 历史资料：保留原文供追溯，不代表当前部署方式或已验证能力。当前入口见 [项目首页](../../README.md)。

# TurboCache API 文档

## 1. HTTP API

### 1.1 缓存操作

#### GET /cache/{group}/{key}

获取缓存值

**请求示例**：
```bash
curl http://localhost:8080/cache/user/user:123
```

**响应**：
- 200: 成功返回缓存值（binary）
- 404: 键不存在
- 500: 服务器错误

---

#### POST/PUT /cache/{group}/{key}

设置缓存值

**请求头**：
- `X-TTL`: 过期时间（秒）

**请求示例**：
```bash
curl -X POST http://localhost:8080/cache/user/user:123 \
  -H "X-TTL: 3600" \
  -d '{"name":"John","age":30}'
```

**响应**：
```json
{
  "success": true,
  "message": "key set successfully"
}
```

---

#### DELETE /cache/{group}/{key}

删除缓存值

**请求示例**：
```bash
curl -X DELETE http://localhost:8080/cache/user/user:123
```

**响应**：
```json
{
  "success": true,
  "found": true,
  "message": "delete operation completed"
}
```

---

### 1.2 批量操作

#### POST /batch/get

批量获取

**请求体**：
```json
{
  "group": "user",
  "keys": ["user:1", "user:2", "user:3"]
}
```

**响应**：
```json
{
  "success": true,
  "results": {
    "user:1": {
      "found": true,
      "value": "..."
    },
    "user:2": {
      "found": false
    }
  }
}
```

---

#### POST /batch/set

批量设置

**请求体**：
```json
{
  "group": "user",
  "items": [
    {
      "key": "user:1",
      "value": "...",
      "ttl_seconds": 3600
    }
  ]
}
```

---

#### POST /batch/delete

批量删除

**请求体**：
```json
{
  "group": "user",
  "keys": ["user:1", "user:2"]
}
```

---

### 1.3 管理接口

#### GET /stats

获取统计信息

**响应**：
```json
{
  "timestamp": "2025-09-29T10:00:00Z",
  "uptime": "2h30m",
  "cache": {
    "hit_count": 10000,
    "miss_count": 2000,
    "hit_rate": 0.83,
    "total_keys": 5000,
    "total_size": 10485760,
    "l1": {
      "keys": 3000,
      "size": 5242880
    },
    "l2": {
      "keys": 2000,
      "size": 5242880
    }
  },
  "latency": {
    "get": {
      "avg_ms": 0.5,
      "p50_ms": 0.3,
      "p95_ms": 1.2,
      "p99_ms": 2.5
    }
  },
  "system": {
    "goroutines": 150,
    "heap_size": 52428800,
    "gc_count": 50
  }
}
```

---

#### GET /health

健康检查

**响应**：
```json
{
  "status": "healthy",
  "timestamp": "2025-09-29T10:00:00Z"
}
```

---

#### POST /clear

清空缓存组

**查询参数**：
- `group`: 要清空的缓存组名称

**请求示例**：
```bash
curl -X POST "http://localhost:8080/clear?group=user"
```

---

## 2. gRPC API

### 2.1 CacheService

#### Get

```protobuf
rpc Get(GetRequest) returns (GetResponse);

message GetRequest {
  string key = 1;
  string group = 2;
  RequestMetadata metadata = 3;
}

message GetResponse {
  bytes value = 1;
  bool found = 2;
  CacheMetadata metadata = 3;
  ResponseStatus status = 4;
}
```

**Go 示例**：
```go
resp, err := client.Get(ctx, &cachepb.GetRequest{
    Key:   "user:123",
    Group: "user",
})
```

---

#### Set

```protobuf
rpc Set(SetRequest) returns (SetResponse);

message SetRequest {
  string key = 1;
  bytes value = 2;
  string group = 3;
  google.protobuf.Duration ttl = 4;
  RequestMetadata metadata = 5;
  SetOptions options = 6;
}
```

**Go 示例**：
```go
resp, err := client.Set(ctx, &cachepb.SetRequest{
    Key:   "user:123",
    Value: []byte("data"),
    Group: "user",
    Ttl:   durationpb.New(time.Hour),
})
```

---

#### BatchGet

```protobuf
rpc BatchGet(BatchGetRequest) returns (BatchGetResponse);
```

---

#### Stats

```protobuf
rpc Stats(StatsRequest) returns (StatsResponse);
```

---

## 3. Go SDK

### 3.1 基本用法

```go
package main

import (
    "context"
    "time"
    "turbocache/pkg/client"
)

func main() {
    // 创建客户端
    c, err := client.NewTurboCacheClient("localhost:9090")
    if err != nil {
        panic(err)
    }
    defer c.Close()

    ctx := context.Background()

    // 设置缓存
    err = c.Set(ctx, "user", "user:123", []byte("John Doe"), time.Hour)

    // 获取缓存
    value, err := c.Get(ctx, "user", "user:123")

    // 删除缓存
    err = c.Delete(ctx, "user", "user:123")
}
```

### 3.2 批量操作

```go
// 批量获取
keys := []string{"user:1", "user:2", "user:3"}
results, err := c.BatchGet(ctx, "user", keys)

for key, result := range results {
    if result.Found {
        fmt.Printf("%s: %s\n", key, result.Value)
    }
}
```

### 3.3 异步客户端

```go
// 创建异步客户端
asyncClient := client.NewAsyncClient(c, 100) // 缓冲区大小 100

// 异步设置
asyncClient.AsyncSet(ctx, "user", "user:123", []byte("data"), time.Hour,
    func(err error) {
        if err != nil {
            fmt.Printf("Async set failed: %v\n", err)
        }
    })

// 等待所有异步操作完成
asyncClient.Wait()
```

---

## 4. 错误代码

| 代码 | 说明 |
|------|------|
| 0 | 成功 |
| 1001 | 键不存在 |
| 1002 | 键已存在 |
| 1003 | 键太大 |
| 1004 | 值太大 |
| 1005 | 存储已满 |
| 1006 | 存储已关闭 |
| 1016 | 网络超时 |
| 1019 | 资源耗尽 |
| 1020 | 键已过期 |

---

## 5. 最佳实践

### 5.1 键设计

```
推荐格式: {namespace}:{entity}:{id}
示例: user:profile:123
     product:detail:456
```

### 5.2 TTL 设置

- 热数据: 5-15 分钟
- 温数据: 30 分钟 - 1 小时
- 冷数据: 2-6 小时

### 5.3 批量操作

单次批量操作建议不超过 100 个键

### 5.4 错误处理

```go
value, err := client.Get(ctx, group, key)
if err != nil {
    if errors.Is(err, storage.ErrKeyNotFound) {
        // 键不存在，从数据源加载
    } else {
        // 其他错误，记录日志
    }
}
```

---

## 6. 性能参考

| 操作 | 延迟 (P99) | 吞吐量 |
|------|-----------|--------|
| Get (L1 Hit) | <1ms | 500K ops/s |
| Get (L2 Hit) | <5ms | 100K ops/s |
| Set | <2ms | 200K ops/s |
| BatchGet (10 keys) | <10ms | 50K ops/s |

*测试环境: 4 核 8G 内存*
