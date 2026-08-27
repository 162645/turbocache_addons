> 历史资料：保留原文供追溯，不代表当前部署方式或已验证能力。当前入口见 [项目首页](../../README.md)。

# TurboCache 架构设计文档

## 1. 系统架构概览

TurboCache 采用分层架构设计，主要包含以下几个层次：

- **客户端层**：提供多语言 SDK 和 HTTP/gRPC 接口
- **接入层**：负载均衡和请求路由
- **缓存层**：L1 本地缓存 + L2 分布式缓存
- **集群层**：节点管理、服务发现、数据复制
- **存储层**：内存存储 + 可选的持久化存储
- **监控层**：指标收集、日志聚合、分布式追踪

## 2. 核心组件设计

### 2.1 多级缓存架构

```
┌─────────────────────────────────────┐
│         Application Layer            │
└──────────────┬──────────────────────┘
               │
        ┌──────▼──────┐
        │  L1 Cache   │  (本地缓存，热数据)
        │  - LRU/LFU  │
        │  - 分片锁    │
        └──────┬──────┘
               │ Miss
        ┌──────▼──────────┐
        │   L2 Cache      │  (分布式缓存)
        │  - 一致性哈希   │
        │  - 数据复制     │
        └──────┬──────────┘
               │ Miss
        ┌──────▼──────┐
        │  Data Source │
        └──────────────┘
```

**L1 本地缓存**：
- 使用分片锁减少锁竞争
- 支持 LRU、LFU、ARC 等淘汰策略
- 热切换淘汰算法
- 自动过期清理

**L2 分布式缓存**：
- 基于一致性哈希的数据分片
- 支持数据复制提高可用性
- 异步同步机制
- 节点动态扩缩容

### 2.2 一致性哈希实现

```go
// 虚拟节点分布
Hash Ring: [0 ---------- 2^32-1]
           |    |    |    |    |
           v1   v2   v3   v4   v5

// 节点映射
Node1 -> [v1, v4]
Node2 -> [v2, v5]
Node3 -> [v3]
```

**特点**：
- 150 个虚拟节点提高均衡性
- 平滑的节点增删
- 最小化数据迁移
- 支持节点权重

### 2.3 防护机制

**布隆过滤器（防穿透）**：
```
Client -> BloomFilter -> Cache -> DB
           |
           ↓ (Not Exists)
        Return Null
```

**SingleFlight（防击穿）**：
```
Request1 ─┐
Request2 ─┼─> SingleFlight -> Execute Once -> Cache
Request3 ─┘                      |
                                 ↓
                          Share Result
```

**熔断器（防雪崩）**：
```
States: Closed -> Open -> Half-Open -> Closed
         |         |         |
    Normal    Failure    Testing
```

### 2.4 服务发现架构

```
┌──────────┐     ┌──────────┐     ┌──────────┐
│  Node 1  │     │  Node 2  │     │  Node 3  │
└────┬─────┘     └────┬─────┘     └────┬─────┘
     │                │                │
     │ Register       │ Register       │ Register
     ↓                ↓                ↓
┌────────────────────────────────────────────┐
│              etcd Cluster                   │
│  /turbocache/nodes/node1                   │
│  /turbocache/nodes/node2                   │
│  /turbocache/nodes/node3                   │
└────────────────────────────────────────────┘
     │                │                │
     │ Watch          │ Watch          │ Watch
     ↓                ↓                ↓
   Updates          Updates          Updates
```

**特性**：
- 基于 etcd 的自动注册
- 心跳保活机制
- 故障节点自动剔除
- 节点变更实时通知

### 2.5 数据复制流程

```
Write Request
     │
     ↓
┌─────────────┐
│ Primary Node│
└──────┬──────┘
       │
       ├─> L1 Cache (Local)
       │
       ├─> L2 Cache (Distributed)
       │
       └─> Replicate to Replicas
           │
           ├─> Replica 1
           ├─> Replica 2
           └─> Replica N
```

**复制模式**：
- 同步复制：等待所有副本确认
- 异步复制：后台异步同步（默认）
- 最终一致性保证

## 3. 数据流详解

### 3.1 读取流程

```
1. Client Request
   ↓
2. Check L1 Cache
   ↓ (Miss)
3. Check L2 Cache via Consistent Hash
   ↓ (Miss)
4. Load from Data Source
   ↓
5. Write to L2 Cache
   ↓
6. Write to L1 Cache
   ↓
7. Return to Client
```

### 3.2 写入流程

```
1. Client Request
   ↓
2. Validate Request
   ↓
3. Write to L1 Cache
   ↓
4. Write to L2 Cache
   ↓
5. Async Replicate (if enabled)
   ↓
6. Return Success
```

## 4. 关键技术实现

### 4.1 分片锁实现

```go
type ShardedCache struct {
    shards []*Shard
    count  int
}

type Shard struct {
    mu    sync.RWMutex
    items map[string]*Entry
}

func (c *ShardedCache) getShard(key string) *Shard {
    hash := fnv32(key)
    return c.shards[hash%c.count]
}
```

**优势**：
- 减少锁竞争
- 提高并发性能
- 默认 64 个分片

### 4.2 热点检测算法

```go
// 滑动窗口计数
type HotKeyDetector struct {
    window    time.Duration
    threshold int64
    counters  sync.Map // key -> *Counter
}

type Counter struct {
    count     int64
    timestamp time.Time
}
```

**检测逻辑**：
- 1 分钟窗口内访问超过 100 次
- 标记为热点 key
- 预加载到 L1 缓存

### 4.3 逻辑过期实现

```go
type Entry struct {
    Value      ByteView
    LogicalTTL time.Time
    Metadata   *Metadata
}

// 不实际删除，只标记过期
func (e *Entry) IsLogicalExpired() bool {
    return time.Now().After(e.LogicalTTL)
}
```

**优势**：
- 避免缓存穿透
- 后台异步刷新
- 提高可用性

## 5. 性能优化

### 5.1 内存优化

- 零拷贝 ByteView
- 对象池复用
- 分代 GC 友好设计

### 5.2 网络优化

- gRPC 连接池
- Protobuf 序列化
- HTTP/2 多路复用

### 5.3 并发优化

- 细粒度锁
- Lock-free 算法
- 协程池

## 6. 监控指标

### 6.1 核心指标

- **命中率**：L1/L2 命中率
- **延迟**：P50/P95/P99
- **吞吐量**：QPS
- **错误率**：失败请求占比

### 6.2 系统指标

- **CPU 使用率**
- **内存使用量**
- **Goroutine 数量**
- **GC 暂停时间**

### 6.3 业务指标

- **热点 key 数量**
- **驱逐次数**
- **复制延迟**
- **节点健康状态**

## 7. 容错设计

### 7.1 故障场景

| 故障类型 | 影响范围 | 恢复策略 |
|---------|---------|---------|
| 单节点故障 | 部分缓存失效 | 自动剔除+重新分片 |
| 网络分区 | 集群分裂 | etcd leader 选举 |
| etcd 故障 | 无法服务发现 | 使用本地缓存的节点列表 |
| 雪崩 | 全局服务降级 | 熔断器保护 |

### 7.2 数据一致性

- **弱一致性**：L1 缓存（性能优先）
- **最终一致性**：L2 缓存（平衡性能和一致性）
- **强一致性**：可选的同步复制模式

## 8. 扩展性设计

### 8.1 水平扩展

- 动态增加节点
- 自动数据重平衡
- 最小化数据迁移

### 8.2 垂直扩展

- 可配置的内存限制
- 自适应的缓存大小
- 弹性资源分配

## 9. 安全设计

### 9.1 访问控制

- API 密钥认证
- TLS 加密传输
- 访问日志审计

### 9.2 数据安全

- 可选的数据加密
- 敏感信息脱敏
- 备份与恢复

## 10. 未来规划

### 10.1 短期目标

- [ ] 支持更多淘汰策略
- [ ] 实现数据持久化
- [ ] 优化内存使用
- [ ] 添加更多监控指标

### 10.2 长期目标

- [ ] 支持多数据中心部署
- [ ] 实现智能预加载
- [ ] 机器学习优化缓存策略
- [ ] 支持更多存储后端
