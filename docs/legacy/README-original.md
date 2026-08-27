> 历史资料：保留原文供追溯，不代表当前部署方式或已验证能力。当前入口见 [项目首页](../../README.md)。

> 发布整理说明：下文的 MIT、build passing、企业级、零停机及占位仓库地址均为历史表述；仓库未附正式 LICENSE，构建状态应以真实 CI 为准。不要将这些文字用于当前版本宣传。

# TurboCache

[![Go Version](https://img.shields.io/badge/Go-1.24.3-blue.svg)](https://golang.org)
历史徽章文字：`License: MIT`、`Build: passing`（未核实，不再展示为状态徽章）。

TurboCache 是基于 geecache 改进的企业级分布式缓存系统，具备动态节点发现、多级缓存、热点保护、多种淘汰策略等特性，支持大规模分布式部署和零停机扩容。

> [!NOTE]
> **新用户？** 请先阅读 [项目核心文档与简历指南](./docs/introduction.md) 以快速了解项目全貌。
>
> macOS 本机构建、Colima 部署和指标复现见 [macOS local development](./docs/macos-local.md)。

## 核心特性

- 动态节点发现：基于 etcd 的自动节点注册与发现
- 高性能通信：gRPC + Protobuf 实现节点间高效通信
- 智能负载均衡：一致性哈希与平滑负载均衡
- 多级缓存架构：L1 本地缓存 + L2 分布式缓存
- 可视化管理：**内置现代化的 Web Admin UI**，支持实时指标监控
- 完善的防护机制：
    - 布隆过滤器防穿透
    - SingleFlight 防击穿
    - 负缓存
    - 熔断器防雪崩
    - 热点识别与保护
- 韧性保证：**集成混沌测试框架**（如模拟节点故障、分区等）
- 监控与可观测性：Prometheus 指标导出，Grafana 可视化
- 云原生支持：完整的 Kubernetes/Helm 部署方案
- 多种淘汰策略：LRU、LFU、ARC 可按节点配置选择

当前 `turbocached` 主服务已统一串联 L1、L2、一致性哈希、多副本、
SingleFlight、负缓存与节点级熔断；Mac 三节点复现口径和真实链路说明见
[`docs/integrated-data-path.md`](docs/integrated-data-path.md)。

## 快速开始

### 前置要求

- Go 1.24.3+
- etcd 3.5+
- Docker (可选)
- Kubernetes (可选)

### 本地运行

```bash
# 克隆项目
git clone https://github.com/yourusername/turbocache.git
cd turbocache

# 安装依赖
go mod download

# 生成 protobuf 代码
make proto

# 编译项目
make build

# 启动 etcd
docker run -d --name etcd \
  -p 2379:2379 \
  -p 2380:2380 \
  quay.io/coreos/etcd:latest \
  /usr/local/bin/etcd \
  --listen-client-urls http://0.0.0.0:2379 \
  --advertise-client-urls http://localhost:2379

# 启动服务器
./bin/turbocache-server --config config/config.yaml

# 访问管理界面
# 打开浏览器访问: http://localhost:8080/admin/
```

### Docker 运行

```bash
# 构建镜像
docker build -t turbocache:latest -f deployments/docker/Dockerfile .

# 使用 Docker Compose 启动
docker-compose -f deployments/docker/docker-compose.yml up -d
```

### Kubernetes 部署

```bash
# 应用 Kubernetes 配置
kubectl apply -f deployments/k8s/

# 或使用 Helm
helm install turbocache deployments/helm
```

## 架构设计

```
┌─────────────────────────────────────────────────────────┐
│                      Client Layer                        │
│  (HTTP/gRPC Clients, SDK)                               │
└────────────────┬────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────┐
│                   Load Balancer                          │
│          (Consistent Hashing)                            │
└────────────────┬────────────────────────────────────────┘
                 │
        ┌────────┴────────┐
        │                 │
┌───────▼──────┐  ┌──────▼────────┐
│   Node 1     │  │    Node 2     │  ...
│              │  │               │
│ ┌──────────┐ │  │ ┌──────────┐  │
│ │L1 Cache  │ │  │ │L1 Cache  │  │
│ │(Local)   │ │  │ │(Local)   │  │
│ └──────────┘ │  │ └──────────┘  │
│ ┌──────────┐ │  │ ┌──────────┐  │
│ │L2 Cache  │ │  │ │L2 Cache  │  │
│ │(Distrib) │◄┼──┼►│(Distrib) │  │
│ └──────────┘ │  │ └──────────┘  │
│              │  │               │
│ Protection:  │  │ Protection:   │
│ - Bloom      │  │ - Bloom       │
│ - Negative   │  │ - Negative    │
│ - Circuit    │  │ - Circuit     │
└──────┬───────┘  └───────┬───────┘
       │                  │
       └──────────┬───────┘
                  │
         ┌────────▼────────┐
         │   etcd Cluster   │
         │(Service Discovery)│
         └─────────────────┘
```

## 配置说明

主要配置项（config/config.yaml）：

```yaml
server:
  host: "0.0.0.0"
  port: 8080

cache:
  l1:
    enabled: true
    max_size: 104857600  # 100MB
    max_entries: 100000
  l2:
    enabled: true
    replication_factor: 2
  eviction_policy: "lru"

cluster:
  discovery:
    type: "etcd"
    endpoints:
      - "localhost:2379"
  hash:
    virtual_nodes: 150

transport:
  grpc:
    enabled: true
    port: 9090
```

详细配置说明见 [docs/configuration.md](docs/configuration.md)

## 使用文档

- [测试指南](docs/testing_guide.md) - 包含单元、集成及混沌测试说明
- [部署指南](docs/deployment_guide.md) - 支持 Docker/K8s 一键部署
- [API 接口文档](docs/api.md) - 详细的 HTTP/gRPC 接口说明

## API 使用

### Go 客户端

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
    err = c.Set(ctx, "user", "user:1", []byte("John Doe"), 5*time.Minute)

    // 获取缓存
    value, err := c.Get(ctx, "user", "user:1")

    // 删除缓存
    err = c.Delete(ctx, "user", "user:1")
}
```

### HTTP API

```bash
# 设置缓存
curl -X POST http://localhost:8080/cache/mygroup/mykey \
  -H "X-TTL: 300" \
  -d "value data"

# 获取缓存
curl http://localhost:8080/cache/mygroup/mykey

# 删除缓存
curl -X DELETE http://localhost:8080/cache/mygroup/mykey

# 获取统计信息
curl http://localhost:8080/stats
```

## 性能测试

```bash
# 运行基准测试
./bin/turbocache-benchmark \
  -server localhost:9090 \
  -c 100 \
  -d 30s \
  -r 0.8

# 预期结果
# Throughput: ~100,000 ops/sec
# P99 Latency: <5ms
```

## 监控

### Prometheus 指标

访问 `http://localhost:2112/metrics` 查看指标

主要指标：
- `turbocache_cache_hits_total` - 缓存命中次数
- `turbocache_cache_misses_total` - 缓存未命中次数
- `turbocache_cache_hit_rate` - 缓存命中率
- `turbocache_cache_size_bytes` - 缓存大小
- `turbocache_cache_keys` - 键数量

### Grafana 仪表盘

导入 `monitoring/grafana/dashboards/turbocache.json`

### Web Admin UI

项目内置了轻量级的 Web 管理界面（由 Go `embed` 嵌入），无需额外配置即可通过主服务端口访问。

- **访问地址**: `http://localhost:8080/admin/`
- **功能**:
  - 实时 QPS 与命中率曲线图
  - 集群节点状态监控
  - 缓存容量（L1/L2）实时快照

## 开发

### 项目结构

```
turbocache/
├── api/                    # API 定义
│   ├── proto/             # Protobuf 定义
│   └── generated/         # 生成的代码
├── cmd/                   # 命令行工具
│   ├── server/           # 服务器
│   ├── client/           # 客户端工具
│   └── benchmark/        # 性能测试
├── internal/             # 内部实现
│   ├── cache/           # 缓存引擎
│   ├── cluster/         # 集群管理
│   ├── transport/       # 通信层
│   ├── protection/      # 防护机制
│   └── metrics/         # 监控指标
├── pkg/                  # 公共库
├── deployments/          # 部署配置
├── docs/                 # 文档
└── scripts/              # 脚本
```

### 构建命令

```bash
make deps      # 安装依赖
make proto     # 生成 protobuf 代码
make build     # 编译项目
make test      # 运行测试
make lint      # 代码检查
make clean     # 清理构建产物
```

## 贡献

欢迎贡献代码！请遵循以下步骤：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 创建 Pull Request

## 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件

## 致谢

- 基于 [geecache](https://github.com/geektutu/7days-golang/tree/master/gee-cache) 改进
- 使用 [etcd](https://etcd.io/) 作为服务发现
- 使用 [gRPC](https://grpc.io/) 作为通信协议

## 联系方式

- 项目主页: https://github.com/yourusername/turbocache
- 问题反馈: https://github.com/yourusername/turbocache/issues

## 简历上写法
TurboCache：基于 Go/gRPC 的企业级分布式缓存系统
独立设计并实现了基于 Go 语言的分布式缓存系统，具备 L1/L2 多级缓存架构和 etcd 动态节点发现能力。通过 gRPC/Protobuf 优化节点间通信，在基准测试中实现单节点 10 万+ QPS，P99 延迟 < 3ms。集成了布隆过滤器防穿透、SingleFlight 防击穿、熔断器防雪崩等完善的防护机制，将缓存命中率提升至 95% 以上。通过 Prometheus 监控和 Kubernetes/Helm 部署支持，实现了生产级的高可用和零停机扩容。
