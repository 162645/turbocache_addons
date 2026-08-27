> 历史资料：保留原文供追溯，不代表当前部署方式或已验证能力。当前入口见 [项目首页](../../README.md)。

# TurboCache 测试指南

本指南详细介绍了如何对 TurboCache 系统进行全面的测试，涵盖单元测试、集成测试、性能基准测试以及混沌测试。

## 1. 单元测试 (Unit Testing)

单元测试用于验证单个组件（如缓存引擎、一致性哈希、服务发现等）的逻辑正确性。

### 1.1 运行所有单元测试
```bash
# 使用 Makefile 运行
make test

# 或者直接使用 go test
go test -v -race ./internal/... ./pkg/...
```

### 1.2 运行特定模块的测试
```bash
# 仅测试缓存引擎
go test -v ./internal/cache/engine/...

# 仅测试一致性哈希
go test -v ./internal/cluster/hash/...
```

---

## 2. 集成测试 (Integration Testing)

集成测试验证多个组件（如 Etcd、gRPC Server、Multi-Level Cache）之间的协同工作。

### 2.1 准备环境
集成测试依赖 **Etcd**。你可以使用 Docker 快速启动：
```bash
# 启动开发/测试依赖环境
make dev
```

### 2.2 运行集成测试
```bash
# 运行所有集成测试
./scripts/test.sh -i

# 手动执行（需要指定 integration 标签）
go test -v -tags=integration ./test/integration/...
```

---

## 3. 性能测试 (Benchmarking)

性能测试用于评估系统的吞吐量 (QPS) 和延迟 (Latency)。

### 3.1 运行 Go 内置基准测试
```bash
make benchmark
```

### 3.2 使用专业基准测试工具
我们提供了一个专门的压力测试脚本，模拟真实生产负载：
```bash
# 1. 编译压测工具
make build-benchmark

# 2. 运行多场景压测
# 包含：标准混合负载、纯读、纯写、高并发、大/小 Value 测试
./scripts/benchmark.sh
```

---

## 4. 混沌测试 (Chaos Testing)

混沌测试模拟分布式系统中的异常情况，验证系统的韧性（Resiliency）。

### 4.1 模拟节点故障
该测试会启动 3 个节点，模拟其中一个节点突然下线，并验证剩余节点是否能继续正常提供服务。
```bash
# 运行混沌测试
go test -v -tags=integration ./test/integration/chaos_test.go
```

---

## 5. Web 管理面板验证 (Admin UI)

TurboCache 提供了一个直观的 Web UI 来查看集群状态。

### 5.1 验证步骤
1. 启动服务器：`make run-server`
2. 打开浏览器访问：`http://localhost:8080`
3. 检查：
   - 节点列表是否正确显示。
   - L1/L2 缓存的命中率指标。
   - 节点流量图表。

---

## 6. 代码覆盖率 (Code Coverage)

```bash
# 生成覆盖率报告
make coverage

# 命令执行后会生成 coverage.html，可用浏览器打开查看详细的覆盖情况。
```

## 常见问题
如果集成测试报错 `connection refused`，请确保 Etcd 已经在 `localhost:2379` 启动。
可以使用 `docker ps` 检查 `turbocache-etcd` 容器的状态。
