> 历史资料：保留原文供追溯，不代表当前部署方式或已验证能力。当前入口见 [项目首页](../../README.md)。

# TurboCache 部署指南

本指南旨在指导用户如何在不同环境下快速、稳定地部署 TurboCache 分布式缓存集群。

## 1. 前置要求

- **操作系统**: Linux, macOS, 或 Windows (推荐使用 WSL2)
- **Go 环境**: v1.24.3+
- **etcd**: v3.5+ (用于服务发现)
- **Docker & Docker Compose**: 推荐部署方式

## 2. 本地二进制部署

适合开发和快速调试。

1. **启动 etcd**:
   ```bash
   # 快速启动单节点 etcd
   docker run -d --name etcd -p 2379:2379 quay.io/coreos/etcd:v3.5.10 /usr/local/bin/etcd --advertise-client-urls http://localhost:2379 --listen-client-urls http://0.0.0.0:2379
   ```

2. **编译并启动 TurboCache**:
   ```bash
   make init
   make build
   ./bin/turbocache-server --config config/config.yaml
   ```

## 3. Docker Compose 部署 (推荐)

一键启动包含 3 个 TurboCache 节点、etcd 集群、Prometheus 和 Grafana 的完整环境。

```bash
# 在项目根目录执行
make dev
```

### 访问地址:
- **Admin UI**: `http://localhost:8080/admin` (正在开发中)
- **HTTP API**: `http://localhost:8080` (Node 1)
- **Prometheus**: `http://localhost:9093`
- **Grafana**: `http://localhost:3000` (默认账号: admin/admin)

## 4. Kubernetes 部署

生产环境的最佳选择。

### 4.1 使用 YAML 文件直接部署
```bash
kubectl apply -f deployments/k8s/etcd.yaml
kubectl apply -f deployments/k8s/turbocache.yaml
```

### 4.2 使用 Helm Chart (推荐)
```bash
# 安装 Chart
helm install my-turbocache ./deployments/helm
```

## 5. 生产环境建议

- **内存分配**: 在 `config` 中根据物理内存合理配置 `max_size`。
- **高可用**: 确保 `etcd` 集群跨可用区部署。
- **监控**: 始终启用指标导出，并配置告警规则。
- **安全**: 在公网环境务必开启 TLS 和 API 秘钥验证。

## 6. 常见问题排查

请参考 [故障排查文档](file:///f:/code_file/golong/turbocache/docs/troubleshootings.md)。
