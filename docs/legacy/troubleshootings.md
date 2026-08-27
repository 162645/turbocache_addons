> 历史资料：保留原文供追溯，不代表当前部署方式或已验证能力。当前入口见 [项目首页](../../README.md)。

# TurboCache 故障排查指南

## 1. 常见问题

### 1.1 服务无法启动

**症状**：服务启动失败或立即退出

**可能原因**：
1. 端口被占用
2. 配置文件错误
3. etcd 连接失败
4. 权限不足

**排查步骤**：

```bash
# 检查端口占用
lsof -i :8080
lsof -i :9090
lsof -i :2112

# 验证配置文件
./turbocache-server --config config/config.yaml --validate

# 检查 etcd 连接
etcdctl --endpoints=localhost:2379 endpoint health

# 查看日志
tail -f /var/log/turbocache/server.log
```

**解决方案**：
- 更改端口或停止占用端口的进程
- 修正配置文件语法错误
- 确保 etcd 服务正常运行
- 使用正确的用户权限启动服务

---

### 1.2 缓存命中率低

**症状**：命中率持续低于 50%

**可能原因**：
1. 缓存容量不足
2. TTL 设置过短
3. 数据访问模式不适合缓存
4. 热点数据过多导致频繁驱逐

**排查步骤**：

```bash
# 检查缓存统计
curl http://localhost:8080/stats | jq '.cache'

# 查看驱逐率
curl http://localhost:2112/metrics | grep eviction

# 检查内存使用
curl http://localhost:8080/stats | jq '.system.heap_size'
```

**解决方案**：
- 增加缓存容量配置
- 调整 TTL 策略
- 分析访问模式，考虑预加载
- 启用热点检测和保护

---

### 1.3 高延迟

**症状**：P99 延迟超过 10ms

**可能原因**：
1. 网络问题
2. GC 压力大
3. 锁竞争严重
4. 节点负载不均

**排查步骤**：

```bash
# 检查延迟分布
curl http://localhost:8080/stats | jq '.latency'

# 查看 GC
