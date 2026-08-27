# 活动监控配置

这里是 `deployments/docker-compose.yml` 实际挂载的 Prometheus/Grafana 配置。

- `prometheus/prometheus.yml`：抓取当前三个缓存节点。
- `grafana/provisioning/`：数据源与仪表盘加载配置。
- `grafana/dashboards/`：当前仪表盘。

历史告警与旧仪表盘位于 `deployments/legacy/monitoring/`，本次没有将未经验证的规则自动启用。
