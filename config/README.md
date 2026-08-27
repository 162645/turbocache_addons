# 配置来源

`config.yaml` 属于旧 YAML 服务入口 `cmd/legacy-server`，不由当前 `turbocached` 自动读取。

当前主服务配置由 `cmd/turbocached/main.go` 的 flags/`TC_*` 环境变量提供；Mac 三节点参数集中在 `deployments/docker-compose.yml`。不要只修改该 YAML 就假定当前集群配置已生效。
