# 部署入口

唯一活动入口：`docker-compose.yml`，由根目录 `make up` 调用。镜像由根 `Dockerfile` 构建 `cmd/turbocached`。

监控配置从 `../monitoring/` 挂载。`legacy/` 中的 Docker、Kubernetes、Helm 与旧监控文件仅为历史参考；未在本轮运行或验证，不用于默认部署。

不要修改 Compose 项目名来规避已有容器；本次保留原文件位置和网络名称。端口、账号为本地演示配置，不应直接暴露到公网。

活动配置的宿主机端口均绑定 `127.0.0.1`，只供本机访问；配置修改需重建容器后生效，不会改变已有运行容器。
