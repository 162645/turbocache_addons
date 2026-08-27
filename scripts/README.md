# 脚本入口

优先从项目根目录运行 `make help`。构建、测试、部署由 Makefile 定义，脚本只负责转发或实验编排。

- `experiments/`：阶梯压测、Bloom 对照、故障实验。
- `integration/`：本机三节点 smoke test。
- `legacy/`：旧构建/部署脚本，保留追溯，不用于当前服务。
- 根目录同名实验脚本：兼容历史命令，转发到 `experiments/`。
- `test.sh`：本地测试，可选 unit / integration / benchmark；不自动安装 linter，也不创建或删除 etcd 容器。旧流程归档在 `legacy/test.sh`。

实验会重建容器或注入节点停止，不是只读检查。请先阅读脚本与结果说明。
