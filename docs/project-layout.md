# 目录规范与本次迁移

## 边界

- `cmd/turbocached`：当前服务启动；依赖 `internal/server` 组装主链路。
- `cmd/turbocachectl`、`cmd/benchmark`：管理和测量工具。
- `cmd/legacy-*`：旧入口仍可编译，但默认构建不选择它们。
- `internal/cache`：本地存储、淘汰及多级引擎；`internal/cluster`：发现、哈希与复制组件；`internal/protection`：保护算法；`internal/transport`：传输适配。
- `pkg`：对外 SDK。保留现有 Go import 路径，不为目录美观打断调用方。
- `api/proto` 和 `api/generated`：协议源文件与生成产物。本次不修改 IDL，也不重新生成协议。
- `test/unit`：集中单元测试；`test/integration`：集成测试（部分带 build tag）；`test/benchmark`：算法实验。默认 `go test ./...` 不代表所有 tagged 集成场景都已执行。
- `results`：原始证据；`docs/experiments`：对应解释；`scripts/experiments`：复现方式。

## 迁移对照

| 原路径 | 当前路径 |
|---|---|
| `cmd/server`、`cmd/client` | `cmd/legacy-server`、`cmd/legacy-client` |
| `deployments/prometheus`、`deployments/grafana` | `monitoring/prometheus`、`monitoring/grafana` |
| 旧 `monitoring` | `deployments/legacy/monitoring` |
| `deployments/docker`、`k8s`、`helm` | `deployments/legacy/docker`、`k8s`、`helm` |
| `scripts/benchmark_matrix.sh`、`bloom_experiment.sh`、`resume_metrics.sh` | `scripts/experiments/` 同名文件；旧路径是转发脚本 |
| 旧 `scripts/build.sh`、`deploy.sh` | `scripts/legacy/` 保留原文；原位置改为当前 Makefile 入口 |
| 旧 `scripts/test.sh` | `scripts/legacy/test.sh`；原位置改为无容器隐式操作的测试入口 |
| `docs/macos-local.md` | `docs/guides/macos-local.md` |
| 已验证设计、实验说明 | `docs/design/`、`docs/experiments/` |
| 旧 README/设计/API/部署文档 | `docs/legacy/` |

## 保持不变

Compose 文件留在 `deployments/docker-compose.yml`，避免因目录名变化生成另一套 Compose project、容器和网络。根 Dockerfile、`bin/` 二进制命名、`results/` 原始 JSON 与 Go module/import 路径保持稳定。仅活动监控配置的挂载源改为 `monitoring/`。

历史配置仅供追溯，不承诺从 legacy 目录直接运行；需要使用前先按当前入口重新适配。部署脚本默认只面向本机 Docker，不再默认执行 Kubernetes 或远程推送。
