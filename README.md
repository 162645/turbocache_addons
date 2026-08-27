# TurboCache

**Go · gRPC · etcd · 多级缓存 · 可复现实验**

基于 Go 的多级分布式缓存个人项目。当前主服务是 `cmd/turbocached`，通过 gRPC 提供缓存读写，使用 etcd 发现节点；本地验证环境为 **一台 Mac 上的三节点容器集群**，不等同于多物理机生产环境。

## 项目亮点

- **多级缓存**：分片 L1 与 L2 分布式读写链路，支持 LRU、LFU、ARC 淘汰策略。
- **分布式路由**：带权一致性哈希、虚拟节点、etcd 节点发现，以及副本读写与故障转移。
- **请求保护**：SingleFlight、负缓存、熔断和 Cluster Bloom；关注保护收益与同步开销之间的权衡。
- **可观测与验证**：Prometheus/Grafana、本地三节点部署、能力阶梯压测、Bloom 对照和节点停机实验。

这个仓库用于学习、工程实践和面试讨论，不是 Redis 的兼容替代品，也不建议直接用于生产。

## 数据链路

```text
gRPC Client
    │
    ▼
turbocached / MultiLevelCache
    ├── L1：进程内分片缓存 + LRU / LFU / ARC
    └── L2：保护检查 + 一致性哈希路由 + 副本读写
                    │
                    └── 其他 TurboCache 节点（gRPC）

etcd ── 节点发现       Prometheus / Grafana ── 指标观测
```

以上是组件关系图，不是所有操作的严格执行顺序；详细语义见 [数据链路设计](docs/design/integrated-data-path.md)。

## 快速开始

准备 Go 1.24.3、Docker/Colima 与 Docker Compose，在项目根目录执行：

先下载仓库源码并进入包含本 README 的目录；使用 Colima 时先启动本机 VM，Docker Desktop 用户可跳过这一步：

```bash
colima start --cpu 4 --memory 6
```

```bash
make help
make build
make test
make up
```

- gRPC：`127.0.0.1:7071`、`:7072`、`:7073`
- 节点健康与统计：`http://127.0.0.1:9091/healthz`、`/stats`、`/metrics`（另两节点为 9092、9093）
- Prometheus：`http://127.0.0.1:9095`
- Grafana：`http://127.0.0.1:3000`（本地演示账号 `admin/admin`，不应暴露到公网）

```bash
bin/turbocachectl set --addr 127.0.0.1:7071 --rf 1 demo hello
bin/turbocachectl get --addr 127.0.0.1:7072 demo
make down
```

`make down` 不删除命名卷。完整步骤见 [Mac 本地指南](docs/guides/macos-local.md)。

默认发布端口仅绑定 `127.0.0.1`。缓存是内存数据，停止或重建容器仍可能丢失数据；“保留卷”不代表缓存持久化。

## 实验快照

以下为历史短测样本，单台 Mac、三容器共享资源；不是当前版本的性能保证。

| 实验 | 条件 | 观测结果 |
|---|---|---|
| 双副本同步确认 | 100 并发、90/10 读写、256 B、约 10 秒 | 约 2.63 万成功 ops/s，P99 15.76 ms |
| Bloom 无效 Key 对照 | 关闭负缓存、100 并发、约 5 秒 | 处理吞吐约 3.69 万 → 5.09 万 ops/s |
| 停止一个节点 | 100 并发纯读、128 B、约 20 秒 | 约 71.8 万次读取，P99 8.29 ms |

完整条件、原始数据、失败样本及解释见 [指标汇总](docs/experiments/metrics-summary.md)。不存在的 Key 按预期返回不等于数据命中；同步确认也不等于线性一致性。

## 项目目录

```text
cmd/                    可执行入口
  turbocached/          当前主服务
  turbocachectl/         操作工具
  benchmark/            公共 gRPC 压测工具
  legacy-server/        原服务入口，保留兼容研究，不用于默认部署
  legacy-client/        原客户端入口
internal/               缓存引擎、路由、保护、传输与服务组装
pkg/                    客户端与管理 SDK
api/                    Protobuf 定义及生成代码
config/                 旧 YAML 入口配置（见目录说明）
deployments/            唯一活动 Compose 配置 + legacy 部署参考
monitoring/             当前 Compose 使用的 Prometheus/Grafana 配置
scripts/                通用命令、实验、集成验证与历史脚本
test/                   unit / integration / benchmark
docs/                   guides / design / experiments / legacy
results/                保留原始实验数据，不混入服务源码
bin/                    本机构建产物，不纳入源码管理
```

目录职责及迁移对照见 [目录规范](docs/project-layout.md)。

## 实现与验证边界

主链路包含分片 L1、LRU/LFU/ARC、L2 一致性哈希路由、SingleFlight、负缓存、熔断、复制及 Cluster Bloom。模块接入不等于完整生产保证：同步副本确认不等于线性一致性，路由重建不等于自动数据迁移。Bloom 的启动、并发重建与故障正确性也需要持续测试，不能只凭短时间压测宣称无假阴性。

仍需完善的方向包括节点重启后的数据补齐/就绪门控、扩缩容数据迁移、长期稳定性测试，以及安全接入与多物理机验证。历史入口和配置在 `legacy` 区域，不属于当前支持的部署路径。

## 实验与文档

```bash
make benchmark-matrix       # 会重建本地三节点并执行阶梯压测
make bloom-experiment       # 会切换 Bloom 配置并执行对照实验
make resume-failover        # 会停止并恢复一个项目节点
```

运行故障/压测脚本前请确认没有需要保留的内存缓存数据。历史数据是特定代码和负载下的观测值，目录调整后未重跑的结果不能当作新版本成绩。

- [文档导航](docs/README.md)
- [重要指标与数据汇总](docs/experiments/metrics-summary.md)
- [统一数据链路](docs/design/integrated-data-path.md)
- [Cluster Bloom 设计与实验](docs/design/cluster-bloom.md)
- [阶梯实验报告](docs/experiments/benchmark-matrix-macos-2026-08-25.md)
- [原始数据说明](results/README.md)

## 参与与发布

- 开发、测试与提交约定：[CONTRIBUTING.md](CONTRIBUTING.md)
- 安全边界与漏洞报告：[SECURITY.md](SECURITY.md)
- 首次公开发布检查：[GitHub 发布指南](docs/guides/github-publishing.md)
- 来源与致谢：[ACKNOWLEDGEMENTS.md](ACKNOWLEDGEMENTS.md)

当前没有随仓库提供正式 `LICENSE`；历史资料中的 MIT 标记尚需维护者确认并补齐。正式开源前请确认代码归属、第三方许可与项目许可证，不能仅凭一个徽章说明授权范围。
