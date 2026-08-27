# 文档导航

## 当前使用

- [Mac 本地运行](guides/macos-local.md)
- [GitHub 首次发布检查](guides/github-publishing.md)
- [项目目录与迁移说明](project-layout.md)
- [统一数据链路](design/integrated-data-path.md)
- [Cluster Bloom](design/cluster-bloom.md)

## 历史实验记录

- [重要指标与数据汇总（含简历表述）](experiments/metrics-summary.md)
- [早期 Mac 压测](experiments/benchmark-results-macos.md)
- [2026-08-25 阶梯实验](experiments/benchmark-matrix-macos-2026-08-25.md)
- [原始结果目录](../results/README.md)

报告中的口径和配置以各次实验为准，不能将不同负载、时间或版本的数据直接归因为某个模块的性能提升。

## 历史设计

`legacy/` 保留原 README、早期架构/API/部署说明及项目总结。它们可能描述旧服务、未接入模块或未经验证的能力，不能作为当前启动文档。当前部署只使用根目录 Dockerfile 和 `deployments/docker-compose.yml`。
