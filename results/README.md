# 实验原始数据

- 根目录 JSON：早期单入口、三入口与故障实验。
- `matrix/`：2026-08-25 阶梯实验，含中间诊断结果。
- `bloom/`：Bloom 开关、优化前后和 Bootstrap Fail-Open 实验。

全部保留，不移动或覆盖旧结果。报告位于 `docs/experiments/` 和 `docs/design/cluster-bloom.md`。

重新运行现有脚本可能覆盖同名输出；保存重要历史结果后再执行。`.failover-ready` 是临时同步信号，不是实验数据。短测结果仅用于对应负载分析，不表示生产可靠性。
