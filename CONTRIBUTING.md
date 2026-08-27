# 参与贡献

感谢你关注 TurboCache。当前维护范围是 `cmd/turbocached` 主链路、Mac 本机 Compose 部署和可复现实验；`legacy` 内容仅供历史参考。

## 开发环境

使用 Go 1.24.3。项目 Makefile 设置 `GOWORK=off`，避免父目录工作区影响依赖。Docker/Colima 只在集群实验时需要。

```bash
make build
make all
make compose-check
```

`make all` 包括格式、vet、默认测试集（含 race）与构建；不代表带 build tag 的集成/故障场景全部通过。`make integration` 会启动本地集群；压测和故障脚本可能重建或停止容器，请先阅读 [脚本说明](scripts/README.md)。

## 修改约定

- 一个 PR 解决一个明确问题，描述动机、改动范围、验证方式和已知限制。
- 修改缓存、路由、复制或并发控制时，为成功、失败、取消/超时和并发路径补充相应测试。
- 不手工修改 `api/generated`；协议变化应同步源文件与生成结果，并记录所用工具版本。
- 不把生产数据、密码、令牌、机器路径或个人信息写入源码、日志、截图及实验结果。
- 不把二进制、覆盖率产物或临时文件提交到仓库。
- 保留失败实验，报告配置和样本口径；不要仅选最快的一轮，也不要覆盖历史数据后继续沿用旧结论。
- 变更行为时同步文档。性能改善应提供相同条件下的基线与新结果，而不仅是新数字。

## 提交前

运行上面的验证命令，检查 `git diff --check` 和待提交文件清单。请确认 Git 仓库根目录是本项目，避免误提交父目录文件。提交信息简洁说明改变了什么，例如 `fix: bound replica retry timeout`。

首次发布前的许可证与仓库设置事项见 [发布指南](docs/guides/github-publishing.md)。
