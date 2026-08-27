# 首次发布到 GitHub

本指南用于把本项目作为独立仓库发布，不自动创建远程仓库、不推送代码、不改写已有历史。

## 1. 先确认 Git 边界

进入本项目根目录（包含 `go.mod`、`README.md` 和 `cmd/turbocached`）后执行：

```bash
pwd
git rev-parse --show-toplevel
```

如果第二条指向父目录或用户主目录，说明当前项目还不是独立 Git 仓库。**不要直接执行 `git add .`、`git add -A` 或推送父仓库。**

本次整理时，项目文件不在独立仓库中；因此没有自动提交或推送。确认当前目录确实是项目目录，再自行初始化独立仓库：

```bash
git init -b main .
git rev-parse --show-toplevel
git status --short
```

确认 Git 根目录与项目目录一致后，才进行后续操作。如果已经存在独立仓库，跳过初始化。

## 2. 许可证与代码归属（需维护者决定）

- 当前没有正式 `LICENSE` 文件；历史 README 曾标记 MIT，但本次没有仅凭徽章替维护者补写授权主体。
- 早期实现的 GeeCache 来源已保留在 [致谢](../../ACKNOWLEDGEMENTS.md)。需核实实际借鉴范围和适用的上游许可。
- 发布前确认代码、协议、文档、截图和数据的来源与使用权限；有第三方版权声明时保留原声明。
- 确定许可证后添加根目录 `LICENSE`，填写真实权利主体，并更新 README 的许可说明。
- 不要因为仓库公开可见，就把“公开项目”和“已经开源授权”混为一谈。

仓库名、所属 GitHub 账号也未指定，因此不填写虚构 remote、CI 徽章或克隆地址。确定地址后可补充真实的 clone 命令与 CI 链接。

## 3. 发布内容

应保留：源码、`go.mod`/`go.sum`、Protobuf 源文件与生成结果、测试、当前部署与监控配置、复现实验脚本、文档和脱敏后的历史 JSON。

不应提交：`bin/`、覆盖率、日志、`.env`、私钥、令牌、数据库转储、真实业务数据、机器截图中的个人信息、IDE 临时文件。`.gitignore` 已覆盖常见产物，但忽略规则不是密钥检测或历史清理工具，也不会移除已经被跟踪的文件。

`legacy` 中保留历史实现，README 已明确非维护入口。没有将旧文档描述的能力当作当前功能保证。

`go.mod` 仍为 `module turbocache`，支持克隆后本地构建。若以后希望用户通过 `go get` / `go install` 按 GitHub 地址使用，需要在确定仓库地址后统一更改 module 与内部 import，再执行完整验证；本次没有猜测仓库地址。

## 4. 本地验收

```bash
make all
make compose-check
```

如需端到端验收，可执行 `make integration`，但它会操作本地项目容器。不要把默认测试集通过写成全部 tagged 集成测试已通过，也不要把 CI 配置已经添加写成 GitHub Actions 已经成功执行。

发布前检查待提交文件和大文件；完成暂存后检查：

```bash
git diff --cached --stat
git diff --cached --check
git diff --cached
```

确认没有 `bin/`、敏感数据或不相关文件后再提交。若已有历史，请另外扫描完整历史中的敏感信息；本次仅检查当前项目文件，不代表父仓库或历史提交安全。

## 5. GitHub 仓库设置

- 创建空仓库后，使用该仓库页面显示的真实地址配置 remote；不要推送用户主目录所属仓库。
- Description 可用：`A Go distributed cache with multi-level caching, replication, Bloom protection, and reproducible single-host experiments.`
- Topics 可用：`golang`、`grpc`、`distributed-cache`、`etcd`、`consistent-hashing`、`bloom-filter`、`performance-testing`。
- 确认 Actions 权限并观察首轮 CI；配置主分支保护和 PR 检查。
- 启用依赖安全告警、可用的密钥扫描与私密漏洞报告。Dependabot 配置只负责依赖更新建议，不代表依赖没有漏洞。
- 不要发布虚构测试徽章、覆盖率、Stars 或生产级性能标签。

## 6. 已完成与尚需人工确认

已整理：公开首页、能力与限制、原始数据导航、贡献/安全说明、Issue/PR 模板、最小权限 CI、依赖更新配置、文件忽略规则、回环地址端口绑定。

仍需维护者确认：许可证/权利主体、真实 GitHub 账号与仓库名、代码与数据公开权限，以及首次 GitHub CI 结果。此次没有发布 GitHub Release、上传文件、初始化仓库或修改父目录 Git 状态。

默认端口改为 `127.0.0.1` 后，需要下一次重建相关容器才会生效；不会自动更改正在运行的旧容器端口绑定。缓存是内存数据，重建前请先确认可丢弃。
