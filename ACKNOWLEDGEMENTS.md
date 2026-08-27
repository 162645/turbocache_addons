# 来源与致谢

历史 README 记载，TurboCache 的早期实现基于 [GeeCache / 7days-golang](https://github.com/geektutu/7days-golang/tree/master/gee-cache) 改进。这里保留该来源说明，不将项目表述为所有算法和组件均从零原创。

项目使用 Go 生态组件，包括 gRPC、Protobuf、etcd、Prometheus、xxhash、bitset 与 `golang.org/x/sync` 等；准确依赖与版本以 [go.mod](go.mod) 和 [go.sum](go.sum) 为准。各依赖遵循其自身许可证。

发布前仍需维护者核实实际借鉴/复制范围，保留适用的原始版权、许可证与 NOTICE；本文件不替代第三方许可文件，也不代表已经完成代码来源审计。
