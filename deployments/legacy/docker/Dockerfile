# ==========================================
# 构建阶段 (Builder Stage)
# ==========================================
FROM golang:1.24.3-alpine AS builder

# 设置工作目录
WORKDIR /app

# 优化：替换 Alpine 源为阿里云，并安装构建依赖
# build-base 包含了编译所需的 gcc, g++, make 等工具
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
    apk add --no-cache git make build-base

# 2. 新增：设置国内 Go 代理环境变量 (这是解决报错的关键)
ENV GOPROXY=https://goproxy.cn,direct

# 复制依赖文件并下载
COPY go.mod go.sum ./
RUN go mod download

# 复制源代码
COPY . .

# 执行构建 (对应 Makefile 中的构建指令)
RUN make build-server

# ==========================================
# 运行阶段 (Final Stage)
# ==========================================
FROM alpine:3.19

# 优化：替换运行环境的 Alpine 源，并安装运行时依赖
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories && \
    apk add --no-cache ca-certificates tzdata

# 创建非 root 用户，增强安全性
RUN addgroup -g 1000 -S turbocache && \
    adduser -u 1000 -S turbocache -G turbocache

# 设置工作目录
WORKDIR /app

# 从构建阶段复制二进制文件
# 确保路径与 Makefile 生成的 bin 目录一致
COPY --from=builder /app/bin/turbocache-server /usr/local/bin/turbocache-server

# 复制配置文件
COPY config/config.yaml /etc/turbocache/config.yaml

# 创建数据与日志目录并授权
RUN mkdir -p /var/lib/turbocache /var/log/turbocache && \
    chown -R turbocache:turbocache /var/lib/turbocache /var/log/turbocache /app

# 切换到非 root 用户运行，遵循最小权限原则
USER turbocache

# 暴露端口 (8080: API, 9090: Metrics, 2112: Cluster)
EXPOSE 8080 9090 2112

# 改进健康检查逻辑
# 使用 -help 检查二进制是否能正常运行
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD /usr/local/bin/turbocache-server -help > /dev/null 2>&1 || exit 1

# 设置环境变量默认值
ENV TURBOCACHE_CONFIG_PATH=/etc/turbocache/config.yaml
ENV TURBOCACHE_LOG_LEVEL=info

# 启动命令
ENTRYPOINT ["turbocache-server"]
CMD ["-config", "/etc/turbocache/config.yaml"]

# 元数据标签
LABEL version="1.0.0"
LABEL description="Archived TurboCache deployment reference"
