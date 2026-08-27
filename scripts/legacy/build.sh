#!/bin/bash

# TurboCache 构建脚本
set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 项目信息
PROJECT_NAME="turbocache"
VERSION=${VERSION:-"1.0.0"}
BUILD_TIME=$(date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "unknown")
GO_VERSION=$(go version | awk '{print $3}')

# 输出目录
OUTPUT_DIR="bin"
PROTO_OUTPUT_DIR="api/generated"

# 打印信息
function info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

function error() {
    echo -e "${RED}[ERROR]${NC} $1"
    exit 1
}

# 检查依赖
function check_dependencies() {
    info "Checking dependencies..."

    # 检查 Go
    if ! command -v go &> /dev/null; then
        error "Go is not installed"
    fi
    info "Go version: ${GO_VERSION}"

    # 检查 protoc
    if ! command -v protoc &> /dev/null; then
        warn "protoc is not installed, skipping protobuf generation"
        SKIP_PROTO=true
    else
        info "protoc version: $(protoc --version)"
    fi
}

# 生成 protobuf 代码
function generate_proto() {
    if [ "$SKIP_PROTO" = true ]; then
        warn "Skipping protobuf generation"
        return
    fi

    info "Generating protobuf code..."

    # 创建输出目录
    mkdir -p "${PROTO_OUTPUT_DIR}/cachepb"
    mkdir -p "${PROTO_OUTPUT_DIR}/discoverypb"
    mkdir -p "${PROTO_OUTPUT_DIR}/metricspb"

    # 生成 Go 代码
    protoc --go_out="${PROTO_OUTPUT_DIR}/cachepb" \
           --go_opt=paths=source_relative \
           --go-grpc_out="${PROTO_OUTPUT_DIR}/cachepb" \
           --go-grpc_opt=paths=source_relative \
           api/proto/cache.proto || warn "Failed to generate cache.proto"

    protoc --go_out="${PROTO_OUTPUT_DIR}/discoverypb" \
           --go_opt=paths=source_relative \
           --go-grpc_out="${PROTO_OUTPUT_DIR}/discoverypb" \
           --go-grpc_opt=paths=source_relative \
           api/proto/discovery.proto || warn "Failed to generate discovery.proto"

    protoc --go_out="${PROTO_OUTPUT_DIR}/metricspb" \
           --go_opt=paths=source_relative \
           --go-grpc_out="${PROTO_OUTPUT_DIR}/metricspb" \
           --go-grpc_opt=paths=source_relative \
           api/proto/metrics.proto || warn "Failed to generate metrics.proto"

    info "Protobuf code generated successfully"
}

# 编译项目
function build() {
    info "Building ${PROJECT_NAME}..."

    # 创建输出目录
    mkdir -p "${OUTPUT_DIR}"

    # 构建标志
    LDFLAGS="-X main.Version=${VERSION} \
             -X main.BuildTime=${BUILD_TIME} \
             -X main.GitCommit=${GIT_COMMIT} \
             -X main.GoVersion=${GO_VERSION}"

    # 构建服务器
    info "Building server..."
    go build -ldflags "${LDFLAGS}" -o "${OUTPUT_DIR}/${PROJECT_NAME}-server" ./cmd/server

    # 构建客户端工具
    info "Building client..."
    go build -ldflags "${LDFLAGS}" -o "${OUTPUT_DIR}/${PROJECT_NAME}-client" ./cmd/client

    # 构建性能测试工具
    info "Building benchmark..."
    go build -ldflags "${LDFLAGS}" -o "${OUTPUT_DIR}/${PROJECT_NAME}-benchmark" ./cmd/benchmark

    info "Build completed successfully"
    info "Binaries are in ${OUTPUT_DIR}/"
}

# 运行测试
function test() {
    info "Running tests..."
    go test -v -race -coverprofile=coverage.out ./...
    info "Tests completed"
}

# 清理
function clean() {
    info "Cleaning build artifacts..."
    rm -rf "${OUTPUT_DIR}"
    rm -f coverage.out
    info "Clean completed"
}

# 安装依赖
function install_deps() {
    info "Installing dependencies..."
    go mod download
    go mod tidy
    info "Dependencies installed"
}

# 代码检查
function lint() {
    info "Running linters..."

    if ! command -v golangci-lint &> /dev/null; then
        warn "golangci-lint is not installed, skipping lint"
        return
    fi

    golangci-lint run ./...
    info "Lint completed"
}

# 格式化代码
function fmt() {
    info "Formatting code..."
    go fmt ./...
    gofmt -s -w .
    info "Format completed"
}

# 显示帮助
function show_help() {
    cat << EOF
TurboCache Build Script

Usage: $0 [command]

Commands:
    build           Build the project (default)
    proto           Generate protobuf code
    test            Run tests
    clean           Clean build artifacts
    deps            Install dependencies
    lint            Run linters
    fmt             Format code
    all             Run proto, deps, fmt, lint, test, and build
    help            Show this help message

Environment Variables:
    VERSION         Set build version (default: 1.0.0)

Examples:
    $0 build
    VERSION=2.0.0 $0 build
    $0 all

EOF
}

# 主函数
function main() {
    check_dependencies

    case "${1:-build}" in
        build)
            build
            ;;
        proto)
            generate_proto
            ;;
        test)
            test
            ;;
        clean)
            clean
            ;;
        deps)
            install_deps
            ;;
        lint)
            lint
            ;;
        fmt)
            fmt
            ;;
        all)
            generate_proto
            install_deps
            fmt
            lint
            test
            build
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            error "Unknown command: $1. Use 'help' for usage information."
            ;;
    esac
}

main "$@"
