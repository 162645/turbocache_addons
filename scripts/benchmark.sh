#!/bin/bash

# TurboCache 性能测试脚本
set -e
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# 颜色
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 配置
SERVER=${SERVER:-"localhost:7071"}
CONCURRENCY=${CONCURRENCY:-100}
DURATION=${DURATION:-30}
KEY_SIZE=${KEY_SIZE:-16}
VALUE_SIZE=${VALUE_SIZE:-1024}
READ_RATIO=${READ_RATIO:-0.8}

function info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

function warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

# 运行基准测试
function run_benchmark() {
    local test_name=$1
    local extra_args=$2

    info "Running benchmark: ${test_name}"

    ./bin/turbocache-benchmark \
        -server ${SERVER} \
        -c ${CONCURRENCY} \
        -d ${DURATION}s \
        -ks ${KEY_SIZE} \
        -vs ${VALUE_SIZE} \
        -r ${READ_RATIO} \
        ${extra_args}

    echo ""
}

info "TurboCache Performance Benchmark"
info "================================"
info "Server: ${SERVER}"
info "Concurrency: ${CONCURRENCY}"
info "Duration: ${DURATION}s"
info "Key Size: ${KEY_SIZE} bytes"
info "Value Size: ${VALUE_SIZE} bytes"
info "Read Ratio: ${READ_RATIO}"
echo ""

# 测试 1: 标准混合负载
run_benchmark "Mixed Workload (80% read)" ""

# 测试 2: 纯读负载
info "Test 2: Read-only workload"
run_benchmark "Read-only" "-r 1.0"

# 测试 3: 纯写负载
info "Test 3: Write-only workload"
run_benchmark "Write-only" "-r 0.0"

# 测试 4: 小值测试
info "Test 4: Small values (100 bytes)"
run_benchmark "Small Values" "-vs 100"

# 测试 5: 大值测试
info "Test 5: Large values (10KB)"
run_benchmark "Large Values" "-vs 10240"

# 测试 6: 高并发测试
info "Test 6: High concurrency (500 clients)"
run_benchmark "High Concurrency" "-c 500"

# 测试 7: 低并发测试
info "Test 7: Low concurrency (10 clients)"
run_benchmark "Low Concurrency" "-c 10"

info "All benchmarks completed"
info "Results printed above; use scripts/experiments for persisted experiment reports."
