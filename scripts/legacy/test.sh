#!/bin/bash
# Historical workflow only: paths and external dependencies need adaptation.

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

echo -e "${GREEN}=== TurboCache Test Suite ===${NC}"
echo "Project Root: ${PROJECT_ROOT}"
echo

# 解析参数
TEST_TYPE="all"
COVERAGE=false
VERBOSE=false
RACE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -u|--unit)
            TEST_TYPE="unit"
            shift
            ;;
        -i|--integration)
            TEST_TYPE="integration"
            shift
            ;;
        -b|--benchmark)
            TEST_TYPE="benchmark"
            shift
            ;;
        -c|--coverage)
            COVERAGE=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -r|--race)
            RACE=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            exit 1
            ;;
    esac
done

# 构建测试标志
TEST_FLAGS="-timeout 30m"
if [ "$VERBOSE" = true ]; then
    TEST_FLAGS="$TEST_FLAGS -v"
fi
if [ "$RACE" = true ]; then
    TEST_FLAGS="$TEST_FLAGS -race"
fi
if [ "$COVERAGE" = true ]; then
    TEST_FLAGS="$TEST_FLAGS -coverprofile=coverage.out -covermode=atomic"
fi

# 运行单元测试
run_unit_tests() {
    echo -e "${BLUE}=== Running Unit Tests ===${NC}"
    go test $TEST_FLAGS ./internal/... ./pkg/... 2>&1 | tee test_unit.log

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓ Unit tests passed${NC}"
        return 0
    else
        echo -e "${RED}✗ Unit tests failed${NC}"
        return 1
    fi
}

# 运行集成测试
run_integration_tests() {
    echo -e "${BLUE}=== Running Integration Tests ===${NC}"

    # 检查依赖服务
    check_etcd

    go test $TEST_FLAGS -tags=integration ./test/integration/... 2>&1 | tee test_integration.log

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓ Integration tests passed${NC}"
        return 0
    else
        echo -e "${RED}✗ Integration tests failed${NC}"
        return 1
    fi
}

# 运行性能测试
run_benchmark_tests() {
    echo -e "${BLUE}=== Running Benchmark Tests ===${NC}"
    go test -bench=. -benchmem -run=^$ ./test/benchmark/... 2>&1 | tee test_benchmark.log

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓ Benchmark tests completed${NC}"
        return 0
    else
        echo -e "${RED}✗ Benchmark tests failed${NC}"
        return 1
    fi
}

# 检查etcd是否运行
check_etcd() {
    if ! nc -z localhost 2379 2>/dev/null; then
        echo -e "${YELLOW}Warning: etcd is not running on localhost:2379${NC}"
        echo "Starting etcd with docker..."
        docker run -d --name turbocache-etcd \
            -p 2379:2379 \
            -p 2380:2380 \
            -e ALLOW_NONE_AUTHENTICATION=yes \
            bitnami/etcd:latest

        echo "Waiting for etcd to be ready..."
        sleep 5
    fi
}

# 生成覆盖率报告
generate_coverage_report() {
    if [ -f coverage.out ]; then
        echo -e "${BLUE}=== Generating Coverage Report ===${NC}"
        go tool cover -html=coverage.out -o coverage.html
        go tool cover -func=coverage.out | tail -n 1
        echo -e "${GREEN}Coverage report generated: coverage.html${NC}"
    fi
}

# 运行代码质量检查
run_linter() {
    echo -e "${BLUE}=== Running Linter ===${NC}"

    if ! command -v golangci-lint &> /dev/null; then
        echo -e "${YELLOW}golangci-lint not found, installing...${NC}"
        go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
    fi

    golangci-lint run ./... 2>&1 | tee lint.log

    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo -e "${GREEN}✓ Linter passed${NC}"
        return 0
    else
        echo -e "${RED}✗ Linter found issues${NC}"
        return 1
    fi
}

# 主测试流程
main() {
    local exit_code=0

    case $TEST_TYPE in
        unit)
            run_unit_tests || exit_code=$?
            ;;
        integration)
            run_integration_tests || exit_code=$?
            ;;
        benchmark)
            run_benchmark_tests || exit_code=$?
            ;;
        all)
            run_unit_tests || exit_code=$?
            echo
            run_integration_tests || exit_code=$?
            echo
            run_benchmark_tests || exit_code=$?
            ;;
    esac

    # 生成覆盖率报告
    if [ "$COVERAGE" = true ]; then
        echo
        generate_coverage_report
    fi

    # 运行linter
    echo
    run_linter || exit_code=$?

    echo
    if [ $exit_code -eq 0 ]; then
        echo -e "${GREEN}=== All tests passed ===${NC}"
    else
        echo -e "${RED}=== Some tests failed ===${NC}"
    fi

    return $exit_code
}

# 清理函数
cleanup() {
    if docker ps -a | grep -q turbocache-etcd; then
        echo -e "${YELLOW}Cleaning up test containers...${NC}"
        docker stop turbocache-etcd 2>/dev/null || true
        docker rm turbocache-etcd 2>/dev/null || true
    fi
}

# 设置清理trap
trap cleanup EXIT

# 执行主函数
main
exit $?
