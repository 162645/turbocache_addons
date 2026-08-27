#!/usr/bin/env bash
# Local test runner; does not install tools or create/delete containers.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.24.3}"
export GOWORK=off
packages=(./...)
flags=(-race)
coverage=false
for option in "$@"; do
    case "$option" in
        -u|--unit) packages=(./internal/... ./pkg/... ./test/unit/...) ;;
        -i|--integration) packages=(./test/integration/...); flags+=(-tags=integration) ;;
        -b|--benchmark) packages=(./test/benchmark/...); flags+=(-bench=. -benchmem '-run=^$') ;;
        -r|--race) ;; # Enabled by default.
        -v|--verbose) flags+=(-v) ;;
        -c|--coverage) coverage=true ;;
        -h|--help)
            echo "Usage: $0 [--unit|--integration|--benchmark] [--verbose] [--coverage]"
            echo "Default: go test -race ./...; tagged integration requires dependencies already running."
            exit 0 ;;
        *) echo "Unknown option: $option" >&2; exit 2 ;;
    esac
done
if "$coverage"; then
    mkdir -p coverage
    flags+=(-coverprofile=coverage/coverage.out -covermode=atomic)
fi
go test "${flags[@]}" "${packages[@]}"
if "$coverage"; then
    go tool cover -html=coverage/coverage.out -o coverage/index.html
fi
