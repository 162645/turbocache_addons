#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-baseline}"
RESULT_DIR="${RESULT_DIR:-results}"
SERVERS="${SERVERS:-127.0.0.1:7071,127.0.0.1:7072,127.0.0.1:7073}"
mkdir -p "$RESULT_DIR" bin

GOCACHE="${GOCACHE:-/private/tmp/turbocache-go-cache}" \
  GOTOOLCHAIN=go1.24.3 GOWORK=off \
  go build -o bin/turbocache-benchmark ./cmd/benchmark

case "$MODE" in
  baseline)
    GOCACHE="${GOCACHE:-/private/tmp/turbocache-go-cache}" \
      GOTOOLCHAIN=go1.24.3 GOWORK=off \
      go test ./test/benchmark \
        -run 'TestCacheHitRate|TestProtectionMechanisms/SingleFlight|TestDistributedFeatures/ConsistentHashing' \
        -count=1 -v

    bin/turbocache-benchmark \
      -servers 127.0.0.1:7071 -c 100 -d 5s -r 1 -kr 10000 -vs 128 \
      -output "$RESULT_DIR/single-read.json"

    bin/turbocache-benchmark \
      -servers "$SERVERS" -c 300 -d 5s -r 1 -kr 10000 -vs 128 \
      -output "$RESULT_DIR/cluster-read.json"

    bin/turbocache-benchmark \
      -servers "$SERVERS" -c 100 -d 5s -r 0.8 -kr 10000 -vs 1024 \
      -output "$RESULT_DIR/cluster-mixed.json"
    ;;

  failover)
    command -v docker >/dev/null
    ready_file="$RESULT_DIR/.failover-ready"
    result_file="$RESULT_DIR/cluster-failover.json"
    node="${FAILOVER_NODE:-deployments-turbocache-2-1}"
    rm -f "$ready_file"

    restore_node() {
      docker start "$node" >/dev/null 2>&1 || true
    }
    trap restore_node EXIT INT TERM

    bin/turbocache-benchmark \
      -servers "$SERVERS" -c 100 -d 20s -r 1 -kr 1000 -vs 128 -timeout 500ms \
      -ready-file "$ready_file" -output "$result_file" &
    benchmark_pid=$!

    for _ in {1..120}; do
      [[ -f "$ready_file" ]] && break
      kill -0 "$benchmark_pid" 2>/dev/null || break
      sleep 1
    done
    [[ -f "$ready_file" ]] || { echo "benchmark did not become ready" >&2; exit 1; }

    sleep 2
    docker stop "$node"
    wait "$benchmark_pid"
    # Restore only after measurement. A restarted in-memory node is empty until
    # replication/backfill exists; mixing that separate problem into this test
    # would not measure failover availability cleanly.
    docker start "$node"
    trap - EXIT INT TERM
    ;;

  *)
    echo "usage: $0 [baseline|failover]" >&2
    exit 2
    ;;
esac
