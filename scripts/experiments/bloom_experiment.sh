#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployments/docker-compose.yml"
RESULT_DIR="$ROOT_DIR/results/bloom"
BENCH_BIN="$ROOT_DIR/bin/turbocache-benchmark"
SERVERS="127.0.0.1:7071,127.0.0.1:7072,127.0.0.1:7073"

mkdir -p "$RESULT_DIR" "$ROOT_DIR/bin"
cd "$ROOT_DIR"
GOTOOLCHAIN="${GOTOOLCHAIN:-go1.24.3}" GOWORK=off \
GOCACHE="${GOCACHE:-/private/tmp/turbocache-go-cache}" \
  go build -o "$BENCH_BIN" ./cmd/benchmark

wait_cluster() {
  for _ in {1..60}; do
    if curl -fsS http://127.0.0.1:9091/healthz >/dev/null \
      && curl -fsS http://127.0.0.1:9092/healthz >/dev/null \
      && curl -fsS http://127.0.0.1:9093/healthz >/dev/null; then
      sleep 6
      return 0
    fi
    sleep 1
  done
  return 1
}

start_profile() {
  local enabled="$1"
  TC_BENCH_BLOOM_ENABLED="$enabled" \
  TC_BENCH_NEGATIVE_CACHE_ENABLED=false \
    docker-compose -f "$COMPOSE_FILE" up -d --force-recreate \
      etcd turbocache-1 turbocache-2 turbocache-3
  wait_cluster
}

run_profile() {
  local label="$1"
  local enabled="$2"
  start_profile "$enabled"
  "$BENCH_BIN" -servers "$SERVERS" -c 100 -d 5s -kr 10000 -vs 256 \
    -missing-only -output "$RESULT_DIR/$label-missing.json"
  "$BENCH_BIN" -servers "$SERVERS" -c 100 -d 5s -kr 10000 -vs 256 -r 0.9 \
    -output "$RESULT_DIR/$label-normal.json"
  for port in 9091 9092 9093; do
    curl -fsS "http://127.0.0.1:${port}/stats"
  done >"$RESULT_DIR/$label-stats.jsonl"
}

run_profile bloom-on true
run_profile bloom-off false

TC_BENCH_BLOOM_ENABLED=true \
TC_BENCH_NEGATIVE_CACHE_ENABLED=true \
  docker-compose -f "$COMPOSE_FILE" up -d --force-recreate \
    etcd turbocache-1 turbocache-2 turbocache-3
wait_cluster

echo "Bloom experiment results: $RESULT_DIR"
