#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/deployments/docker-compose.yml"
RESULT_DIR="$ROOT_DIR/results/matrix"
BENCH_BIN="$ROOT_DIR/bin/turbocache-benchmark"
SERVERS="127.0.0.1:7071,127.0.0.1:7072,127.0.0.1:7073"

mkdir -p "$RESULT_DIR" "$ROOT_DIR/bin"

export GOTOOLCHAIN="${GOTOOLCHAIN:-go1.24.3}"
export GOWORK=off
export GOCACHE="${GOCACHE:-/private/tmp/turbocache-go-cache}"

cd "$ROOT_DIR"
go build -o "$BENCH_BIN" ./cmd/benchmark

wait_cluster() {
  local deadline=$((SECONDS + 60))
  while (( SECONDS < deadline )); do
    if curl -fsS http://127.0.0.1:9091/healthz >/dev/null \
      && curl -fsS http://127.0.0.1:9092/healthz >/dev/null \
      && curl -fsS http://127.0.0.1:9093/healthz >/dev/null; then
      sleep 6
      return 0
    fi
    sleep 1
  done
  echo "cluster did not become ready" >&2
  return 1
}

run_profile() {
  local name="$1"
  local l2="$2"
  local rf="$3"
  local consistency="$4"
	local servers="${5:-$SERVERS}"

  echo "=== profile: $name (l2=$l2 rf=$rf consistency=$consistency) ==="
  TC_BENCH_L2_ENABLED="$l2" \
  TC_BENCH_REPLICATION_FACTOR="$rf" \
  TC_BENCH_CONSISTENCY="$consistency" \
    docker-compose -f "$COMPOSE_FILE" up -d --force-recreate \
      etcd turbocache-1 turbocache-2 turbocache-3
  wait_cluster

  "$BENCH_BIN" -servers "$servers" -c 100 -d 10s -kr 10000 \
    -vs 256 -r 0.9 -timeout 2s -output "$RESULT_DIR/$name.json"
}

run_profile "01-l1-only-single-ingress" false 1 strong "127.0.0.1:7071"
run_profile "01b-l1-only-three-ingress-diagnostic" false 1 strong
run_profile "02-l2-rf1" true 1 strong
run_profile "03-l2-rf2-eventual" true 2 eventual
run_profile "04-l2-rf2-strong" true 2 strong

# Leave the developer cluster in its documented default profile.
TC_BENCH_L2_ENABLED=true \
TC_BENCH_REPLICATION_FACTOR=2 \
TC_BENCH_CONSISTENCY=strong \
  docker-compose -f "$COMPOSE_FILE" up -d --force-recreate \
    etcd turbocache-1 turbocache-2 turbocache-3
wait_cluster

echo "matrix results: $RESULT_DIR"
