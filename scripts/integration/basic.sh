#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

COMPOSE_FILE="deployments/docker-compose.yml"

if docker compose version >/dev/null 2>&1; then
  COMPOSE=(docker compose)
elif command -v docker-compose >/dev/null 2>&1; then
  COMPOSE=(docker-compose)
else
  echo "Docker Compose is required" >&2
  exit 1
fi

echo "==> Starting cluster (docker compose)"
"${COMPOSE[@]}" -f "$COMPOSE_FILE" up -d --build

wait_http() {
  local url="$1"
  local name="$2"
  echo -n "Waiting for $name "
  for i in {1..60}; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo " OK"
      return 0
    fi
    echo -n "."
    sleep 1
  done
  echo
  echo "Timed out waiting for $name ($url)"
  return 1
}

wait_http "http://localhost:9091/healthz" "turbocache-1"
wait_http "http://localhost:9092/healthz" "turbocache-2"
wait_http "http://localhost:9093/healthz" "turbocache-3"

echo "==> Basic set/get/del"

run_ctl() {
  docker run --rm \
    --network turbocache-net \
    --entrypoint /turbocachectl \
    turbocache:local "$@"
}

run_ctl set --etcd http://etcd:2379 --ttl 30s foo bar
out=$(run_ctl get --etcd http://etcd:2379 foo | tail -n 1)
if [[ "$out" != "bar" ]]; then
  echo "unexpected get result: $out"
  exit 1
fi

run_ctl del --etcd http://etcd:2379 foo
out=$(run_ctl get --etcd http://etcd:2379 foo | tail -n 1)
if [[ "$out" != "(not found)" ]]; then
  echo "expected (not found), got: $out"
  exit 1
fi

echo "==> OK"
