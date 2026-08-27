# macOS local development

This project is developed and deployed locally on macOS. Docker workloads run in
the local Colima VM; no remote deployment target is required.

## Prerequisites

- Apple Silicon or Intel Mac
- Go 1.24.3 through the official Go toolchain mechanism
- Colima and Docker CLI
- Docker Compose (`docker compose` or `docker-compose`)

Verify the toolchain:

```bash
GOTOOLCHAIN=go1.24.3 GOWORK=off go version
```

## Build and test

```bash
make build
make vet
make test
```

The Makefile pins project commands to Go 1.24.3 and disables parent `go.work`
discovery. Binaries are written to `bin/`.

## Start the local cluster

```bash
colima start --cpu 4 --memory 6
make up
```

Local endpoints:

- node gRPC: `127.0.0.1:7071`, `127.0.0.1:7072`, `127.0.0.1:7073`
- node HTTP metrics/health: `http://127.0.0.1:9091` through `:9093`
- etcd: `127.0.0.1:2379`
- Prometheus: `http://127.0.0.1:9095`
- Grafana: `http://127.0.0.1:3000` (`admin` / `admin`)

Run the three-node smoke test:

```bash
make integration
```

The cluster advertises Docker-internal node names through etcd. Therefore the
integration test runs `turbocachectl` inside `turbocache-net`. For a direct host
test, target one published node and use one replica:

```bash
bin/turbocachectl set --addr 127.0.0.1:7071 --rf 1 foo bar
bin/turbocachectl get --addr 127.0.0.1:7071 foo
```

## Resume metric reproduction

The complete reproducible suite writes JSON results and includes single-node,
three-ingress, mixed-load and algorithm metrics:

```bash
make resume-metrics
```

To stop one local project node during load and restore it afterward:

```bash
make resume-failover
```

See [benchmark-results-macos.md](../experiments/benchmark-results-macos.md) for the latest
measured results, methodology, and the restart/backfill limitation.

Algorithm metrics:

```bash
GOTOOLCHAIN=go1.24.3 GOWORK=off go test ./test/benchmark \
  -run 'TestCacheHitRate|TestProtectionMechanisms/SingleFlight|TestDistributedFeatures/ConsistentHashing' \
  -count=1 -v
```

For the single-node gRPC benchmark, start an isolated node without etcd:

```bash
bin/turbocached \
  --node-id bench-local \
  --listen 127.0.0.1:17070 \
  --advertise 127.0.0.1:17070 \
  --metrics 127.0.0.1:19090 \
  --health-check=false
```

In another terminal:

```bash
bin/turbocache-benchmark \
  -server 127.0.0.1:17070 \
  -c 100 -d 5s -r 1.0 -kr 10000 -vs 128
```

The benchmark fully warms the deterministic key range and reports actual sorted
P50/P95/P99 percentiles. Report observed errors and misses with each run; do not interpret a short run as a reliability guarantee.

## Stop

```bash
make down
colima stop
```

`make down` stops this project's containers and preserves named volumes.
