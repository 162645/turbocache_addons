# Unified production data path

The `turbocached` process is the canonical service entry point. Its public RPCs
are composed as follows:

```text
gRPC CacheService
  -> MultiLevelCache (write-through)
     -> local L1 (sharded memory + LRU/LFU/ARC)
     -> distributed L2
        -> weighted consistent-hash routing
        -> per-key SingleFlight
        -> per-peer circuit breaker
        -> negative cache
        -> synchronous or eventual replica writes
        -> pooled gRPC connections
```

Public writes enter `MultiLevelCache` once. Replica RPCs carry
`X-TurboCache-Internal: replica`, which makes the receiving node mutate only
its local L1 and prevents recursive replication storms. Public reads first use
the ingress L1, then L2 routes to the primary replica and promotes a remote hit
back into the ingress L1.

## Mac three-node profile

`deployments/docker-compose.yml` enables L2, replication factor 2, strong
replication, SingleFlight, negative caching, and circuit breakers. To exercise
all implemented replacement algorithms in one local cluster, node 1 uses LRU,
node 2 LFU, and node 3 ARC. `/stats` reports both the selected eviction policy
and all L2 protection/replication counters.

Bloom filtering is enabled as a cluster-level, versioned view with batched
all-node updates, snapshot bootstrap/rebuild, and fail-open behavior while the
view is incomplete. See [`cluster-bloom.md`](cluster-bloom.md) for the
correctness protocol and Mac measurements.

## Reproduction

```bash
docker-compose -f deployments/docker-compose.yml up -d --build
curl http://127.0.0.1:9091/stats
curl http://127.0.0.1:9092/stats
curl http://127.0.0.1:9093/stats
go run ./cmd/benchmark \
  -servers 127.0.0.1:7071,127.0.0.1:7072,127.0.0.1:7073 \
  -c 100 -d 10s -kr 10000 -vs 256 -r 0.9
```

On 2026-08-24, the integrated profile completed 318,129 operations at
31,802.79 ops/s with 0 errors; P50/P95/P99 were 2.424/7.794/12.639 ms. This is
an end-to-end distributed result and must not be compared directly with the
pure in-process L1 benchmark.

The controlled capability-cost matrix and node-failure experiment are recorded
in [`benchmark-matrix-macos-2026-08-25.md`](../experiments/benchmark-matrix-macos-2026-08-25.md).
