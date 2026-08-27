# Cluster Bloom protection

TurboCache now runs Bloom protection in the canonical `turbocached` L2 path.
Each node holds the same cluster-level bitset. Negative results are used only
while the view is `ready` and not `stale`; otherwise the read fails open to the
normal consistent-hash and replica route.

## Write and synchronization protocol

New keys are added locally and submitted to a bounded 16,384-entry update
queue. Concurrent writers are coalesced into batches of at most 100 keys or a
1 ms flush interval. A successful Set waits for every live node to acknowledge
the batch, preventing a successful write from racing a negative Bloom decision
at another ingress. Overwrites of keys already admitted by the complete Bloom
view skip propagation because they do not change any bit.

The internal protocol reuses protobuf `BatchSet` and `BatchGet` with the
reserved `X-TurboCache-Bloom-Operation` header for delta, snapshot, and local
key enumeration operations. Public batch cache operations remain unsupported.

## Bootstrap, rebuild, and deletion

- A joining node starts with `ready=false` and therefore fails open.
- It pulls a complete bitset snapshot from a ready peer.
- In a fresh in-memory cluster, the lexicographically first node bootstraps the
  empty view and peers pull it.
- The elected node periodically enumerates live L1 keys from all healthy nodes,
  builds a replacement filter off-lock, atomically swaps it, and distributes the
  snapshot. A partial rebuild is never published.
- Deletes and expirations do not clear bits. They can temporarily cause safe
  false positives and disappear at the next rebuild.

## Metrics

Prometheus and `/stats` expose readiness, stale state, version, rejections,
fail-open reads, synchronization errors, queue depth, rebuild count, bitset
capacity, and estimated false-positive rate.

## Mac measurements (2026-08-25)

With 100 concurrent requests and Negative Cache disabled to isolate Bloom:

| Workload | Bloom off | Bloom on | Interpretation |
|---|---:|---:|---|
| Unknown keys | 36.9K ops/s, P99 9.37 ms | 50.9K ops/s, P99 6.32 ms | Avoided remote miss route |
| 90/10 normal traffic | 43.3K ops/s, P99 9.05 ms | 37.2K ops/s, P99 10.47 ms | Hash checks and new-key synchronization add cost |

The measurements are short Mac single-host experiments, not universal service
limits. They show the intended trade-off: Bloom protects the cluster during
invalid-key traffic but is not a free throughput optimization for normal load.

In a separate bootstrap test, all three nodes remained `ready=false` during a
deliberate 30-second synchronization delay. About 108K missing-key requests
were recorded as fail-open and none were Bloom-rejected before readiness.

Reproduce the on/off comparison with:

```bash
./scripts/bloom_experiment.sh
```
