# Mac local capability-cost matrix (2026-08-25)

## Fixed workload

- Host: one Apple Silicon Mac; client and all containers share the host
- Service: public `turbocached` gRPC path
- Concurrency: 100
- Duration: 10 seconds after warming 10,000 keys
- Workload: 90% reads / 10% writes
- Value: 256 bytes
- Nodes: three Docker containers, except the L1-only measurement uses one ingress
- L1 policies: node-1 LRU, node-2 LFU, node-3 ARC

## Results

| Profile | Successful throughput | P50 | P95 | P99 | Error rate |
|---|---:|---:|---:|---:|---:|
| L1-only, one ingress | 38,144.64 ops/s | 2.422 ms | 4.509 ms | 6.359 ms | 0% |
| L2 routing, RF=1 | 30,218.27 ops/s | 2.635 ms | 7.738 ms | 12.884 ms | 0% |
| L2, RF=2 eventual | 33,340.89 ops/s | 2.369 ms | 7.154 ms | 11.681 ms | 0% |
| L2, RF=2 strong | 26,292.55 ops/s | 2.931 ms | 9.413 ms | 15.757 ms | 0% |

Compared with the one-ingress L1-only public path, enabling distributed routing
with RF=1 costs 20.78% throughput. RF=2 eventual is 10.33% faster than RF=1 in
this read-heavy workload because a second replica raises the probability of a
local ingress hit while replica acknowledgement stays off the request critical
path. Waiting for RF=2 acknowledgement costs 21.14% versus eventual and 31.07%
versus L1-only.

The three-ingress L1-only diagnostic is intentionally excluded from the valid
performance matrix: it produced a 35.39% error rate because a key written to one
ingress cannot be retrieved through another without L2 routing. This is useful
correctness evidence, not a publishable throughput result.

## One-node failure experiment

The default RF=2 strong cluster was warmed with 1,000 keys, then node-2 was
stopped two seconds into a 20-second, 100-concurrency read workload. The client
kept the three original ingress addresses throughout the run.

- 718,029 requests
- 35,896.87 ops/s
- 0 failed requests / 0% final error rate
- P50/P95/P99: 2.415/5.385/8.287 ms
- Maximum latency: 107.395 ms
- Every one-second sample retained 100% success

The roughly 100 ms maximum-latency spikes show the cost of probing the failed
endpoint and retrying/cooling it down. Therefore the accurate claim is “zero
final request errors with retry/failover, with transient tail-latency spikes,”
not “failure had no impact.” The node was automatically restored after the run.

## Engineering issue found and fixed

The first eventual-consistency run exposed unbounded goroutine creation during
replication and an incorrect failure when the ingress was not itself in the
replica set. L2 eventual replication now uses a 256-task semaphore, bounded RPC
timeouts, lifecycle tracking, and accepts any non-empty replica set. The full Go
test suite and selected race tests passed after the fix.

Raw JSON is under `results/matrix/`. Re-run the four-stage matrix with:

```bash
./scripts/benchmark_matrix.sh
```

Run the failure experiment with:

```bash
RESULT_DIR=results/matrix ./scripts/resume_metrics.sh failover
```
