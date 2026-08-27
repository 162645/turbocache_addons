# macOS benchmark report

Date: 2026-08-24

Environment: macOS arm64, Go 1.24.3, Colima/Docker, three TurboCache
containers on one physical host. These numbers validate local engineering
behaviour; they are not multi-machine or multi-AZ results.

## Reproduced results

| Scenario | Concurrency | Value | Result | P99 | Errors |
| --- | ---: | ---: | ---: | ---: | ---: |
| Single node, warmed reads, median of 3 | 100 | 128 B | 52,427 ops/s | 4.35 ms | 0% |
| Three ingress nodes, warmed reads, median of 3 | 100 | 128 B | 47,386 ops/s | 8.62 ms | 0% |
| Three ingress nodes, peak-throughput run, median of 3 | 300 | 128 B | 66,084 ops/s | 17.68 ms | 0% |
| Three ingress nodes, 80% read / 20% write | 100 | 1 KiB | 40,219 ops/s | 8.95 ms | 0% |
| One of three nodes offline during load | 100 | 128 B | 54,863 successful ops/s | 5.72 ms | 0% |

The failover run lasts 20 seconds. Node 2 is stopped two seconds after the
measurement begins and remains offline until it ends. The client quarantines a
transport-failing endpoint for two seconds, so healthy endpoints are preferred
instead of repeatedly paying the dead endpoint timeout.

Algorithm tests additionally reproduced:

- 96.98% cache hit rate under the deterministic hot/cold workload;
- 0.27 percentage-point maximum weighted hash distribution deviation;
- 14.30% key movement when the 1/7-weight node is removed (14.29% expected);
- 100 concurrent requests for one hot key collapsed into one backend call.

## Important limitation found by fault injection

A separate stop-and-restart experiment produced an 8.65% error rate because a
restarted in-memory node was immediately eligible for traffic before its data
was backfilled. The container is operationally healthy, but its cache is cold.
This result is deliberately not presented as successful recovery.

The next reliability milestone is replica backfill or readiness gating:

1. restart as `NOT_SERVING`;
2. restore or warm the node's assigned keys;
3. verify ownership and replica coverage;
4. mark the node `SERVING` and add it to the ring.

Until that is implemented, the defensible resume claim is **single-node
traffic failover while the node remains unavailable**, not transparent data
recovery after restart.

## Reproduction

Start the local cluster and run the baseline suite:

```bash
make up
make resume-metrics
```

Run the destructive local fault test (it stops one project container and
restores it after measurement):

```bash
make resume-failover
```

Machine-readable results are stored in `results/*.json`. Use at least three
runs and report the median for resume figures; do not select the fastest run.
