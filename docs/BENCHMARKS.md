# Benchmarks

Reproduce with `uv run python benchmarks/bench_core.py`.
Results below come from one run on 2026-11-01: an Apple Silicon Mac on macOS 27.2, Python 3.14.6, with other work running at the same time, so treat them as indicative.

Each operation is timed one after another with no arrival rate.
These are service times, not latency under load, which adds queueing.

| Operation | p50 | p99 | p99.9 |
|---|---|---|---|
| `decimal_midpoint` | 0.62 us | 1.54 us | 8.79 us |
| `decimal_round_to_step` | 1.0 us | 2.5 us | 13.25 us |
| Construct a validated `Trade` | 2.58 us | 7.38 us | 38.29 us |
| Book delta, 1000 levels | 0.88 us | 2.17 us | 11.0 us |
| Full risk check | 8.12 us | 30.21 us | 94.88 us |
| Journal append with full flush | 4.0 ms | 6.0 ms | 9.9 ms |
| Read kill state from journal | 3.83 us | 9.96 us | 46.5 us |
| `OrderManager.submit`, end to end, simulated venue | 19.8 ms | 53.3 ms | 94.7 ms |

## What limits throughput

The bottleneck is durable journal writes.
A submit makes at least three (intent, send started, ack), and each waits for a real flush.
On this machine that means roughly 50 orders per second per order manager.
Filesystem and drive determine this far more than Python does, so measure your own hardware.
The flush is what makes crash recovery true, and turning it off would make the 4 ms figure disappear along with the guarantee.
Before `fullfsync` was enabled this measured about 45 us, because macOS `fsync()` does not flush the drive cache.

## What this is not for

This is not suitable for colocated, sub-millisecond strategies, and it does not try to be.
Group commit would raise throughput under concurrent load, and has not been built.

CI gates CPU-bound paths against generous budgets in `benchmarks/budgets.json`, to catch order-of-magnitude regressions on shared runners.
