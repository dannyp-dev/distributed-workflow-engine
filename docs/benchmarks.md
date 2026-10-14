# Benchmark results

Recorded on October 13, 2026 using Go 1.26.6 on Windows with Redis 7.4 running in Docker Desktop. Results vary with hardware, Docker configuration, and host load.

## Concurrent scheduling

Command:

```powershell
go run ./cmd/benchmark -mode scheduling -jobs 5000 -concurrency 10
```

Result:

```text
jobs: 5000, concurrency: 10
throughput: 13,336 jobs/second
p50: 501.5 microseconds
p95: 1.3209 milliseconds
p99: 1.6685 milliseconds
sub-millisecond: 75.44%
```

This measures the scheduling operation itself, a concurrent Redis Stream `XADD`, from the local Go client. The median scheduling latency remained below one millisecond under concurrent load.

## Worker-failure recovery

Command:

```powershell
go run ./cmd/benchmark -mode recovery -jobs 1000 -reclaim-idle 100ms
```

Result:

```text
abandoned jobs: 1000
recovered jobs: 1000/1000 (100.000%)
recovery time: 561.6503 milliseconds
```

The benchmark creates a Redis consumer group, delivers every job to a consumer that intentionally never acknowledges its messages, waits until they are reclaimable, and uses `XAUTOCLAIM` from a second consumer. Every recovered message is acknowledged after reclamation.

An additional end-to-end test stopped a real worker container during a 10-second job. A different worker reclaimed the message after the 30-second idle threshold and completed the job on attempt 2, preserving its PostgreSQL lifecycle record.
