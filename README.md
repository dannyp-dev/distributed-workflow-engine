# Distributed Workflow Engine

A simulated distributed task engine written in Go. An HTTP API accepts asynchronous jobs, PostgreSQL stores durable task state, Redis Streams distributes work through a consumer group, and independently scalable worker processes coordinate with the API over gRPC.

## Architecture

```text
                         gRPC registration, heartbeats,
                         state transitions, completion
                                      |
                                      v
Client -- HTTP --> API / Coordinator -----> PostgreSQL
                     |
                     | XADD job ID
                     v
                Redis Stream
                     |
                     | XREADGROUP / XAUTOCLAIM
                     v
               Worker processes
```

The services have separate responsibilities:

- The HTTP API validates requests, creates durable PostgreSQL records, and schedules job IDs in Redis.
- Redis Streams uses a consumer group to assign each message to one worker and retain unacknowledged messages.
- Workers register and send heartbeats over gRPC, execute simulated tasks, and report success or failure.
- PostgreSQL is the source of truth for lifecycle state, attempts, worker ownership, timestamps, and the last error.
- A worker acknowledges a Redis message only after PostgreSQL records completion or permanent failure.

## Reliability behavior

Every state transition is conditional in PostgreSQL. A worker can start only a pending job, while a reclaimed Redis message may take over a job left running by a dead worker. Attempts are incremented atomically, and completion or failure is accepted only from the current owner.

Redis keeps delivered messages in its pending entries list until they are acknowledged. If a worker disappears, another worker uses `XAUTOCLAIM` after the configured idle timeout, takes ownership of the same message, and resumes the job. Failed executions return to `pending` until `max_attempts` is reached, after which the job becomes `failed`.

The project also retains a PostgreSQL `ClaimNext` example that uses a CTE with `FOR UPDATE SKIP LOCKED`. The CTE selects and locks one pending row, while the outer `UPDATE ... RETURNING` changes and returns it in the same statement. `SKIP LOCKED` lets concurrent callers move past rows another transaction already owns. The running worker path uses Redis consumer groups for distribution and PostgreSQL conditional updates for durable state.

## Repository layout

```text
cmd/api/                 HTTP API and gRPC coordinator process
cmd/worker/              Redis consumer and task executor process
cmd/benchmark/           scheduling and recovery benchmark command
gen/coordinator/v1/      generated gRPC bindings
internal/coordinator/    coordinator RPC implementation
internal/database/       PostgreSQL connection setup
internal/jobs/           job model plus memory and PostgreSQL stores
internal/queue/          Redis Streams operations
internal/worker/         worker loop, retry logic, and simulated executor
migrations/              idempotent PostgreSQL schema migrations
proto/coordinator/v1/    Protocol Buffer service definition
```

## Run the complete stack

Requirements: Docker Desktop and Docker Compose.

Copy the example environment file once and choose a local-only PostgreSQL password:

```powershell
Copy-Item .env.example .env
```

Build and start PostgreSQL, Redis, the API, and three workers:

```powershell
docker compose up -d --build --scale worker=3
docker compose ps
```

The migration service automatically applies every SQL file in `migrations/` before the API starts. PostgreSQL and Redis use named Docker volumes, so their data survives container restarts.

Submit a job from PowerShell:

```powershell
$body = @{
    type = "echo"
    payload = @{ message = "hello" }
    max_attempts = 3
} | ConvertTo-Json

$job = Invoke-RestMethod `
    -Method Post `
    -Uri "http://127.0.0.1:8080/jobs" `
    -ContentType "application/json" `
    -Body $body

Invoke-RestMethod -Uri "http://127.0.0.1:8080/jobs/$($job.id)"
```

Stop the stack without deleting durable data:

```powershell
docker compose down
```

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Check that the HTTP server is responding. |
| `POST` | `/jobs` | Create, persist, and schedule a job. |
| `GET` | `/jobs/{id}` | Read the current durable state of one job. |

Example request:

```json
{
  "type": "sleep",
  "payload": {"duration_ms": 500},
  "max_attempts": 3
}
```

`max_attempts` defaults to 3 and accepts values from 1 through 10.

## Simulated task types

| Type | Payload | Behavior |
|---|---|---|
| `noop` | `{}` | Completes immediately. |
| `echo` | `{"message":"hello"}` | Writes the message to the worker log. |
| `sleep` | `{"duration_ms":500}` | Waits for the requested time, capped at 10 seconds. |
| `fail` | `{"failures_before_success":2}` | Exercises retry handling by succeeding after two failed attempts. Omit the field to fail permanently. |

## gRPC coordination

The contract in `proto/coordinator/v1/coordinator.proto` defines worker registration, heartbeats, start, complete, and fail operations. Generated Go bindings live under `gen/coordinator/v1` and are intentionally committed so a contributor does not need Protocol Buffer tooling merely to build the project.

To regenerate the bindings after changing the contract, install Buf plus the Go plugins and run:

```powershell
buf generate
```

## Tests and checks

Unit tests do not require running infrastructure:

```powershell
go test ./...
go test -race ./...
go vet ./...
```

Run the PostgreSQL lifecycle integration test against the Compose database:

```powershell
$settings = Get-Content .env | ConvertFrom-StringData
$env:TEST_DATABASE_URL = "postgres://workflow:$($settings.POSTGRES_PASSWORD)@127.0.0.1:5432/workflow?sslmode=disable"
go test ./internal/jobs -run TestPostgresStoreSaveAndGet -v
```

## Benchmarks

With Redis running, execute both repeatable benchmarks:

```powershell
go run ./cmd/benchmark -mode all -jobs 1000 -concurrency 20
```

The scheduling benchmark measures concurrent Redis `XADD` latency and throughput. The recovery benchmark deliberately delivers messages to a simulated crashed consumer without acknowledging them, then measures how many another consumer recovers through `XAUTOCLAIM`.

See [docs/benchmarks.md](docs/benchmarks.md) for the recorded local results and methodology.

This is a simulated portfolio system, not a production workflow service. Production hardening would add authentication, authorization, TLS, an outbox for atomic database-and-queue publication, observability, resource limits, and administrative tooling.
