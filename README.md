# Distributed Workflow Engine

A learning project for building a small distributed job system in Go. The API can create and retrieve jobs, and PostgreSQL provides durable storage for the running server.

## The eventual architecture

```text
Client
  |
  | HTTP requests
  v
API / coordinator  --->  job storage
                              ^
                              |
                         worker process
```

The two programs are deliberately separate:

- `cmd/api` will accept jobs and report their status.
- `cmd/worker` will claim and execute jobs.

Keeping them separate will eventually let you run one API process and several workers. For the first milestone, focus only on the API and keep jobs in memory.

## Repository layout

```text
.
|-- cmd/
|   |-- api/          # HTTP API executable
|   `-- worker/       # Worker executable
|-- internal/
|   |-- jobs/         # Job model and state transitions
|   |-- database/     # PostgreSQL connection setup
|   `-- worker/       # Future worker behavior
|-- migrations/       # Future database migrations
|-- .vscode/          # Go extension and editor recommendations
|-- go.mod
`-- README.md
```

## Getting started in VS Code

1. Install [Go](https://go.dev/dl/) if it is not already installed.
2. Open this repository folder in VS Code.
3. Accept the recommendation to install the official Go extension.
4. Open the integrated terminal and verify the scaffold:

   ```sh
   go build ./...
   go test ./...
   ```

5. Run either placeholder:

   ```sh
   go run ./cmd/api
   go run ./cmd/worker
   ```

## Local PostgreSQL setup

Copy `.env.example` to `.env`, choose a local development password, and start PostgreSQL:

```powershell
Copy-Item .env.example .env
docker compose up -d postgres
docker compose exec postgres pg_isready -U workflow -d workflow
```

Apply the database migration:

```powershell
Get-Content migrations/001_create_jobs.sql | docker compose exec -T postgres psql -U workflow -d workflow
```

Set `DATABASE_URL` in the terminal that will run the API. Replace `YOUR_PASSWORD` with the password from `.env`:

```powershell
$env:DATABASE_URL = "postgres://workflow:YOUR_PASSWORD@127.0.0.1:5432/workflow?sslmode=disable"
go run ./cmd/api
```

The real API uses `PostgresStore`. Unit tests use the in-memory `Store`, so regular tests do not require PostgreSQL.

## Current API

The API exposes these endpoints:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Return a simple success response so you know the server is running. |
| `POST` | `/jobs` | Validate a request, create a job, store it in PostgreSQL, and return it. |
| `GET` | `/jobs/{id}` | Look up one stored job by ID or return `404`. |

A useful first job shape is:

```json
{
  "id": "generated-id",
  "type": "send_email",
  "payload": {"to": "learner@example.com"},
  "status": "pending",
  "created_at": "2026-01-01T12:00:00Z"
}
```

The first milestone was completed in this order:

1. Define `Job` and its status values in `internal/jobs`.
2. Start an HTTP server using Go's standard `net/http` package.
3. Add `GET /health`.
4. Add a small in-memory store, such as a map protected by a mutex.
5. Add `POST /jobs`, then `GET /jobs/{id}`.
6. Write handler and store tests with `net/http/httptest`.

Jobs created by the running API persist across API restarts because they are stored in PostgreSQL.

## Tests

Run the unit tests and race detector without a database:

```powershell
go test ./...
go test -race ./...
```

To run the PostgreSQL integration test against a migrated development database:

```powershell
$env:TEST_DATABASE_URL = $env:DATABASE_URL
go test ./internal/jobs -run TestPostgresStoreSaveAndGet -v
```

## Next milestone

The next step is a worker that can safely claim a pending job without two workers claiming the same job. Later milestones will add execution, retries, heartbeats, failed-job handling, and observability.
