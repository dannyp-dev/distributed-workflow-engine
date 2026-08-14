# Distributed Workflow Engine

A learning project for building a small distributed job system in Go. This repository is intentionally only a scaffold: it compiles, but the actual engine is yours to design and implement.

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
|   |-- database/     # Future persistence layer
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

There are no external packages, services, environment variables, containers, or databases to configure yet.

## First milestone: an in-memory jobs API

Build these three endpoints in `cmd/api`:

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health` | Return a simple success response so you know the server is running. |
| `POST` | `/jobs` | Validate a request, create a job, store it in memory, and return it. |
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

Suggested order of work:

1. Define `Job` and its status values in `internal/jobs`.
2. Start an HTTP server using Go's standard `net/http` package.
3. Add `GET /health`.
4. Add a small in-memory store, such as a map protected by a mutex.
5. Add `POST /jobs`, then `GET /jobs/{id}`.
6. Write handler and store tests with `net/http/httptest`.

You are done with milestone one when you can create a job, fetch the same job, receive `404` for an unknown ID, and run `go test ./...` successfully.

## What not to add yet

Avoid PostgreSQL, Redis, Docker, gRPC, retries, heartbeats, dead-letter queues, authentication, and a web UI until the in-memory API works and you can explain every part of it.

After milestone one, the next meaningful step is a worker that can claim a pending job without two workers claiming the same one. That problem will naturally motivate a real database and transactions.
