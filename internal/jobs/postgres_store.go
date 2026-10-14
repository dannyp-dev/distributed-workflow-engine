package jobs

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	pool *pgxpool.Pool
}

func NewPostgresStore(pool *pgxpool.Pool) *PostgresStore {
	return &PostgresStore{
		pool: pool,
	}
}

func (s *PostgresStore) Save(ctx context.Context, job Job) error {
	if job.MaxAttempts <= 0 {
		job.MaxAttempts = 3
	}
	if job.UpdatedAt.IsZero() {
		job.UpdatedAt = job.CreatedAt
	}

	const query = `
		INSERT INTO jobs (
			id, type, payload, status, attempts, max_attempts, worker_id,
			created_at, updated_at, started_at, completed_at, last_error
		)
		VALUES ($1, $2, $3, $4, $5, $6, NULLIF($7, ''), $8, $9, $10, $11, $12)
	`

	_, err := s.pool.Exec(
		ctx,
		query,
		job.ID,
		job.Type,
		job.Payload,
		job.Status,
		job.Attempts,
		job.MaxAttempts,
		job.WorkerID,
		job.CreatedAt,
		job.UpdatedAt,
		job.StartedAt,
		job.CompletedAt,
		job.LastError,
	)
	if err != nil {
		return fmt.Errorf("save job %q: %w", job.ID, err)
	}

	return nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (Job, error) {
	const query = `
		SELECT
			id, type, payload, status, attempts, max_attempts,
			COALESCE(worker_id, ''), created_at, updated_at,
			started_at, completed_at, last_error
		FROM jobs
		WHERE id = $1
	`

	job, err := scanJob(s.pool.QueryRow(ctx, query, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job %q: %w", id, err)
	}

	return job, nil
}

func (s *PostgresStore) ClaimNext(ctx context.Context) (Job, error) {
	const query = `
		WITH next_job AS (
			SELECT id
			FROM jobs
			WHERE status = $1
			ORDER BY created_at ASC
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs
		SET
			status = $2,
			attempts = attempts + 1,
			started_at = COALESCE(started_at, NOW()),
			updated_at = NOW()
		FROM next_job
		WHERE jobs.id = next_job.id
		RETURNING
			jobs.id, jobs.type, jobs.payload, jobs.status, jobs.attempts,
			jobs.max_attempts, COALESCE(jobs.worker_id, ''), jobs.created_at,
			jobs.updated_at, jobs.started_at, jobs.completed_at, jobs.last_error
	`

	job, err := scanJob(s.pool.QueryRow(
		ctx,
		query,
		StatusPending,
		StatusRunning,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoPendingJobs
	}
	if err != nil {
		return Job{}, fmt.Errorf("claim next job: %w", err)
	}

	return job, nil
}

func (s *PostgresStore) Start(
	ctx context.Context,
	id string,
	workerID string,
	reclaimed bool,
) (Job, bool, error) {
	const query = `
		UPDATE jobs
		SET
			status = $3,
			worker_id = $2,
			attempts = attempts + 1,
			started_at = COALESCE(started_at, NOW()),
			updated_at = NOW(),
			last_error = ''
		WHERE id = $1
			AND (status = $4 OR ($5 AND status = $6))
			AND attempts < max_attempts
		RETURNING
			id, type, payload, status, attempts, max_attempts,
			COALESCE(worker_id, ''), created_at, updated_at,
			started_at, completed_at, last_error
	`

	job, err := scanJob(s.pool.QueryRow(
		ctx,
		query,
		id,
		workerID,
		StatusRunning,
		StatusPending,
		reclaimed,
		StatusRunning,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, nil
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("start job %q: %w", id, err)
	}

	return job, true, nil
}

func (s *PostgresStore) Complete(
	ctx context.Context,
	id string,
	workerID string,
) (Job, error) {
	const query = `
		UPDATE jobs
		SET
			status = $3,
			updated_at = NOW(),
			completed_at = NOW()
		WHERE id = $1 AND worker_id = $2 AND status = $4
		RETURNING
			id, type, payload, status, attempts, max_attempts,
			COALESCE(worker_id, ''), created_at, updated_at,
			started_at, completed_at, last_error
	`

	job, err := scanJob(s.pool.QueryRow(
		ctx,
		query,
		id,
		workerID,
		StatusCompleted,
		StatusRunning,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrInvalidTransition
	}
	if err != nil {
		return Job{}, fmt.Errorf("complete job %q: %w", id, err)
	}

	return job, nil
}

func (s *PostgresStore) Fail(
	ctx context.Context,
	id string,
	workerID string,
	errorMessage string,
) (Job, bool, error) {
	const query = `
		UPDATE jobs
		SET
			status = CASE WHEN attempts < max_attempts THEN $4 ELSE $5 END,
			worker_id = CASE WHEN attempts < max_attempts THEN NULL ELSE worker_id END,
			updated_at = NOW(),
			completed_at = CASE WHEN attempts < max_attempts THEN NULL ELSE NOW() END,
			last_error = $3
		WHERE id = $1 AND worker_id = $2 AND status = $6
		RETURNING
			id, type, payload, status, attempts, max_attempts,
			COALESCE(worker_id, ''), created_at, updated_at,
			started_at, completed_at, last_error
	`

	job, err := scanJob(s.pool.QueryRow(
		ctx,
		query,
		id,
		workerID,
		errorMessage,
		StatusPending,
		StatusFailed,
		StatusRunning,
	))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, false, ErrInvalidTransition
	}
	if err != nil {
		return Job{}, false, fmt.Errorf("fail job %q: %w", id, err)
	}

	return job, job.Status == StatusPending, nil
}

func scanJob(row pgx.Row) (Job, error) {
	var job Job
	err := row.Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.Attempts,
		&job.MaxAttempts,
		&job.WorkerID,
		&job.CreatedAt,
		&job.UpdatedAt,
		&job.StartedAt,
		&job.CompletedAt,
		&job.LastError,
	)
	return job, err
}
