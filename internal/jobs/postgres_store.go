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
	const query = `
		INSERT INTO jobs (id, type, payload, status, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`

	_, err := s.pool.Exec(
		ctx,
		query,
		job.ID,
		job.Type,
		job.Payload,
		job.Status,
		job.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("save job %q: %w", job.ID, err)
	}

	return nil
}

func (s *PostgresStore) Get(ctx context.Context, id string) (Job, error) {
	const query = `
		SELECT id, type, payload, status, created_at
		FROM jobs
		WHERE id = $1
	`

	var job Job
	err := s.pool.QueryRow(ctx, query, id).Scan(
		&job.ID,
		&job.Type,
		&job.Payload,
		&job.Status,
		&job.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrJobNotFound
	}
	if err != nil {
		return Job{}, fmt.Errorf("get job %q: %w", id, err)
	}

	return job, nil
}
