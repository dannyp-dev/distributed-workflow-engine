package jobs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresStoreSaveAndGet(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("could not create database pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("could not connect to database: %v", err)
	}

	store := NewPostgresStore(pool)
	job := Job{
		ID:        fmt.Sprintf("integration-%d", time.Now().UnixNano()),
		Type:      "resize_image",
		Payload:   map[string]any{"filename": "photo.jpg"},
		Status:    StatusPending,
		CreatedAt: time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC),
	}
	if _, err := store.Get(ctx, "missing-"+job.ID); !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound for missing job, got %v", err)
	}

	t.Cleanup(func() {
		if _, err := pool.Exec(context.Background(), "DELETE FROM jobs WHERE id = $1", job.ID); err != nil {
			t.Errorf("could not clean up integration job: %v", err)
		}
		pool.Close()
	})

	if err := store.Save(ctx, job); err != nil {
		t.Fatalf("could not save job: %v", err)
	}

	saved, err := store.Get(ctx, job.ID)
	if err != nil {
		t.Fatalf("could not get job: %v", err)
	}

	if saved.ID != job.ID {
		t.Errorf("expected ID %q, got %q", job.ID, saved.ID)
	}
	if saved.Type != job.Type {
		t.Errorf("expected type %q, got %q", job.Type, saved.Type)
	}
	if !reflect.DeepEqual(saved.Payload, job.Payload) {
		t.Errorf("expected payload %#v, got %#v", job.Payload, saved.Payload)
	}
	if saved.Status != job.Status {
		t.Errorf("expected status %q, got %q", job.Status, saved.Status)
	}
	if !saved.CreatedAt.Equal(job.CreatedAt) {
		t.Errorf("expected created_at %v, got %v", job.CreatedAt, saved.CreatedAt)
	}

	claimed, accepted, err := store.Start(ctx, job.ID, "worker-1", false)
	if err != nil {
		t.Fatalf("could not claim job: %v", err)
	}
	if !accepted {
		t.Fatal("expected pending job to be claimable")
	}
	if claimed.ID != job.ID {
		t.Errorf("expected claimed job ID %q, got %q", job.ID, claimed.ID)
	}
	if claimed.Status != StatusRunning {
		t.Errorf("expected claimed status %q, got %q", StatusRunning, claimed.Status)
	}

	failed, retry, err := store.Fail(ctx, job.ID, "worker-1", "temporary failure")
	if err != nil {
		t.Fatalf("could not fail job: %v", err)
	}
	if !retry || failed.Status != StatusPending {
		t.Fatalf("expected retryable pending job, got retry=%v status=%q", retry, failed.Status)
	}

	retried, accepted, err := store.Start(ctx, job.ID, "worker-2", true)
	if err != nil || !accepted {
		t.Fatalf("could not reclaim job: accepted=%v err=%v", accepted, err)
	}
	if retried.Attempts != 2 || retried.WorkerID != "worker-2" {
		t.Fatalf("unexpected reclaimed job: %#v", retried)
	}

	completed, err := store.Complete(ctx, job.ID, "worker-2")
	if err != nil {
		t.Fatalf("could not complete job: %v", err)
	}
	if completed.Status != StatusCompleted || completed.CompletedAt == nil {
		t.Fatalf("expected completed job, got %#v", completed)
	}
}
