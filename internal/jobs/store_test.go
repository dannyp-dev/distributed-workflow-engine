package jobs

import (
	"context"
	"errors"
	"testing"
)

func TestStoreSaveAndGet(t *testing.T) {
	store := NewStore()

	original := Job{
		ID:     "job-1",
		Type:   "resize_image",
		Status: StatusPending,
	}

	if err := store.Save(context.Background(), original); err != nil {
		t.Fatalf("could not save job: %v", err)
	}

	saved, err := store.Get(context.Background(), "job-1")
	if err != nil {
		t.Fatalf("could not get saved job: %v", err)
	}

	if saved.ID != original.ID {
		t.Errorf("expected ID %q, got %q", original.ID, saved.ID)
	}

	if saved.Type != original.Type {
		t.Errorf("expected type %q, got %q", original.Type, saved.Type)
	}

	if saved.Status != original.Status {
		t.Errorf("expected status %q, got %q", original.Status, saved.Status)
	}
}

func TestStoreGetMissingJob(t *testing.T) {
	store := NewStore()

	_, err := store.Get(context.Background(), "missing-job")
	if !errors.Is(err, ErrJobNotFound) {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}
