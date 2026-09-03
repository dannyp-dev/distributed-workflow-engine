package jobs

import "testing"

func TestStoreSaveAndGet(t *testing.T) {
	store := NewStore()

	original := Job{
		ID:     "job-1",
		Type:   "resize_image",
		Status: StatusPending,
	}

	store.Save(original)

	saved, found := store.Get("job-1")
	if !found {
		t.Fatal("expected saved job to be found")
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

	_, found := store.Get("missing-job")
	if found {
		t.Error("expected missing job not to be found")
	}
}