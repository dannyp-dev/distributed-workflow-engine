package queue

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestDecodeMessage(t *testing.T) {
	message, err := decodeMessage(redis.XMessage{
		ID:     "1-0",
		Values: map[string]any{"job_id": "job-1"},
	}, true)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if message.ID != "1-0" || message.JobID != "job-1" || !message.Reclaimed {
		t.Fatalf("unexpected message: %#v", message)
	}
}

func TestDecodeMessageRejectsMissingJobID(t *testing.T) {
	_, err := decodeMessage(redis.XMessage{ID: "1-0", Values: map[string]any{}}, false)
	if err == nil {
		t.Fatal("expected missing job_id error")
	}
}
