package coordinator

import (
	"context"
	"errors"
	"testing"
	"time"

	coordinatorv1 "distributed-workflow-engine/gen/coordinator/v1"
	"distributed-workflow-engine/internal/jobs"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeStore struct {
	job       jobs.Job
	accepted  bool
	retry     bool
	startErr  error
	completed bool
	failed    bool
}

func (s *fakeStore) Start(_ context.Context, _ string, workerID string, _ bool) (jobs.Job, bool, error) {
	if s.startErr != nil {
		return jobs.Job{}, false, s.startErr
	}
	s.job.WorkerID = workerID
	s.job.Status = jobs.StatusRunning
	s.job.Attempts++
	return s.job, s.accepted, nil
}

func (s *fakeStore) Complete(_ context.Context, _ string, _ string) (jobs.Job, error) {
	s.completed = true
	s.job.Status = jobs.StatusCompleted
	return s.job, nil
}

func (s *fakeStore) Fail(_ context.Context, _ string, _ string, message string) (jobs.Job, bool, error) {
	s.failed = true
	s.job.LastError = message
	return s.job, s.retry, nil
}

func TestWorkerRegistrationAndHeartbeat(t *testing.T) {
	server := NewServer(&fakeStore{})
	server.now = func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) }

	response, err := server.RegisterWorker(context.Background(), &coordinatorv1.RegisterWorkerRequest{
		WorkerId: "worker-1",
	})
	if err != nil || !response.GetAccepted() {
		t.Fatalf("expected accepted registration, response=%v err=%v", response, err)
	}

	_, err = server.Heartbeat(context.Background(), &coordinatorv1.HeartbeatRequest{
		WorkerId:     "worker-1",
		CurrentJobId: "job-1",
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	snapshot := server.WorkerSnapshot()
	if len(snapshot) != 1 || snapshot[0].CurrentJobID != "job-1" {
		t.Fatalf("unexpected worker snapshot: %#v", snapshot)
	}
}

func TestStartAndCompleteJob(t *testing.T) {
	store := &fakeStore{
		accepted: true,
		job: jobs.Job{
			ID:          "job-1",
			Type:        "echo",
			Payload:     map[string]any{"message": "hello"},
			Status:      jobs.StatusPending,
			MaxAttempts: 3,
		},
	}
	server := NewServer(store)

	started, err := server.StartJob(context.Background(), &coordinatorv1.StartJobRequest{
		JobId:    "job-1",
		WorkerId: "worker-1",
	})
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if !started.GetAccepted() || started.GetJob().GetWorkerId() != "worker-1" {
		t.Fatalf("unexpected start response: %#v", started)
	}

	completed, err := server.CompleteJob(context.Background(), &coordinatorv1.CompleteJobRequest{
		JobId:    "job-1",
		WorkerId: "worker-1",
	})
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}
	if !store.completed || completed.GetStatus() != string(jobs.StatusCompleted) {
		t.Fatalf("job was not completed: %#v", completed)
	}
}

func TestStorageErrorsBecomeGRPCErrors(t *testing.T) {
	server := NewServer(&fakeStore{startErr: jobs.ErrInvalidTransition})
	_, err := server.StartJob(context.Background(), &coordinatorv1.StartJobRequest{
		JobId:    "job-1",
		WorkerId: "worker-1",
	})
	if !errors.Is(err, status.Error(codes.FailedPrecondition, jobs.ErrInvalidTransition.Error())) &&
		status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected failed precondition, got %v", err)
	}
}
