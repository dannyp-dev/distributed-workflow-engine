package worker

import (
	"context"
	"errors"
	"io"
	"log"
	"testing"
	"time"

	coordinatorv1 "distributed-workflow-engine/gen/coordinator/v1"
	"distributed-workflow-engine/internal/jobs"
	"distributed-workflow-engine/internal/queue"

	"google.golang.org/grpc"
)

type fakeQueue struct {
	acked []string
}

func (*fakeQueue) Read(context.Context, time.Duration) (queue.Message, error) {
	return queue.Message{}, queue.ErrNoMessage
}

func (*fakeQueue) Reclaim(context.Context, time.Duration) (queue.Message, error) {
	return queue.Message{}, queue.ErrNoMessage
}

func (q *fakeQueue) Ack(_ context.Context, messageID string) error {
	q.acked = append(q.acked, messageID)
	return nil
}

type fakeCoordinator struct {
	starts        int
	failures      int
	completed     bool
	reclaimedSeen bool
}

func (*fakeCoordinator) RegisterWorker(context.Context, *coordinatorv1.RegisterWorkerRequest, ...grpc.CallOption) (*coordinatorv1.RegisterWorkerResponse, error) {
	return &coordinatorv1.RegisterWorkerResponse{Accepted: true}, nil
}

func (*fakeCoordinator) Heartbeat(context.Context, *coordinatorv1.HeartbeatRequest, ...grpc.CallOption) (*coordinatorv1.HeartbeatResponse, error) {
	return &coordinatorv1.HeartbeatResponse{}, nil
}

func (c *fakeCoordinator) StartJob(_ context.Context, request *coordinatorv1.StartJobRequest, _ ...grpc.CallOption) (*coordinatorv1.StartJobResponse, error) {
	c.starts++
	c.reclaimedSeen = c.reclaimedSeen || request.GetReclaimed()
	return &coordinatorv1.StartJobResponse{
		Accepted: true,
		Job: &coordinatorv1.Job{
			Id:          request.GetJobId(),
			Type:        "fail",
			PayloadJson: `{"failures_before_success":1}`,
			Status:      string(jobs.StatusRunning),
			Attempts:    int32(c.starts),
			MaxAttempts: 3,
			WorkerId:    request.GetWorkerId(),
		},
	}, nil
}

func (c *fakeCoordinator) CompleteJob(context.Context, *coordinatorv1.CompleteJobRequest, ...grpc.CallOption) (*coordinatorv1.CompleteJobResponse, error) {
	c.completed = true
	return &coordinatorv1.CompleteJobResponse{Status: string(jobs.StatusCompleted)}, nil
}

func (c *fakeCoordinator) FailJob(context.Context, *coordinatorv1.FailJobRequest, ...grpc.CallOption) (*coordinatorv1.FailJobResponse, error) {
	c.failures++
	return &coordinatorv1.FailJobResponse{
		Retry:       true,
		Attempts:    int32(c.failures),
		MaxAttempts: 3,
		Status:      string(jobs.StatusPending),
	}, nil
}

func TestProcessMessageRetriesThenAcknowledges(t *testing.T) {
	messageQueue := &fakeQueue{}
	coordinator := &fakeCoordinator{}
	service := New(
		Config{ID: "worker-1", RetryBaseDelay: time.Millisecond},
		messageQueue,
		coordinator,
		NewSimulatedExecutor(log.New(io.Discard, "", 0)),
		log.New(io.Discard, "", 0),
	)

	err := service.processMessage(context.Background(), queue.Message{
		ID:        "1-0",
		JobID:     "job-1",
		Reclaimed: true,
	})
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if coordinator.starts != 2 || coordinator.failures != 1 || !coordinator.completed {
		t.Fatalf("unexpected coordinator calls: %#v", coordinator)
	}
	if !coordinator.reclaimedSeen {
		t.Error("expected reclaimed flag to reach coordinator")
	}
	if len(messageQueue.acked) != 1 || messageQueue.acked[0] != "1-0" {
		t.Fatalf("expected one acknowledgement, got %v", messageQueue.acked)
	}
}

type failingExecutor struct{}

func (failingExecutor) Execute(context.Context, jobs.Job) error {
	return errors.New("boom")
}

type permanentFailureCoordinator struct {
	fakeCoordinator
}

func (c *permanentFailureCoordinator) FailJob(context.Context, *coordinatorv1.FailJobRequest, ...grpc.CallOption) (*coordinatorv1.FailJobResponse, error) {
	c.failures++
	return &coordinatorv1.FailJobResponse{
		Retry:       false,
		Attempts:    3,
		MaxAttempts: 3,
		Status:      string(jobs.StatusFailed),
	}, nil
}

func TestProcessMessageAcknowledgesPermanentFailure(t *testing.T) {
	messageQueue := &fakeQueue{}
	coordinator := &permanentFailureCoordinator{}
	service := New(
		Config{ID: "worker-1"},
		messageQueue,
		coordinator,
		failingExecutor{},
		log.New(io.Discard, "", 0),
	)

	err := service.processMessage(context.Background(), queue.Message{ID: "2-0", JobID: "job-2"})
	if err != nil {
		t.Fatalf("process failed: %v", err)
	}
	if coordinator.failures != 1 {
		t.Fatalf("expected one recorded failure, got %d", coordinator.failures)
	}
	if len(messageQueue.acked) != 1 || messageQueue.acked[0] != "2-0" {
		t.Fatalf("expected permanent failure to be acknowledged, got %v", messageQueue.acked)
	}
}

func TestRetryDelayIsCapped(t *testing.T) {
	if got, want := retryDelay(time.Second, 100), 32*time.Second; got != want {
		t.Fatalf("expected %s, got %s", want, got)
	}
}
