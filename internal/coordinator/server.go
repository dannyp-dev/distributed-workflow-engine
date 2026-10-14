// Package coordinator implements the gRPC control plane used by workers.
package coordinator

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"time"

	coordinatorv1 "distributed-workflow-engine/gen/coordinator/v1"
	"distributed-workflow-engine/internal/jobs"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Store interface {
	Start(ctx context.Context, id string, workerID string, reclaimed bool) (jobs.Job, bool, error)
	Complete(ctx context.Context, id string, workerID string) (jobs.Job, error)
	Fail(ctx context.Context, id string, workerID string, errorMessage string) (jobs.Job, bool, error)
}

type WorkerState struct {
	ID           string
	CurrentJobID string
	LastSeen     time.Time
}

type Server struct {
	coordinatorv1.UnimplementedCoordinatorServiceServer

	store   Store
	mu      sync.RWMutex
	workers map[string]WorkerState
	now     func() time.Time
}

func NewServer(store Store) *Server {
	return &Server{
		store:   store,
		workers: make(map[string]WorkerState),
		now:     time.Now,
	}
}

func (s *Server) RegisterWorker(
	_ context.Context,
	request *coordinatorv1.RegisterWorkerRequest,
) (*coordinatorv1.RegisterWorkerResponse, error) {
	if request.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	s.recordHeartbeat(request.GetWorkerId(), "")
	return &coordinatorv1.RegisterWorkerResponse{Accepted: true}, nil
}

func (s *Server) Heartbeat(
	_ context.Context,
	request *coordinatorv1.HeartbeatRequest,
) (*coordinatorv1.HeartbeatResponse, error) {
	if request.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}

	now := s.recordHeartbeat(request.GetWorkerId(), request.GetCurrentJobId())
	return &coordinatorv1.HeartbeatResponse{ServerTime: now.Format(time.RFC3339Nano)}, nil
}

func (s *Server) StartJob(
	ctx context.Context,
	request *coordinatorv1.StartJobRequest,
) (*coordinatorv1.StartJobResponse, error) {
	if request.GetJobId() == "" || request.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and worker_id are required")
	}

	job, accepted, err := s.store.Start(
		ctx,
		request.GetJobId(),
		request.GetWorkerId(),
		request.GetReclaimed(),
	)
	if err != nil {
		return nil, rpcError(err)
	}
	if !accepted {
		return &coordinatorv1.StartJobResponse{
			Accepted: false,
			Reason:   "job is not claimable",
		}, nil
	}

	s.recordHeartbeat(request.GetWorkerId(), request.GetJobId())
	protoJob, err := toProtoJob(job)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode job payload: %v", err)
	}

	return &coordinatorv1.StartJobResponse{
		Accepted: true,
		Job:      protoJob,
	}, nil
}

func (s *Server) CompleteJob(
	ctx context.Context,
	request *coordinatorv1.CompleteJobRequest,
) (*coordinatorv1.CompleteJobResponse, error) {
	if request.GetJobId() == "" || request.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and worker_id are required")
	}

	job, err := s.store.Complete(ctx, request.GetJobId(), request.GetWorkerId())
	if err != nil {
		return nil, rpcError(err)
	}

	s.recordHeartbeat(request.GetWorkerId(), "")
	return &coordinatorv1.CompleteJobResponse{Status: string(job.Status)}, nil
}

func (s *Server) FailJob(
	ctx context.Context,
	request *coordinatorv1.FailJobRequest,
) (*coordinatorv1.FailJobResponse, error) {
	if request.GetJobId() == "" || request.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "job_id and worker_id are required")
	}

	job, retry, err := s.store.Fail(
		ctx,
		request.GetJobId(),
		request.GetWorkerId(),
		request.GetErrorMessage(),
	)
	if err != nil {
		return nil, rpcError(err)
	}

	s.recordHeartbeat(request.GetWorkerId(), "")
	return &coordinatorv1.FailJobResponse{
		Retry:       retry,
		Attempts:    int32(job.Attempts),
		MaxAttempts: int32(job.MaxAttempts),
		Status:      string(job.Status),
	}, nil
}

func (s *Server) WorkerSnapshot() []WorkerState {
	s.mu.RLock()
	defer s.mu.RUnlock()

	workers := make([]WorkerState, 0, len(s.workers))
	for _, worker := range s.workers {
		workers = append(workers, worker)
	}
	return workers
}

func (s *Server) recordHeartbeat(workerID string, currentJobID string) time.Time {
	now := s.now().UTC()
	s.mu.Lock()
	s.workers[workerID] = WorkerState{
		ID:           workerID,
		CurrentJobID: currentJobID,
		LastSeen:     now,
	}
	s.mu.Unlock()
	return now
}

func toProtoJob(job jobs.Job) (*coordinatorv1.Job, error) {
	payload, err := json.Marshal(job.Payload)
	if err != nil {
		return nil, err
	}

	return &coordinatorv1.Job{
		Id:          job.ID,
		Type:        job.Type,
		PayloadJson: string(payload),
		Status:      string(job.Status),
		Attempts:    int32(job.Attempts),
		MaxAttempts: int32(job.MaxAttempts),
		WorkerId:    job.WorkerID,
		CreatedAt:   formatTime(job.CreatedAt),
		StartedAt:   formatOptionalTime(job.StartedAt),
		CompletedAt: formatOptionalTime(job.CompletedAt),
		LastError:   job.LastError,
	}, nil
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func formatOptionalTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return formatTime(*value)
}

func rpcError(err error) error {
	switch {
	case errors.Is(err, jobs.ErrJobNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, jobs.ErrInvalidTransition):
		return status.Error(codes.FailedPrecondition, err.Error())
	default:
		return status.Errorf(codes.Internal, "coordinator storage error: %v", err)
	}
}
