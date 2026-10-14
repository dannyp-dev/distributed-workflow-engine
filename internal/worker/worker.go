package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	coordinatorv1 "distributed-workflow-engine/gen/coordinator/v1"
	"distributed-workflow-engine/internal/jobs"
	"distributed-workflow-engine/internal/queue"

	"google.golang.org/grpc"
)

type MessageQueue interface {
	Read(ctx context.Context, block time.Duration) (queue.Message, error)
	Reclaim(ctx context.Context, minIdle time.Duration) (queue.Message, error)
	Ack(ctx context.Context, messageID string) error
}

type CoordinatorClient interface {
	RegisterWorker(
		ctx context.Context,
		in *coordinatorv1.RegisterWorkerRequest,
		opts ...grpc.CallOption,
	) (*coordinatorv1.RegisterWorkerResponse, error)
	Heartbeat(
		ctx context.Context,
		in *coordinatorv1.HeartbeatRequest,
		opts ...grpc.CallOption,
	) (*coordinatorv1.HeartbeatResponse, error)
	StartJob(
		ctx context.Context,
		in *coordinatorv1.StartJobRequest,
		opts ...grpc.CallOption,
	) (*coordinatorv1.StartJobResponse, error)
	CompleteJob(
		ctx context.Context,
		in *coordinatorv1.CompleteJobRequest,
		opts ...grpc.CallOption,
	) (*coordinatorv1.CompleteJobResponse, error)
	FailJob(
		ctx context.Context,
		in *coordinatorv1.FailJobRequest,
		opts ...grpc.CallOption,
	) (*coordinatorv1.FailJobResponse, error)
}

type Executor interface {
	Execute(ctx context.Context, job jobs.Job) error
}

type Config struct {
	ID                string
	ReadBlock         time.Duration
	ReclaimMinIdle    time.Duration
	HeartbeatInterval time.Duration
	RetryBaseDelay    time.Duration
}

type Worker struct {
	config      Config
	queue       MessageQueue
	coordinator CoordinatorClient
	executor    Executor
	logger      *log.Logger

	mu           sync.RWMutex
	currentJobID string
}

func New(
	config Config,
	messageQueue MessageQueue,
	coordinator CoordinatorClient,
	executor Executor,
	logger *log.Logger,
) *Worker {
	if config.ReadBlock <= 0 {
		config.ReadBlock = 2 * time.Second
	}
	if config.ReclaimMinIdle <= 0 {
		config.ReclaimMinIdle = 30 * time.Second
	}
	if config.HeartbeatInterval <= 0 {
		config.HeartbeatInterval = 5 * time.Second
	}
	if config.RetryBaseDelay <= 0 {
		config.RetryBaseDelay = 250 * time.Millisecond
	}
	if logger == nil {
		logger = log.Default()
	}

	return &Worker{
		config:      config,
		queue:       messageQueue,
		coordinator: coordinator,
		executor:    executor,
		logger:      logger,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	if w.config.ID == "" {
		return errors.New("worker ID is required")
	}

	for {
		response, err := w.coordinator.RegisterWorker(
			ctx,
			&coordinatorv1.RegisterWorkerRequest{WorkerId: w.config.ID},
		)
		if err == nil && response.GetAccepted() {
			break
		}
		if err == nil {
			err = errors.New("coordinator rejected worker registration")
		}
		w.logger.Printf("could not register worker: %v", err)
		if !wait(ctx, time.Second) {
			return nil
		}
	}

	heartbeatCtx, cancelHeartbeat := context.WithCancel(ctx)
	defer cancelHeartbeat()
	go w.heartbeatLoop(heartbeatCtx)

	w.logger.Printf("worker %s is ready", w.config.ID)
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		message, err := w.nextMessage(ctx)
		if errors.Is(err, queue.ErrNoMessage) {
			continue
		}
		if err != nil {
			w.logger.Printf("could not read work: %v", err)
			if !wait(ctx, time.Second) {
				return nil
			}
			continue
		}

		if err := w.processMessage(ctx, message); err != nil {
			w.logger.Printf("could not process message %s: %v", message.ID, err)
			if !wait(ctx, time.Second) {
				return nil
			}
		}
	}
}

func (w *Worker) nextMessage(ctx context.Context) (queue.Message, error) {
	message, err := w.queue.Reclaim(ctx, w.config.ReclaimMinIdle)
	if err == nil {
		return message, nil
	}
	if !errors.Is(err, queue.ErrNoMessage) {
		return queue.Message{}, err
	}
	return w.queue.Read(ctx, w.config.ReadBlock)
}

func (w *Worker) processMessage(ctx context.Context, message queue.Message) error {
	reclaimed := message.Reclaimed
	for {
		start, err := w.coordinator.StartJob(ctx, &coordinatorv1.StartJobRequest{
			JobId:     message.JobID,
			WorkerId:  w.config.ID,
			Reclaimed: reclaimed,
		})
		if err != nil {
			return fmt.Errorf("start job %q: %w", message.JobID, err)
		}
		if !start.GetAccepted() {
			w.logger.Printf("acknowledging unclaimable job %s: %s", message.JobID, start.GetReason())
			return w.queue.Ack(ctx, message.ID)
		}

		job, err := decodeJob(start.GetJob())
		if err != nil {
			return fmt.Errorf("decode job %q: %w", message.JobID, err)
		}
		w.setCurrentJob(job.ID)

		executionErr := w.executor.Execute(ctx, job)
		if ctx.Err() != nil {
			w.setCurrentJob("")
			return nil
		}

		if executionErr == nil {
			_, err = w.coordinator.CompleteJob(ctx, &coordinatorv1.CompleteJobRequest{
				JobId:    job.ID,
				WorkerId: w.config.ID,
			})
			w.setCurrentJob("")
			if err != nil {
				return fmt.Errorf("complete job %q: %w", job.ID, err)
			}
			if err := w.queue.Ack(ctx, message.ID); err != nil {
				return err
			}
			w.logger.Printf("completed job %s on attempt %d", job.ID, job.Attempts)
			return nil
		}

		failure, err := w.coordinator.FailJob(ctx, &coordinatorv1.FailJobRequest{
			JobId:        job.ID,
			WorkerId:     w.config.ID,
			ErrorMessage: executionErr.Error(),
		})
		w.setCurrentJob("")
		if err != nil {
			return fmt.Errorf("record failure for job %q: %w", job.ID, err)
		}
		if !failure.GetRetry() {
			if err := w.queue.Ack(ctx, message.ID); err != nil {
				return err
			}
			w.logger.Printf("job %s failed permanently after %d attempts", job.ID, failure.GetAttempts())
			return nil
		}

		delay := retryDelay(w.config.RetryBaseDelay, int(failure.GetAttempts()))
		w.logger.Printf(
			"retrying job %s after attempt %d/%d in %s",
			job.ID,
			failure.GetAttempts(),
			failure.GetMaxAttempts(),
			delay,
		)
		if !wait(ctx, delay) {
			return nil
		}
		reclaimed = false
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.config.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := w.coordinator.Heartbeat(ctx, &coordinatorv1.HeartbeatRequest{
				WorkerId:     w.config.ID,
				CurrentJobId: w.currentJob(),
			})
			if err != nil && ctx.Err() == nil {
				w.logger.Printf("heartbeat failed: %v", err)
			}
		}
	}
}

func (w *Worker) setCurrentJob(jobID string) {
	w.mu.Lock()
	w.currentJobID = jobID
	w.mu.Unlock()
}

func (w *Worker) currentJob() string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.currentJobID
}

func decodeJob(protoJob *coordinatorv1.Job) (jobs.Job, error) {
	if protoJob == nil {
		return jobs.Job{}, errors.New("coordinator returned no job")
	}

	payload := make(map[string]any)
	if err := json.Unmarshal([]byte(protoJob.GetPayloadJson()), &payload); err != nil {
		return jobs.Job{}, err
	}

	return jobs.Job{
		ID:          protoJob.GetId(),
		Type:        protoJob.GetType(),
		Payload:     payload,
		Status:      jobs.Status(protoJob.GetStatus()),
		Attempts:    int(protoJob.GetAttempts()),
		MaxAttempts: int(protoJob.GetMaxAttempts()),
		WorkerID:    protoJob.GetWorkerId(),
		LastError:   protoJob.GetLastError(),
	}, nil
}

func retryDelay(base time.Duration, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	if attempt > 6 {
		attempt = 6
	}
	return base * time.Duration(1<<(attempt-1))
}

func wait(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
