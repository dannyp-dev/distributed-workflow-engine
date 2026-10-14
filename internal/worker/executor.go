package worker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"distributed-workflow-engine/internal/jobs"
)

type SimulatedExecutor struct {
	logger *log.Logger
}

func NewSimulatedExecutor(logger *log.Logger) *SimulatedExecutor {
	if logger == nil {
		logger = log.Default()
	}
	return &SimulatedExecutor{logger: logger}
}

func (e *SimulatedExecutor) Execute(ctx context.Context, job jobs.Job) error {
	switch job.Type {
	case "noop":
		return nil
	case "echo":
		e.logger.Printf("job %s says: %v", job.ID, job.Payload["message"])
		return nil
	case "sleep":
		duration := durationFromPayload(job.Payload, "duration_ms", time.Second)
		if duration > 10*time.Second {
			duration = 10 * time.Second
		}
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	case "fail":
		failuresBeforeSuccess := int(numberFromPayload(job.Payload, "failures_before_success", -1))
		if failuresBeforeSuccess >= 0 && job.Attempts > failuresBeforeSuccess {
			return nil
		}
		return errors.New("simulated task failure")
	default:
		return fmt.Errorf("unsupported job type %q", job.Type)
	}
}

func durationFromPayload(payload map[string]any, key string, fallback time.Duration) time.Duration {
	milliseconds := numberFromPayload(payload, key, float64(fallback/time.Millisecond))
	if milliseconds < 0 {
		return fallback
	}
	return time.Duration(milliseconds) * time.Millisecond
}

func numberFromPayload(payload map[string]any, key string, fallback float64) float64 {
	value, ok := payload[key]
	if !ok {
		return fallback
	}

	switch number := value.(type) {
	case float64:
		return number
	case int:
		return float64(number)
	case int64:
		return float64(number)
	default:
		return fallback
	}
}
