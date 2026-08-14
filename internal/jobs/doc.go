// Package jobs will contain the core job model and its state transitions.
package jobs

import "time"

type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

type Job struct {
	ID        string         `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	Status    Status         `json:"status"`
	CreatedAt time.Time      `json:"created_at"`
}
