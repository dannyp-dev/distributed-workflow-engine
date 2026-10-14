package jobs

import (
	"context"
	"errors"
	"sync"
)

var ErrJobNotFound = errors.New("job not found")
var ErrNoPendingJobs = errors.New("no pending jobs")
var ErrInvalidTransition = errors.New("invalid job state transition")

type JobStore interface {
	Save(ctx context.Context, job Job) error
	Get(ctx context.Context, id string) (Job, error)
}

type Store struct {
	mu   sync.RWMutex
	jobs map[string]Job
}

func NewStore() *Store {
	return &Store{
		jobs: make(map[string]Job),
	}
}

func (s *Store) Save(_ context.Context, job Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	return nil
}

func (s *Store) Get(_ context.Context, id string) (Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, found := s.jobs[id]
	if !found {
		return Job{}, ErrJobNotFound
	}

	return job, nil
}
