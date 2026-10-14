// Command api accepts workflow jobs over HTTP and coordinates workers over gRPC.
package main

import (
	"context"
	"crypto/rand"
	coordinatorv1 "distributed-workflow-engine/gen/coordinator/v1"
	"distributed-workflow-engine/internal/coordinator"
	"distributed-workflow-engine/internal/database"
	"distributed-workflow-engine/internal/jobs"
	"distributed-workflow-engine/internal/queue"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

const defaultMaxAttempts = 3

func healthHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	_, _ = w.Write([]byte("OK"))
}

func (a *api) jobHandler(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	job, err := a.store.Get(r.Context(), id)
	if errors.Is(err, jobs.ErrJobNotFound) {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("could not get job: %v", err)
		http.Error(w, "Could not get job", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Printf("could not encode job response: %v", err)
	}
}

func (a *api) jobsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var input createJobRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	input.Type = strings.TrimSpace(input.Type)
	if input.Type == "" {
		http.Error(w, "Job type is required", http.StatusBadRequest)
		return
	}
	if input.MaxAttempts == 0 {
		input.MaxAttempts = defaultMaxAttempts
	}
	if input.MaxAttempts < 1 || input.MaxAttempts > 10 {
		http.Error(w, "max_attempts must be between 1 and 10", http.StatusBadRequest)
		return
	}

	id, err := newJobID()
	if err != nil {
		http.Error(w, "Could not generate job ID", http.StatusInternalServerError)
		return
	}

	now := time.Now().UTC()
	job := jobs.Job{
		ID:          id,
		Type:        input.Type,
		Payload:     input.Payload,
		Status:      jobs.StatusPending,
		MaxAttempts: input.MaxAttempts,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := a.store.Save(r.Context(), job); err != nil {
		log.Printf("could not save job: %v", err)
		http.Error(w, "Could not save job", http.StatusInternalServerError)
		return
	}
	if _, err := a.queue.Enqueue(r.Context(), job.ID); err != nil {
		log.Printf("job %s was saved but could not be scheduled: %v", job.ID, err)
		http.Error(w, "Job was saved but could not be scheduled", http.StatusServiceUnavailable)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Printf("could not encode job response: %v", err)
	}
}

func newJobID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

type createJobRequest struct {
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload"`
	MaxAttempts int            `json:"max_attempts,omitempty"`
}

type jobQueue interface {
	Enqueue(ctx context.Context, jobID string) (string, error)
}

type api struct {
	store jobs.JobStore
	queue jobQueue
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is required")
	}

	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect to database: %w", err)
	}
	defer pool.Close()

	stream, err := queue.Open(
		envOrDefault("REDIS_URL", "redis://127.0.0.1:6379/0"),
		envOrDefault("REDIS_STREAM", "workflow:jobs"),
		envOrDefault("REDIS_GROUP", "workflow-workers"),
		"api",
	)
	if err != nil {
		return err
	}
	defer stream.Close()
	if err := stream.Ping(ctx); err != nil {
		return err
	}
	if err := stream.EnsureGroup(ctx); err != nil {
		return err
	}

	store := jobs.NewPostgresStore(pool)
	app := &api{store: store, queue: stream}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler)
	mux.HandleFunc("POST /jobs", app.jobsHandler)
	mux.HandleFunc("GET /jobs/{id}", app.jobHandler)
	httpServer := &http.Server{
		Addr:              envOrDefault("HTTP_ADDR", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	grpcAddress := envOrDefault("GRPC_ADDR", ":9090")
	grpcListener, err := net.Listen("tcp", grpcAddress)
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	grpcServer := grpc.NewServer()
	coordinatorv1.RegisterCoordinatorServiceServer(grpcServer, coordinator.NewServer(store))
	healthServer := health.NewServer()
	healthv1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)

	errCh := make(chan error, 2)
	go func() {
		log.Printf("HTTP API listening on %s", httpServer.Addr)
		errCh <- httpServer.ListenAndServe()
	}()
	go func() {
		log.Printf("gRPC coordinator listening on %s", grpcAddress)
		errCh <- grpcServer.Serve(grpcListener)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		healthServer.SetServingStatus("", healthv1.HealthCheckResponse_NOT_SERVING)
		grpcServer.GracefulStop()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down HTTP server: %w", err)
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func envOrDefault(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
