// Command worker consumes and executes workflow jobs.
package main

import (
	"context"
	"crypto/rand"
	coordinatorv1 "distributed-workflow-engine/gen/coordinator/v1"
	"distributed-workflow-engine/internal/queue"
	workerpkg "distributed-workflow-engine/internal/worker"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	workerID, err := resolveWorkerID()
	if err != nil {
		return err
	}

	stream, err := queue.Open(
		envOrDefault("REDIS_URL", "redis://127.0.0.1:6379/0"),
		envOrDefault("REDIS_STREAM", "workflow:jobs"),
		envOrDefault("REDIS_GROUP", "workflow-workers"),
		workerID,
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

	connection, err := grpc.NewClient(
		envOrDefault("COORDINATOR_ADDR", "127.0.0.1:9090"),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return fmt.Errorf("create coordinator client: %w", err)
	}
	defer connection.Close()

	config := workerpkg.Config{
		ID:                workerID,
		ReadBlock:         durationFromEnv("WORKER_READ_BLOCK", 2*time.Second),
		ReclaimMinIdle:    durationFromEnv("WORKER_RECLAIM_MIN_IDLE", 30*time.Second),
		HeartbeatInterval: durationFromEnv("WORKER_HEARTBEAT_INTERVAL", 5*time.Second),
		RetryBaseDelay:    durationFromEnv("WORKER_RETRY_BASE_DELAY", 250*time.Millisecond),
	}
	service := workerpkg.New(
		config,
		stream,
		coordinatorv1.NewCoordinatorServiceClient(connection),
		workerpkg.NewSimulatedExecutor(log.Default()),
		log.Default(),
	)

	log.Printf("starting worker %s", workerID)
	return service.Run(ctx)
}

func resolveWorkerID() (string, error) {
	if value := os.Getenv("WORKER_ID"); value != "" {
		return value, nil
	}
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("read hostname: %w", err)
	}
	randomBytes := make([]byte, 4)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("generate worker ID: %w", err)
	}
	return hostname + "-" + hex.EncodeToString(randomBytes), nil
}

func durationFromEnv(name string, fallback time.Duration) time.Duration {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err == nil {
		return duration
	}
	if milliseconds, conversionErr := strconv.Atoi(value); conversionErr == nil {
		return time.Duration(milliseconds) * time.Millisecond
	}
	log.Printf("invalid %s=%q, using %s", name, value, fallback)
	return fallback
}

func envOrDefault(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
