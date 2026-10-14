// Command benchmark measures Redis scheduling latency and abandoned-task recovery.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	mode := flag.String("mode", "all", "benchmark mode: scheduling, recovery, or all")
	jobs := flag.Int("jobs", 1000, "number of jobs")
	concurrency := flag.Int("concurrency", 20, "concurrent scheduling clients")
	reclaimIdle := flag.Duration("reclaim-idle", 100*time.Millisecond, "minimum idle time before reclaiming")
	flag.Parse()

	if *jobs < 1 || *concurrency < 1 {
		log.Fatal("jobs and concurrency must be positive")
	}

	ctx := context.Background()
	client, err := openRedis(ctx, envOrDefault("REDIS_URL", "redis://127.0.0.1:6379/0"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	switch *mode {
	case "scheduling":
		err = schedulingBenchmark(ctx, client, *jobs, *concurrency)
	case "recovery":
		err = recoveryBenchmark(ctx, client, *jobs, *reclaimIdle)
	case "all":
		if err = schedulingBenchmark(ctx, client, *jobs, *concurrency); err == nil {
			err = recoveryBenchmark(ctx, client, *jobs, *reclaimIdle)
		}
	default:
		err = fmt.Errorf("unknown mode %q", *mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func openRedis(ctx context.Context, redisURL string) (*redis.Client, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping Redis: %w", err)
	}
	return client, nil
}

func schedulingBenchmark(ctx context.Context, client *redis.Client, count int, concurrency int) error {
	stream := fmt.Sprintf("benchmark:scheduling:%d", time.Now().UnixNano())
	defer client.Del(context.Background(), stream)

	latencies := make(chan time.Duration, count)
	errCh := make(chan error, count)
	semaphore := make(chan struct{}, concurrency)
	var waitGroup sync.WaitGroup
	benchmarkStart := time.Now()

	for index := 0; index < count; index++ {
		waitGroup.Add(1)
		semaphore <- struct{}{}
		go func(jobNumber int) {
			defer waitGroup.Done()
			defer func() { <-semaphore }()
			started := time.Now()
			err := client.XAdd(ctx, &redis.XAddArgs{
				Stream: stream,
				Values: map[string]any{"job_id": fmt.Sprintf("job-%d", jobNumber)},
			}).Err()
			latencies <- time.Since(started)
			if err != nil {
				errCh <- err
			}
		}(index)
	}
	waitGroup.Wait()
	close(latencies)
	close(errCh)
	if err := <-errCh; err != nil {
		return fmt.Errorf("schedule job: %w", err)
	}

	durations := make([]time.Duration, 0, count)
	subMillisecond := 0
	for latency := range latencies {
		durations = append(durations, latency)
		if latency < time.Millisecond {
			subMillisecond++
		}
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	elapsed := time.Since(benchmarkStart)

	fmt.Println("Scheduling benchmark")
	fmt.Printf("  jobs: %d, concurrency: %d\n", count, concurrency)
	fmt.Printf("  throughput: %.0f jobs/second\n", float64(count)/elapsed.Seconds())
	fmt.Printf("  p50: %s, p95: %s, p99: %s\n", percentile(durations, 50), percentile(durations, 95), percentile(durations, 99))
	fmt.Printf("  sub-millisecond: %.2f%%\n", 100*float64(subMillisecond)/float64(count))
	return nil
}

func recoveryBenchmark(ctx context.Context, client *redis.Client, count int, minIdle time.Duration) error {
	stream := fmt.Sprintf("benchmark:recovery:%d", time.Now().UnixNano())
	group := "recovery-workers"
	defer client.Del(context.Background(), stream)

	if err := client.XGroupCreateMkStream(ctx, stream, group, "0").Err(); err != nil {
		return fmt.Errorf("create recovery group: %w", err)
	}
	for index := 0; index < count; index++ {
		if err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]any{"job_id": fmt.Sprintf("job-%d", index)},
		}).Err(); err != nil {
			return fmt.Errorf("enqueue recovery job: %w", err)
		}
	}

	delivered := 0
	for delivered < count {
		streams, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: "crashed-worker",
			Streams:  []string{stream, ">"},
			Count:    int64(min(500, count-delivered)),
		}).Result()
		if err != nil {
			return fmt.Errorf("simulate crashed consumer: %w", err)
		}
		if len(streams) == 0 {
			break
		}
		delivered += len(streams[0].Messages)
	}

	time.Sleep(minIdle + 10*time.Millisecond)
	start := "0-0"
	recovered := make(map[string]struct{}, count)
	started := time.Now()
	for len(recovered) < delivered {
		messages, next, err := client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: "recovery-worker",
			MinIdle:  minIdle,
			Start:    start,
			Count:    500,
		}).Result()
		if err != nil && !errors.Is(err, redis.Nil) {
			return fmt.Errorf("reclaim abandoned jobs: %w", err)
		}
		if len(messages) == 0 && next == "0-0" {
			break
		}
		for _, message := range messages {
			recovered[message.ID] = struct{}{}
			if err := client.XAck(ctx, stream, group, message.ID).Err(); err != nil {
				return fmt.Errorf("acknowledge recovered job: %w", err)
			}
		}
		start = next
	}

	percent := 100 * float64(len(recovered)) / float64(count)
	fmt.Println("Recovery benchmark")
	fmt.Printf("  abandoned jobs: %d\n", delivered)
	fmt.Printf("  recovered jobs: %d/%d (%.3f%%)\n", len(recovered), count, percent)
	fmt.Printf("  recovery time: %s\n", time.Since(started))
	return nil
}

func percentile(values []time.Duration, percent int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := (len(values)*percent + 99) / 100
	if index < 1 {
		index = 1
	}
	return values[index-1]
}

func envOrDefault(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
