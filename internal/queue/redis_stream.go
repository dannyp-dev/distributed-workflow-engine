// Package queue provides Redis Streams scheduling for workflow jobs.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrNoMessage = errors.New("no stream message available")

type Message struct {
	ID        string
	JobID     string
	Reclaimed bool
}

type Stream struct {
	client   *redis.Client
	name     string
	group    string
	consumer string
}

func Open(redisURL string, streamName string, group string, consumer string) (*Stream, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}

	return &Stream{
		client:   redis.NewClient(options),
		name:     streamName,
		group:    group,
		consumer: consumer,
	}, nil
}

func (s *Stream) Ping(ctx context.Context) error {
	if err := s.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping Redis: %w", err)
	}
	return nil
}

func (s *Stream) Close() error {
	return s.client.Close()
}

func (s *Stream) EnsureGroup(ctx context.Context) error {
	err := s.client.XGroupCreateMkStream(ctx, s.name, s.group, "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("create Redis consumer group: %w", err)
	}
	return nil
}

func (s *Stream) Enqueue(ctx context.Context, jobID string) (string, error) {
	id, err := s.client.XAdd(ctx, &redis.XAddArgs{
		Stream: s.name,
		Values: map[string]any{
			"job_id":      jobID,
			"enqueued_at": time.Now().UTC().UnixNano(),
		},
	}).Result()
	if err != nil {
		return "", fmt.Errorf("enqueue job %q: %w", jobID, err)
	}
	return id, nil
}

func (s *Stream) Read(ctx context.Context, block time.Duration) (Message, error) {
	streams, err := s.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    s.group,
		Consumer: s.consumer,
		Streams:  []string{s.name, ">"},
		Count:    1,
		Block:    block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return Message{}, ErrNoMessage
	}
	if err != nil {
		return Message{}, fmt.Errorf("read Redis stream: %w", err)
	}
	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return Message{}, ErrNoMessage
	}

	return decodeMessage(streams[0].Messages[0], false)
}

func (s *Stream) Reclaim(ctx context.Context, minIdle time.Duration) (Message, error) {
	messages, _, err := s.client.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   s.name,
		Group:    s.group,
		Consumer: s.consumer,
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    1,
	}).Result()
	if errors.Is(err, redis.Nil) || (err == nil && len(messages) == 0) {
		return Message{}, ErrNoMessage
	}
	if err != nil {
		return Message{}, fmt.Errorf("reclaim Redis message: %w", err)
	}

	return decodeMessage(messages[0], true)
}

func (s *Stream) Ack(ctx context.Context, messageID string) error {
	if err := s.client.XAck(ctx, s.name, s.group, messageID).Err(); err != nil {
		return fmt.Errorf("acknowledge Redis message %q: %w", messageID, err)
	}
	return nil
}

func decodeMessage(message redis.XMessage, reclaimed bool) (Message, error) {
	value, ok := message.Values["job_id"]
	if !ok {
		return Message{}, fmt.Errorf("Redis message %q has no job_id", message.ID)
	}

	jobID := fmt.Sprint(value)
	if jobID == "" {
		return Message{}, fmt.Errorf("Redis message %q has an empty job_id", message.ID)
	}

	return Message{
		ID:        message.ID,
		JobID:     jobID,
		Reclaimed: reclaimed,
	}, nil
}
