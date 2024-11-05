package workers

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"io"
	"time"
)

const (
	NanoSecondPrecision = 1000000000.0
)

type EnqueueData struct {
	Queue      string      `json:"queue,omitempty"`
	Class      string      `json:"class"`
	Args       interface{} `json:"args"`
	Jid        string      `json:"jid"`
	EnqueuedAt float64     `json:"enqueued_at"`
	EnqueueOptions
}

type EnqueueOptions struct {
	RetryCount        int          `json:"retry_count,omitempty"`
	Retry             bool         `json:"retry,omitempty"`
	RetryMax          int          `json:"retry_max,omitempty"`
	At                float64      `json:"at,omitempty"`
	RetryOptions      RetryOptions `json:"retry_options,omitempty"`
	ConnectionOptions Options      `json:"connection_options,omitempty"`
}

type RetryOptions struct {
	Exp      int `json:"exp"`
	MinDelay int `json:"min_delay"`
	MaxDelay int `json:"max_delay"`
	MaxRand  int `json:"max_rand"`
}

func generateJid() string {
	// Return 12 random bytes as 24 character hex
	b := make([]byte, 12)
	_, err := io.ReadFull(rand.Reader, b)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", b)
}

func Enqueue(queue, class string, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision()})
}

func EnqueueIn(queue, class string, in float64, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: nowToSecondsWithNanoPrecision() + in})
}

func EnqueueAt(queue, class string, at time.Time, args interface{}) (string, error) {
	return EnqueueWithOptions(queue, class, args, EnqueueOptions{At: timeToSecondsWithNanoPrecision(at)})
}

func EnqueueWithOptions(queue, class string, args interface{}, opts EnqueueOptions) (string, error) {
	now := nowToSecondsWithNanoPrecision()
	ctx := context.Background()

	data := EnqueueData{
		Queue:          queue,
		Class:          class,
		Args:           args,
		Jid:            generateJid(),
		EnqueuedAt:     now,
		EnqueueOptions: opts,
	}

	bytes, err := json.Marshal(data)
	if err != nil {
		return "", err
	}

	if now < opts.At {
		err := enqueueAt(ctx, data.At, bytes)
		return data.Jid, err
	}

	conn := Config.Client.Instance

	_, err = conn.SAdd(ctx, Config.Namespace+"queues", queue).Result()
	if err != nil {
		return "", err
	}

	queue = Config.Namespace + "queue:" + queue
	_, err = conn.LPush(ctx, queue, bytes).Result()
	if err != nil {
		return "", err
	}

	return data.Jid, nil
}

func enqueueAt(ctx context.Context, at float64, bytes []byte) error {
	conn := Config.Client.Instance

	zItem := redis.Z{
		Score:  at,
		Member: bytes,
	}

	_, err := conn.ZAdd(ctx, Config.Namespace+SCHEDULED_JOBS_KEY, zItem).Result()
	if err != nil {
		return err
	}

	return nil
}

func timeToSecondsWithNanoPrecision(t time.Time) float64 {
	return float64(t.UnixNano()) / NanoSecondPrecision
}

func durationToSecondsWithNanoPrecision(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / NanoSecondPrecision
}

func nowToSecondsWithNanoPrecision() float64 {
	return timeToSecondsWithNanoPrecision(time.Now())
}
