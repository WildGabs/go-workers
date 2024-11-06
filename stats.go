package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"net/http"
	"strconv"
)

type stats struct {
	Processed int         `json:"processed"`
	Failed    int         `json:"failed"`
	Jobs      interface{} `json:"jobs"`
	Enqueued  interface{} `json:"enqueued"`
	Retries   int64       `json:"retries"`
}

// Stats writes stats on response writer
func Stats(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	stats := getStats(ctx)

	body, _ := json.MarshalIndent(stats, "", "  ")
	fmt.Fprintln(w, string(body))
}

// WorkerStats holds workers stats
type WorkerStats struct {
	Processed int               `json:"processed"`
	Failed    int               `json:"failed"`
	Enqueued  map[string]string `json:"enqueued"`
	Retries   int64             `json:"retries"`
}

// GetStats returns workers stats
func GetStats() *WorkerStats {
	ctx := context.TODO()

	stats := getStats(ctx)
	enqueued := map[string]string{}
	if statsEnqueued, ok := stats.Enqueued.(map[string]string); ok {
		enqueued = statsEnqueued
	}

	return &WorkerStats{
		Processed: stats.Processed,
		Failed:    stats.Failed,
		Retries:   stats.Retries,
		Enqueued:  enqueued,
	}
}

func getStats(ctx context.Context) stats {
	jobs := make(map[string][]*map[string]interface{})
	enqueued := make(map[string]string)

	for _, m := range managers {
		queue := m.queueName()
		jobs[queue] = make([]*map[string]interface{}, 0)
		enqueued[queue] = ""
		for _, worker := range m.workers {
			message := worker.currentMsg
			startedAt := worker.startedAt

			if message != nil && startedAt > 0 {
				jobs[queue] = append(jobs[queue], &map[string]interface{}{
					"message":    message,
					"started_at": startedAt,
				})
			}
		}
	}

	stats := stats{
		0,
		0,
		jobs,
		enqueued,
		0,
	}

	conn := Config.Client

	pipe := conn.TxPipeline()
	pipe.Get(ctx, Config.Namespace+"stat:processed")
	pipe.Get(ctx, Config.Namespace+"stat:failed")
	pipe.ZCard(ctx, Config.Namespace+RETRY_KEY)

	for key := range enqueued {
		pipe.LLen(ctx, fmt.Sprintf("%squeue:%s", Config.Namespace, key))
	}

	results, err := pipe.Exec(ctx)
	if err != nil {
		Logger.Errorln("failed to retrieve stats:", err)
	}

	if len(results) == (3 + len(enqueued)) {
		for index, result := range results {
			if index == 0 && result != nil {
				srtResult := result.(*redis.StringCmd)
				stats.Processed, _ = strconv.Atoi(srtResult.Val())
				continue
			}
			if index == 1 && result != nil {
				srtResult := result.(*redis.StringCmd)
				stats.Failed, _ = strconv.Atoi(srtResult.Val())
				continue
			}

			if index == 2 && result != nil {
				intResult := result.(*redis.IntCmd)
				stats.Retries = intResult.Val()
				continue
			}

			queueIndex := 0
			for key := range enqueued {
				if queueIndex == (index - 3) {
					intResult := result.(*redis.IntCmd)
					enqueued[key] = fmt.Sprintf("%d", intResult.Val())
				}
				queueIndex++
			}
		}
	}

	return stats
}
