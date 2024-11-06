package workers

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"strings"
	"time"
)

type scheduled struct {
	keys   []string
	closed chan bool
	exit   chan bool
}

func (s *scheduled) start(ctx context.Context) {
	go (func() {
		for {
			select {
			case <-s.closed:
				return
			default:
			}

			s.poll(ctx)

			time.Sleep(time.Duration(Config.PoolInterval) * time.Second)
		}
	})()
}

func (s *scheduled) quit() {
	close(s.closed)
}

func (s *scheduled) poll(ctx context.Context) {
	conn := Config.Client

	now := nowToSecondsWithNanoPrecision()

	for _, key := range s.keys {
		key = Config.Namespace + key
		for {
			opt := &redis.ZRangeBy{
				Min:    "-inf",
				Max:    fmt.Sprintf("%f", now),
				Offset: 0,
				Count:  1,
			}

			messages, _ := conn.ZRangeByScore(ctx, key, opt).Result()
			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := conn.ZRem(ctx, key, messages[0]).Result(); removed > 0 {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, Config.Namespace)
				message.Set("enqueued_at", nowToSecondsWithNanoPrecision())

				conn.LPush(ctx, Config.Namespace+"queue:"+queue, message.ToJson())
			}
		}
	}
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{keys, make(chan bool), make(chan bool)}
}
