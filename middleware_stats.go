package workers

import (
	"context"
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	ctx := context.Background()

	defer func() {
		if e := recover(); e != nil {
			incrementStats(ctx, "failed")
			panic(e)
		}
	}()

	acknowledge = next()

	incrementStats(ctx, "processed")

	return
}

func incrementStats(ctx context.Context, metric string) {
	conn := Config.Client.Instance

	today := time.Now().UTC().Format("2006-01-02")

	pipe := conn.TxPipeline()
	pipe.Incr(ctx, Config.Namespace+"stat:"+metric)
	pipe.Incr(ctx, Config.Namespace+"stat:"+metric+":"+today)

	if _, err := pipe.Exec(ctx); err != nil {
		Logger.Errorln("failed to save stats:", err)
	}
}
