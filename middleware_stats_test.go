package workers

import (
	"context"
	"strconv"
	"time"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func MiddlewareStatsSpec(c gospec.Context) {
	ctx := context.Background()
	const queueName = "queue-middleware_stats"

	var job = (func(message *Msg) {
		// noop
	})

	layout := "2006-01-02"
	manager := newManager(queueName, job, 1)
	worker := newWorker(manager)
	message, _ := NewMsg("{\"jid\":\"2\",\"retry\":true}")

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("increments processed stats", func() {
		conn := Config.Client

		count, _ := strconv.Atoi(conn.Get(ctx, "prod:stat:processed").Val())
		dayCount, _ := strconv.Atoi(conn.Get(ctx, "prod:stat:processed:"+time.Now().UTC().Format(layout)).Val())

		c.Expect(count, Equals, 0)
		c.Expect(dayCount, Equals, 0)

		worker.process(message)

		count, _ = strconv.Atoi(conn.Get(ctx, "prod:stat:processed").Val())
		dayCount, _ = strconv.Atoi(conn.Get(ctx, "prod:stat:processed:"+time.Now().UTC().Format(layout)).Val())

		c.Expect(count, Equals, 1)
		c.Expect(dayCount, Equals, 1)
	})

	c.Specify("failed job", func() {
		var job = (func(message *Msg) {
			panic("AHHHH")
		})

		manager := newManager(queueName, job, 1)
		worker := newWorker(manager)

		c.Specify("increments failed stats", func() {
			conn := Config.Client

			count, _ := strconv.Atoi(conn.Get(ctx, "prod:stat:failed").Val())
			dayCount, _ := strconv.Atoi(conn.Get(ctx, "prod:stat:failed:"+time.Now().UTC().Format(layout)).Val())

			c.Expect(count, Equals, 0)
			c.Expect(dayCount, Equals, 0)

			worker.process(message)

			count, _ = strconv.Atoi(conn.Get(ctx, "prod:stat:failed").Val())
			dayCount, _ = strconv.Atoi(conn.Get(ctx, "prod:stat:failed:"+time.Now().UTC().Format(layout)).Val())

			c.Expect(count, Equals, 1)
			c.Expect(dayCount, Equals, 1)
		})
	})

	Config.Namespace = was
}
