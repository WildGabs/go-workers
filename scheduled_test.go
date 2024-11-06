package workers

import (
	"context"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/redis/go-redis/v9"
)

func ScheduledSpec(c gospec.Context) {
	ctx := context.Background()
	scheduled := newScheduled(RETRY_KEY)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("empties retry queues up to the current time", func() {
		conn := Config.Client

		now := nowToSecondsWithNanoPrecision()

		message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
		message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
		message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

		item1 := redis.Z{
			Score:  now - 60.0,
			Member: message1.ToJson(),
		}
		item2 := redis.Z{
			Score:  now - 10.0,
			Member: message2.ToJson(),
		}
		item3 := redis.Z{
			Score:  now + 60.0,
			Member: message3.ToJson(),
		}
		conn.ZAdd(ctx, "prod:"+RETRY_KEY, item1)
		conn.ZAdd(ctx, "prod:"+RETRY_KEY, item2)
		conn.ZAdd(ctx, "prod:"+RETRY_KEY, item3)

		scheduled.poll(ctx)

		defaultCount, _ := conn.LLen(ctx, "prod:queue:default").Result()
		myqueueCount, _ := conn.LLen(ctx, "prod:queue:myqueue").Result()
		pending, _ := conn.ZCard(ctx, "prod:"+RETRY_KEY).Result()

		c.Expect(defaultCount, Equals, int64(1))
		c.Expect(myqueueCount, Equals, int64(1))
		c.Expect(pending, Equals, int64(1))
	})

	Config.Namespace = was
}
