package workers

import (
	"context"
	"encoding/json"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func EnqueueSpec(c gospec.Context) {
	ctx := context.Background()
	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("Enqueue", func() {
		conn := Config.Client

		c.Specify("makes the queue available", func() {
			enqueue, err := Enqueue("enqueue1", "Add", []int{1, 2})
			if err != nil {
				panic(err)
			}

			c.Expect(enqueue, Not(Equals), "")
			found, _ := conn.SIsMember(ctx, "prod:queues", "enqueue1").Result()
			c.Expect(found, IsTrue)
		})

		c.Specify("adds a job to the queue", func() {
			nb, _ := conn.LLen(ctx, "prod:queue:enqueue2").Result()
			c.Expect(nb, Equals, int64(0))

			Enqueue("enqueue2", "Add", []int{1, 2})

			nb, _ = conn.LLen(ctx, "prod:queue:enqueue2").Result()
			c.Expect(nb, Equals, int64(1))
		})

		c.Specify("saves the arguments", func() {
			Enqueue("enqueue3", "Compare", []string{"foo", "bar"})

			strResult, _ := conn.LPop(ctx, "prod:queue:enqueue3").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(strResult), &result)
			c.Expect(result["class"], Equals, "Compare")

			args := result["args"].([]interface{})
			c.Expect(len(args), Equals, 2)
			c.Expect(args[0], Equals, "foo")
			c.Expect(args[1], Equals, "bar")
		})

		c.Specify("has a jid", func() {
			Enqueue("enqueue4", "Compare", []string{"foo", "bar"})

			strResult, _ := conn.LPop(ctx, "prod:queue:enqueue4").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(strResult), &result)
			c.Expect(result["class"], Equals, "Compare")

			jid := result["jid"].(string)
			c.Expect(len(jid), Equals, 24)
		})

		c.Specify("has enqueued_at that is close to now", func() {
			Enqueue("enqueue5", "Compare", []string{"foo", "bar"})

			strResult, _ := conn.LPop(ctx, "prod:queue:enqueue5").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(strResult), &result)
			c.Expect(result["class"], Equals, "Compare")

			ea := result["enqueued_at"].(float64)
			c.Expect(ea, Not(Equals), 0)
			c.Expect(ea, IsWithin(0.1), nowToSecondsWithNanoPrecision())
		})

		c.Specify("has retry and retry_max when set", func() {
			EnqueueWithOptions("enqueue6", "Compare", []string{"foo", "bar"}, EnqueueOptions{RetryMax: 13, Retry: true})

			strResult, _ := conn.LPop(ctx, "prod:queue:enqueue6").Result()
			var result map[string]interface{}
			json.Unmarshal([]byte(strResult), &result)
			c.Expect(result["class"], Equals, "Compare")

			retry := result["retry"].(bool)
			c.Expect(retry, Equals, true)

			retryMax := int(result["retry_max"].(float64))
			c.Expect(retryMax, Equals, 13)
		})

		c.Specify("has retry_options when set", func() {
			EnqueueWithOptions(
				"enqueue7", "Compare", []string{"foo", "bar"},
				EnqueueOptions{
					RetryMax: 13,
					Retry:    true,
					RetryOptions: RetryOptions{
						Exp:      2,
						MinDelay: 0,
						MaxDelay: 60,
						MaxRand:  30,
					},
				})

			strResult, _ := conn.LPop(ctx, "prod:queue:enqueue7").Result()
			var result map[string]interface{}
			err := json.Unmarshal([]byte(strResult), &result)
			if err != nil {
				panic(err)
			}
			c.Expect(result["class"], Equals, "Compare")

			retryOptions := result["retry_options"].(map[string]interface{})
			c.Expect(len(retryOptions), Equals, 4)
			c.Expect(retryOptions["exp"].(float64), Equals, float64(2))
			c.Expect(retryOptions["min_delay"].(float64), Equals, float64(0))
			c.Expect(retryOptions["max_delay"].(float64), Equals, float64(60))
			c.Expect(retryOptions["max_rand"].(float64), Equals, float64(30))
		})
	})

	c.Specify("EnqueueIn", func() {
		scheduleQueue := "prod:" + SCHEDULED_JOBS_KEY
		conn := Config.Client

		c.Specify("has added a job in the scheduled queue", func() {
			_, err := EnqueueIn("enqueuein1", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			scheduledCount, _ := conn.ZCard(ctx, scheduleQueue).Result()
			c.Expect(scheduledCount, Equals, int64(1))

			conn.Del(ctx, scheduleQueue)
		})

		c.Specify("has the correct 'queue'", func() {
			_, err := EnqueueIn("enqueuein2", "Compare", 10, map[string]interface{}{"foo": "bar"})
			c.Expect(err, Equals, nil)

			var data EnqueueData
			elem, err := conn.ZRange(ctx, scheduleQueue, 0, -1).Result()
			json.Unmarshal([]byte(elem[0]), &data)

			c.Expect(data.Queue, Equals, "enqueuein2")

			conn.Del(ctx, scheduleQueue)
		})
	})

	Config.Namespace = was
}
