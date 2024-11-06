package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/redis/go-redis/v9"
)

func ConfigSpec(c gospec.Context) {
	var recoverOnPanic = func(f func()) (err interface{}) {
		defer func() {
			if cause := recover(); cause != nil {
				err = cause
			}
		}()

		f()

		return
	}

	c.Specify("can specify custom process", func() {
		c.Expect(Config.processId, Equals, "1")

		redisClient := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})

		Configure(Options{
			RedisClient: redisClient,
			ProcessID:   "2",
		})

		c.Expect(Config.processId, Equals, "2")
	})

	c.Specify("requires a redis parameter", func() {
		err := recoverOnPanic(func() {
			Configure(Options{ProcessID: "2"})
		})

		c.Expect(err, Equals, "Configure requires a redis client interface")
	})

	c.Specify("requires a process parameter", func() {
		redisClient := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})

		err := recoverOnPanic(func() {
			Configure(Options{RedisClient: redisClient})
		})

		c.Expect(err, Equals, "Configure requires a 'ProcessID' option, which uniquely identifies this instance")
	})

	c.Specify("adds ':' to the end of the namespace", func() {
		c.Expect(Config.Namespace, Equals, "")

		redisClient := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})

		Configure(Options{
			RedisClient: redisClient,
			ProcessID:   "1",
			Namespace:   "prod",
		})

		c.Expect(Config.Namespace, Equals, "prod:")
	})

	c.Specify("defaults poll interval to 15 seconds", func() {
		redisClient := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})

		Configure(Options{
			RedisClient: redisClient,
			ProcessID:   "1",
		})

		c.Expect(Config.PoolInterval, Equals, 15)
	})

	c.Specify("allows customization of poll interval", func() {
		redisClient := redis.NewClient(&redis.Options{
			Addr: "localhost:6379",
		})

		Configure(Options{
			RedisClient:  redisClient,
			ProcessID:    "1",
			PoolInterval: 1,
		})

		c.Expect(Config.PoolInterval, Equals, 1)
	})
}
