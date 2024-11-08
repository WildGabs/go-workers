package workers

import (
	"context"
	"testing"
	"time"

	"github.com/customerio/gospec"
	"github.com/redis/go-redis/v9"
)

// You will need to list every spec in a TestXxx method like this,
// so that gotest can be used to run the specs. Later GoSpec might
// get its own command line tool similar to gotest, but for now this
// is the way to go. This shouldn't require too much typing, because
// there will be typically only one top-level spec per class/feature.

func TestAllSpecs(t *testing.T) {
	r := gospec.NewRunner()
	ctx := context.Background()

	r.Parallel = false

	redisClient := redis.NewClient(&redis.Options{
		Addr:        "localhost:6379",
		PoolSize:    1,
		ReadTimeout: time.Second,
	})

	defer func() {
		err := redisClient.Close()
		if err != nil {
			panic("failed close connection: " + err.Error())
		}
	}()

	r.BeforeEach = func() {
		Configure(Options{
			RedisClient: redisClient,
			ProcessID:   "1",
		})

		conn := Config.Client
		_, err := conn.FlushDB(ctx).Result()
		if err != nil {
			panic("failed to flush db: " + err.Error())
		}
	}

	// List all specs here
	r.AddSpec(WorkersSpec)
	r.AddSpec(ConfigSpec)
	r.AddSpec(MsgSpec)
	r.AddSpec(FetchSpec)
	r.AddSpec(WorkerSpec)
	r.AddSpec(ManagerSpec)
	r.AddSpec(ScheduledSpec)
	r.AddSpec(EnqueueSpec)
	r.AddSpec(MiddlewareSpec)
	r.AddSpec(MiddlewareRetrySpec)
	r.AddSpec(MiddlewareStatsSpec)

	// Run GoSpec and report any errors to gotest's `testing.T` instance
	gospec.MainGoTest(r, t)
}
