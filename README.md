# Go Workers

**Go Workers** provides background worker functionality in Go, compatible with [Sidekiq](http://sidekiq.org/). It allows you to manage reliable background jobs with advanced features such as retries, custom middleware, and concurrency control.

### Features:
- **Reliable Queueing**: Uses Redis to manage queues, leveraging [BLMOVE](http://redis.io/commands/blmove) for reliable job processing.
- **Job Retries**: Supports automatic retries for failed jobs.
- **Custom Middleware**: Allows the use of custom middleware to process jobs.
- **Concurrency Control**: Customize concurrency per queue.
- **Graceful Shutdown**: Responds to Unix signals to safely wait for jobs to finish before exiting.
- **Job Monitoring**: Provides stats on jobs that are currently running.
- **Well-tested**: Thoroughly tested and reliable.

Compared to v1.2.1, this version contains braking changes:
* Redis Client: The code now uses go-redis v9, replacing the previous use of redigo. This expects an updated Redis universal interface. This change promoves enhanced performance and compatibility to cluster mode.
* Compatible with Redis engine 7.
* Command Update: The BRPOPLPUSH command has been deprecated in favor of the more modern BLMOVE. BLMOVE is now used for reliable queueing and job processing, replacing BRPOPLPUSH for moving elements between Redis lists with blocking behavior.
* Removed another redis client requirement for EnqueueOptions. It will always use default client from configuration.

## Example Usage

Here's an example of how to use Go Workers in your Go application:

```go
package main

import (
	"github.com/topfreegames/go-workers"
	"github.com/redis/go-redis/v9"
	workers "go-workers"
)

func myJob(message *workers.Msg) {
	// do something with your message
	// message.Jid()
	// message.Args() is a wrapper around go-simplejson (http://godoc.org/github.com/bitly/go-simplejson)
}

type myMiddleware struct{}

func (r *myMiddleware) Call(queue string, message *workers.Msg, next func() bool) (acknowledge bool) {
	// do something before each message is processed
	acknowledge = next()
	// do something after each message is processed
	return
}

func main() {

	redisClient := redis.NewClient(&redis.Options{
		Addr:        "localhost:6379",
		PoolSize:    10,
		ReadTimeout: time.Second,
		
	})

	// OR Cluster client. You MUST setup a namespace with braces to avoid CROSSLOT errors.
	//redisClient := redis.NewClusterClient(&redis.Options{
	//	Addr:        "localhost:6379",
	//	PoolSize:    1,
	//	ReadTimeout: time.Second,
	//  Namespace: "{worker}",
	//})

	workers.Configure(workers.Options{
		// redis client
		RedisClient: redisClient,
		// seconds between poll attempts
		PoolInterval: "30",
		// unique process id for this instance of workers (for proper recovery of inprogress jobs on crash)
		ProcessID: "1",
	})

	workers.Middleware.Append(&myMiddleware{})

	// pull messages from "myqueue" with concurrency of 10
	workers.Process("myqueue", myJob, 10)

	// pull messages from "myqueue2" with concurrency of 20
	workers.Process("myqueue2", myJob, 20)

	// Add a job to a queue
	workers.Enqueue("myqueue3", "Add", []int{1, 2})

	// Add a job to a queue with retry
	workers.EnqueueWithOptions("myqueue3", "Add", []int{1, 2}, workers.EnqueueOptions{Retry: true})

	// Add a job to a queue in a different redis instance
	workers.EnqueueWithOptions("myqueue4", "Add", []int{1, 2},
		workers.EnqueueOptions{
			Retry: true,
		},
	)

	// stats will be available at http://localhost:8080/stats
	go workers.StatsServer(8080)

	// Blocks until process is told to exit via unix signal
	workers.Run()
}
```