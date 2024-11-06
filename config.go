package workers

import (
	"github.com/redis/go-redis/v9"
)

type Options struct {
	RedisClient  redis.UniversalClient
	Namespace    string
	ProcessID    string
	PoolInterval int
}

type WorkerConfig struct {
	processId    string
	Namespace    string
	PoolInterval int
	Client       redis.UniversalClient
	Fetch        func(queue string) Fetcher
}

var Config *WorkerConfig

func Configure(options Options) {
	var namespace string

	if options.RedisClient == nil {
		panic("Configure requires a redis client interface")
	}
	if options.ProcessID == "" {
		panic("Configure requires a 'ProcessID' option, which uniquely identifies this instance")
	}
	if options.Namespace != "" {
		namespace = options.Namespace + ":"
	}
	if options.PoolInterval == 0 {
		options.PoolInterval = 15
	}

	Config = &WorkerConfig{
		options.ProcessID,
		namespace,
		options.PoolInterval,
		options.RedisClient,
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}
