package workers

import (
	redis "github.com/topfreegames/extensions/v9/redis/experimental"
)

type Options struct {
	RedisClient  *redis.Client
	Namespace    string
	ProcessID    string
	PoolInterval int
}

type WorkerConfig struct {
	processId    string
	Namespace    string
	PoolInterval int
	Client       *redis.Client
	Fetch        func(queue string) Fetcher
}

var Config *WorkerConfig

func Configure(options Options) {
	var namespace string

	if options.RedisClient == nil {
		panic("Configure requires a 'RedisClient' option, which identifies a Redis instance")
	}
	if options.ProcessID == "" {
		panic("Configure requires a 'ProcessID' option, which uniquely identifies this instance")
	}
	if options.Namespace != "" {
		namespace = options.Namespace + ":"
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
