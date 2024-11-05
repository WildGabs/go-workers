package workers

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"time"
)

type Fetcher interface {
	Queue() string
	Fetch()
	Acknowledge(*Msg)
	Ready() chan bool
	FinishedWork() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
}

type fetch struct {
	queue        string
	ready        chan bool
	finishedwork chan bool
	messages     chan *Msg
	stop         chan bool
	exit         chan bool
	closed       chan bool
}

func NewFetch(queue string, messages chan *Msg, ready chan bool) Fetcher {
	return &fetch{
		queue,
		ready,
		make(chan bool),
		messages,
		make(chan bool),
		make(chan bool),
		make(chan bool),
	}
}

func (f *fetch) Queue() string {
	return f.queue
}

func (f *fetch) processOldMessages(ctx context.Context) {
	messages := f.inprogressMessages(ctx)

	for _, message := range messages {
		<-f.Ready()
		f.sendMessage(message)
	}
}

func (f *fetch) Fetch() {
	ctx := context.Background()
	f.processOldMessages(ctx)

	go func() {
		for {
			// f.Close() has been called
			if f.Closed() {
				break
			}
			<-f.Ready()
			f.tryFetchMessage(ctx)
		}
	}()

	for {
		select {
		case <-f.stop:
			// Stop the redis-polling goroutine
			close(f.closed)
			// Signal to Close() that the fetcher has stopped
			close(f.exit)
			break
		}
	}
}

func (f *fetch) tryFetchMessage(ctx context.Context) {
	conn := Config.Client.Instance

	message, err := conn.BLMove(ctx, f.queue, f.inprogressQueue(), "right", "left", 1).Result()
	if err != nil {
		// If redis returns null, the queue is empty. Just ignore the error.
		if err.Error() != redis.Nil.Error() {
			Logger.Errorln("failed to fetch message", err)
			time.Sleep(1 * time.Second)
		}
	} else {
		f.sendMessage(message)
	}
}

func (f *fetch) sendMessage(message string) {
	msg, err := NewMsg(message)

	if err != nil {
		Logger.Errorln("failed to create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *fetch) Acknowledge(message *Msg) {
	ctx := context.Background()
	conn := Config.Client.Instance

	conn.LRem(ctx, f.inprogressQueue(), -1, message.OriginalJson())
}

func (f *fetch) Messages() chan *Msg {
	return f.messages
}

func (f *fetch) Ready() chan bool {
	return f.ready
}

func (f *fetch) FinishedWork() chan bool {
	return f.finishedwork
}

func (f *fetch) Close() {
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	select {
	case <-f.closed:
		return true
	default:
		return false
	}
}

func (f *fetch) inprogressMessages(ctx context.Context) []string {
	conn := Config.Client.Instance

	messages, err := conn.LRange(ctx, f.inprogressQueue(), 0, -1).Result()
	if err != nil {
		Logger.Errorln("failed to fetch messages in progress", err)
	}

	return messages
}

func (f *fetch) inprogressQueue() string {
	return fmt.Sprint(f.queue, ":", Config.processId, ":inprogress")
}
