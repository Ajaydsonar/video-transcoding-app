// Package events
package events

import (
	"errors"
	"sync"
)

var ErrSubscribed = errors.New("events: job already has an active listener")

type Update struct {
	JobID    string `json:"jobId"`
	Status   string `json:"status"`
	Progress int    `json:"progress"`
}

// Broker fans out job progress to ONE listener per job. A second Subscribe
// attempt for the same job is rejected so a video's progress is only ever
// streamed to a single client.
type Broker struct {
	mu          sync.Mutex
	subscribers map[string]chan Update // jobId -> the single active listener
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string]chan Update),
	}
}

// Subscribe registers one listener for jobID. Returns ErrSubscribed if a
// listener is already active. You MUST call the returned func (defer it),
// or the job's slot stays locked forever.
func (b *Broker) Subscribe(jobID string) (chan Update, func(), error) {
	c := make(chan Update, 4)

	b.mu.Lock()
	if _, ok := b.subscribers[jobID]; ok {
		b.mu.Unlock()
		return nil, nil, ErrSubscribed
	}
	b.subscribers[jobID] = c
	b.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			b.mu.Lock()
			if b.subscribers[jobID] == c {
				delete(b.subscribers, jobID)
			}
			b.mu.Unlock()
			close(c)
		})
	}

	return c, unsubscribe, nil
}

func (b *Broker) Publish(update Update) {
	b.mu.Lock()
	ch, ok := b.subscribers[update.JobID]
	b.mu.Unlock()
	if !ok {
		return
	}

	select {
	case ch <- update:
	default:
		// Listener is slow and its buffer is full: drop this tick.
		// Terminal events are recovered by the SSE handler's self-heal.
	}
}
