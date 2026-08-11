// Package events
package events

import "sync"

type Update struct {
	JobID    string `json:"jobId"`
	Status   string `json:"status"`
	Progress int    `json:"progress"`
}

type Broker struct {
	mu          sync.Mutex
	subscribers map[string][]chan Update // jonId -> jobs listner
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string][]chan Update),
	}
}

// Subscribe registers a new channel for jobID. Returns the channel to read
// from, and an unsubscribe func the caller MUST defer-call, or you leak a
// channel (and a slot in the map slice) forever.
func (b *Broker) Subscribe(jobID string) (chan Update, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	c := make(chan Update, 4)

	b.subscribers[jobID] = append(b.subscribers[jobID], c)

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			b.mu.Lock()
			subs := b.subscribers[jobID]
			for i, s := range subs {
				if s == c {
					// remove this one; keep the rest
					b.subscribers[jobID] = append(subs[:i], subs[i+1:]...)
					if len(b.subscribers[jobID]) == 0 {
						delete(b.subscribers, jobID)
					}
					break
				}
			}
			b.mu.Unlock()
			close(c)
		})
	}

	return c, unsubscribe
}

func (b *Broker) Publish(update Update) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ch := range b.subscribers[update.JobID] {
		select {
		case ch <- update:
			//
		default:

		}
	}
}
