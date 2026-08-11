// Package queue
package queue

import (
	"context"
)

type Channel struct {
	ch chan string
}

func NewChannel(bufferSize int) *Channel {
	ch := make(chan string, bufferSize)
	return &Channel{
		ch: ch,
	}
}

func (c *Channel) Enqueue(ctx context.Context, jobID string) error {
	select {
	case c.ch <- jobID:
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Channel) Dequeue(ctx context.Context) (string, error) {
	select {
	case id := <-c.ch:
		return id, nil
	case <-ctx.Done():
		return "Dequeue Canceled", ctx.Err()
	}
}

func (c *Channel) Len() int {
	return len(c.ch) // safe to call concurrently — len() on a channel is a lock-free, O(1) read
}

var _ Queue = (*Channel)(nil)
