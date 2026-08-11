package queue

import "context"

type Queue interface {
	Enqueue(ctx context.Context, jobID string) error
	Dequeue(ctx context.Context) (string, error)
	// Len reports how many jobs are currently buffered, waiting for a
	// worker. Used for backpressure — reject new uploads before they're
	// accepted at all if the queue is already too deep.
	Len() int
}
