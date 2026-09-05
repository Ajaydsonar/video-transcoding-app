package job

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"
)

// ErrNotFound is a sentinel error — a specific, comparable error value
// callers can check for with errors.Is, instead of string-matching error
// messages. You'll define one of these per "expected failure" in a
// well-behaved Go codebase.
var ErrNotFound = errors.New("job: not found")

// Store abstracts job persistence, same idea as the Storage interface.
// MemoryStore below is fine for local dev; internal/job/postgres.go (a
// future step) will implement this against Postgres for production —
// again, without any handler code changing.
type Store interface {
	Create(ctx context.Context, j *Job) error
	Get(ctx context.Context, id string) (Job, error)
	Update(ctx context.Context, j *Job) error
	Delete(ctx context.Context, id string) error
	// ListOlderThan returns jobs eligible for cleanup: created before
	// cutoff AND in a terminal state (completed or failed). Implementations
	// must never return pending/processing jobs here, no matter their
	// age — deleting an active job's files out from under a running
	// worker would be far worse than a slow cleanup.
	ListOlderThan(ctx context.Context, cutoff time.Time) ([]Job, error)
	// ListStaleUploads returns non-terminal, never-validated upload
	// sessions (status "uploading") created before cutoff — clients that
	// fetched a presigned URL but never finished (or never called
	// complete). The caller deletes their partial objects and records.
	ListStaleUploads(ctx context.Context, cutoff time.Time) ([]Job, error)
	// ListRecent returns the newest jobs first, capped at limit (callers
	// should clamp limit to something sane like 50). Used for the
	// "recent uploads" listing — no status filter here, the handler
	// decides what to show.
	ListRecent(ctx context.Context, limit int) ([]Job, error)
}

// MemoryStore holds jobs in a plain map. Multiple goroutines will hit this
// concurrently (an HTTP handler creating a job while a worker goroutine
// updates another), so every access is guarded by a mutex.
type MemoryStore struct {
	mu   sync.RWMutex
	jobs map[string]*Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{jobs: make(map[string]*Job)}
}

func (s *MemoryStore) Create(ctx context.Context, j *Job) error {
	s.mu.Lock() // exclusive lock: we're writing
	defer s.mu.Unlock()
	s.jobs[j.ID] = j
	return nil
}

func (s *MemoryStore) Get(ctx context.Context, id string) (Job, error) {
	s.mu.RLock() // shared lock: many readers can hold this at once, just no writer
	defer s.mu.RUnlock()
	j, ok := s.jobs[id]
	if !ok {
		return Job{}, ErrNotFound
	}
	return *j, nil
}

func (s *MemoryStore) Update(ctx context.Context, j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.ID]; !ok {
		return ErrNotFound
	}
	s.jobs[j.ID] = j
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[id]; !ok {
		return ErrNotFound
	}
	delete(s.jobs, id)
	return nil
}

func (s *MemoryStore) ListOlderThan(ctx context.Context, cutoff time.Time) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Job
	for _, j := range s.jobs {
		terminal := j.Status == StatusCompleted || j.Status == StatusFailed
		if terminal && j.CreatedAt.Before(cutoff) {
			result = append(result, *j)
		}
	}
	return result, nil
}

func (s *MemoryStore) ListStaleUploads(ctx context.Context, cutoff time.Time) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Job
	for _, j := range s.jobs {
		if j.Status == StatusUploading && j.CreatedAt.Before(cutoff) {
			result = append(result, *j)
		}
	}
	return result, nil
}

func (s *MemoryStore) ListRecent(ctx context.Context, limit int) ([]Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		result = append(result, *j)
	}
	sort.Slice(result, func(a, b int) bool {
		return result[a].CreatedAt.After(result[b].CreatedAt)
	})
	if limit > 0 && len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

var _ Store = (*MemoryStore)(nil) // compile-time interface check, same trick as storage/local.go
