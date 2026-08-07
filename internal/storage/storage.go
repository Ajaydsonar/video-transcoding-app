package storage

import (
	"context"
	"io"
)

// Storage abstracts "put a blob somewhere, get it back later." Handlers
// and workers only ever talk to this interface — swapping Local for R2
// later (production) means writing ONE new file that satisfies this
// interface, and changing ONE line in main.go. Nothing else changes.
//
// This is what "implicit interface satisfaction" buys you in Go: Local
// below doesn't declare "I implement Storage" anywhere — it just happens
// to have a matching Put method, and that's enough.
type Storage interface {
	// Put streams r into the given key. We take an io.Reader (not a
	// []byte) so callers can stream a multi-GB upload straight through
	// to disk/S3 without ever holding the whole file in memory.
	Put(ctx context.Context, key string, r io.Reader) error

	// Get returns a reader for the given key. Caller must Close it.
	Get(ctx context.Context, key string) (io.ReadCloser, error)
}
