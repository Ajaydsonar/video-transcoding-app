package storage

import (
	"context"
	"errors"
	"io"
	"time"
)

// ErrNotFound reports a missing object. HeadObject/Get equivalents map
// their backend-specific "no such key" errors onto this so callers can
// use errors.Is instead of string-matching provider messages.
var ErrNotFound = errors.New("storage: not found")

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

	// Delete removes the object at key. Implementations should treat
	// deleting an already-missing key as success, not an error — the
	// caller's goal ("this key shouldn't exist") is already satisfied.
	Delete(ctx context.Context, key string) error

	// DeletePrefix removes every object under prefix (e.g. a whole
	// "processed/<job-id>/hls/" tree). Deleting a prefix that matches
	// nothing is success, not an error.
	DeletePrefix(ctx context.Context, prefix string) error

	// PresignPut returns a URL the client can PUT the object to directly,
	// without routing bytes through this server. The URL is valid for
	// (at most) expiry. Backends that can't hand out upload URLs
	// (e.g. Local) return an error — callers should surface "direct
	// upload unsupported" instead of falling back silently.
	PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error)

	// Stat returns the object's size in bytes, or ErrNotFound wrapped
	// when the key doesn't exist.
	Stat(ctx context.Context, key string) (int64, error)
}
