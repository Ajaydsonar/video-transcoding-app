// Package storage
package storage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

// Local implements Storage by writing to a directory on disk. Good enough
// for local dev and for running this whole pipeline on a single free-tier
// VM. internal/storage/r2.go (a future step) implements the exact same
// Storage interface against Cloudflare R2 for production.
type Local struct {
	baseDir string
}

func NewLocal(baseDir string) (*Local, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("storage: creating base dir %s: %w", baseDir, err)
	}
	return &Local{baseDir: baseDir}, nil
}

func (l *Local) Put(ctx context.Context, key string, r io.Reader) error {
	dest := filepath.Join(l.baseDir, key)

	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return fmt.Errorf("storage: creating dir for %s: %w", key, err)
	}

	f, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("storage: creating file %s: %w", key, err)
	}
	defer f.Close() // guaranteed to run even if io.Copy fails below

	if _, err := io.Copy(f, r); err != nil {
		return fmt.Errorf("storage: writing %s: %w", key, err)
	}
	return nil
}

func (l *Local) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	f, err := os.Open(filepath.Join(l.baseDir, key))
	if err != nil {
		return nil, fmt.Errorf("storage: opening %s: %w", key, err)
	}
	return f, nil
}

func (l *Local) Delete(ctx context.Context, key string) error {
	if err := os.Remove(filepath.Join(l.baseDir, key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("storage: deleting %s: %w", key, err)
	}
	return nil
}

func (l *Local) DeletePrefix(ctx context.Context, prefix string) error {
	if err := os.RemoveAll(filepath.Join(l.baseDir, filepath.FromSlash(prefix))); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("storage: deleting prefix %s: %w", prefix, err)
	}
	return nil
}

// PresignPut can't exist for disk storage — there's no URL a browser
// could PUT to that lands on this machine's filesystem. The upload
// endpoints must refuse direct upload with 501 when Local is wired up,
// not pretend it worked.
func (l *Local) PresignPut(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return "", fmt.Errorf("storage: local backend: %w", ErrPresignUnsupported)
}

func (l *Local) Stat(ctx context.Context, key string) (int64, error) {
	info, err := os.Stat(filepath.Join(l.baseDir, filepath.FromSlash(key)))
	if err != nil {
		if os.IsNotExist(err) {
			return 0, fmt.Errorf("storage: stat %s: %w", key, ErrNotFound)
		}
		return 0, fmt.Errorf("storage: stat %s: %w", key, err)
	}
	return info.Size(), nil
}

// This line does nothing at runtime — it's a compile-time check. If Local
// ever stops satisfying the Storage interface (e.g. someone renames Put),
// this line fails to compile with a clear error, right here, instead of
// the error surfacing confusingly wherever Local gets used as a Storage.
var _ Storage = (*Local)(nil)
