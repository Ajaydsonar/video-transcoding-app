package job

// SQLiteStore is the durable Store: same interface as MemoryStore, but
// every mutation is a disk write, so job records survive a crash or
// restart. One table, no migration tooling — CREATE TABLE IF NOT EXISTS
// on open is the whole schema story at this scale.
//
// Concurrency contract (SQLite in one process):
//   - WAL mode: readers (polls, SSE snapshots, listings) never block.
//   - busy_timeout: a writer waits instead of failing with SQLITE_BUSY.
//   - MaxOpenConns(1): a single writer at a time. Progress milestones
//     and status transitions are tiny writes; serializing them is
//     plenty for this workload and eliminates all lock contention.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS jobs (
	id         TEXT PRIMARY KEY,
	kind       TEXT NOT NULL DEFAULT '',
	status     TEXT NOT NULL,
	progress   INTEGER NOT NULL DEFAULT 0,
	raw_key    TEXT NOT NULL DEFAULT '',
	master_key TEXT NOT NULL DEFAULT '',
	outputs    TEXT NOT NULL DEFAULT '[]',
	error      TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs (status, created_at);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs (created_at DESC);
`

// OpenSQLite opens (creating parent dirs and the file as needed) a
// SQLite-backed Store. dsn is a filename; WAL/busy_timeout pragmas are
// applied via query parameters so callers pass a plain path.
func OpenSQLite(dbPath string) (*SQLiteStore, error) {
	dsn := dbPath + "?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=synchronous(NORMAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("job: opening sqlite %s: %w", dbPath, err)
	}
	// One writer connection (see contract above). Reads multiplex on it
	// too — fine, they're all milliseconds.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(sqliteSchema); err != nil {
		db.Close()
		return nil, fmt.Errorf("job: creating schema: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

type SQLiteStore struct {
	db *sql.DB
}

func (s *SQLiteStore) Close() error {
	return s.db.Close()
}

func encodeOutputs(outputs []Output) (string, error) {
	if outputs == nil {
		return "[]", nil
	}
	b, err := json.Marshal(outputs)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func scanJob(row interface {
	Scan(dest ...any) error
}) (Job, error) {
	var j Job
	var kind, status, rawKey, masterKey, outputsJSON, errStr, createdAt, updatedAt string
	var progress int
	if err := row.Scan(
		&j.ID, &kind, &status, &progress, &rawKey, &masterKey,
		&outputsJSON, &errStr, &createdAt, &updatedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, ErrNotFound
		}
		return Job{}, fmt.Errorf("job: scanning row: %w", err)
	}
	j.Kind = Kind(kind)
	j.Status = Status(status)
	j.Progress = progress
	j.RawKey = rawKey
	j.MasterKey = masterKey
	if outputsJSON != "" && outputsJSON != "[]" {
		if err := json.Unmarshal([]byte(outputsJSON), &j.Outputs); err != nil {
			return Job{}, fmt.Errorf("job: decoding outputs for %s: %w", j.ID, err)
		}
	}
	j.Error = errStr
	if t, err := time.Parse(time.RFC3339Nano, createdAt); err == nil {
		j.CreatedAt = t
	}
	if t, err := time.Parse(time.RFC3339Nano, updatedAt); err == nil {
		j.UpdatedAt = t
	}
	return j, nil
}

const sqliteColumns = "id, kind, status, progress, raw_key, master_key, outputs, error, created_at, updated_at"

func (s *SQLiteStore) Create(ctx context.Context, j *Job) error {
	outputs, err := encodeOutputs(j.Outputs)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO jobs (`+sqliteColumns+`) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		j.ID, string(j.Kind), string(j.Status), j.Progress, j.RawKey,
		j.MasterKey, outputs, j.Error,
		j.CreatedAt.UTC().Format(time.RFC3339Nano),
		j.UpdatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return fmt.Errorf("job: creating %s: %w", j.ID, err)
	}
	return nil
}

func (s *SQLiteStore) Get(ctx context.Context, id string) (Job, error) {
	return scanJob(s.db.QueryRowContext(ctx,
		`SELECT `+sqliteColumns+` FROM jobs WHERE id = ?`, id))
}

func (s *SQLiteStore) Update(ctx context.Context, j *Job) error {
	outputs, err := encodeOutputs(j.Outputs)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE jobs SET kind = ?, status = ?, progress = ?,
		 raw_key = ?, master_key = ?, outputs = ?, error = ?,
		 created_at = ?, updated_at = ? WHERE id = ?`,
		string(j.Kind), string(j.Status), j.Progress, j.RawKey,
		j.MasterKey, outputs, j.Error,
		j.CreatedAt.UTC().Format(time.RFC3339Nano),
		j.UpdatedAt.UTC().Format(time.RFC3339Nano),
		j.ID,
	)
	if err != nil {
		return fmt.Errorf("job: updating %s: %w", j.ID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("job: updating %s: %w", j.ID, err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) Delete(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM jobs WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("job: deleting %s: %w", id, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("job: deleting %s: %w", id, err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

func (s *SQLiteStore) queryJobs(ctx context.Context, query string, args ...any) ([]Job, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("job: querying: %w", err)
	}
	defer rows.Close()
	var result []Job
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, j)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("job: iterating: %w", err)
	}
	return result, nil
}

func (s *SQLiteStore) ListOlderThan(ctx context.Context, cutoff time.Time) ([]Job, error) {
	return s.queryJobs(ctx,
		`SELECT `+sqliteColumns+` FROM jobs
		 WHERE status IN ('completed', 'failed') AND created_at < ?
		 ORDER BY created_at ASC`,
		cutoff.UTC().Format(time.RFC3339Nano))
}

func (s *SQLiteStore) ListStaleUploads(ctx context.Context, cutoff time.Time) ([]Job, error) {
	return s.queryJobs(ctx,
		`SELECT `+sqliteColumns+` FROM jobs
		 WHERE status = 'uploading' AND created_at < ?
		 ORDER BY created_at ASC`,
		cutoff.UTC().Format(time.RFC3339Nano))
}

func (s *SQLiteStore) ListRecent(ctx context.Context, limit int) ([]Job, error) {
	query := `SELECT ` + sqliteColumns + ` FROM jobs ORDER BY created_at DESC`
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
	}
	return s.queryJobs(ctx, query)
}

var _ Store = (*SQLiteStore)(nil)
