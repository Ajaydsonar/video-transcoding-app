// Package job
package job

import "time"

// Status is a string-backed enum. Go has no native enum type — this is the
// idiomatic substitute: a named type over string, with a fixed set of
// valid values declared as constants below.
type Status string

const (
	StatusPending    Status = "pending"    // uploaded, not yet picked up by a worker
	StatusProcessing Status = "processing" // a worker is actively transcoding it
	StatusCompleted  Status = "completed"  // all resolutions ready
	StatusFailed     Status = "failed"
)

// Output describes one transcoded rendition that's ready to be downloaded.
type Output struct {
	Name      string `json:"name"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
	SizeBytes int64  `json:"sizeBytes"`
}

// Job tracks one video through the whole pipeline: upload -> transcode ->
// ready. It's the single source of truth the API reads from to answer
// "what's the status of my video?"
type Job struct {
	ID        string    `json:"id"`
	Status    Status    `json:"status"`
	Progress  int       `json:"progress"` // 0-100, updated by the worker in a later step
	RawKey    string    `json:"rawKey"`   // storage key of the uploaded original
	Outputs   []Output  `json:"outputs,omitempty"`
	Error     string    `json:"error,omitempty"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}
