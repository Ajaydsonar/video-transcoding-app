// Package config centralizes how the app reads its runtime configuration.
// Keeping this in one place means nothing else in the codebase calls
// os.Getenv directly — easier to see every knob the app has, in one file.
package config

import (
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Port  string // HTTP port the server listens on
	Env   string // "development" | "production"
	Level string

	// StorageBackend selects which Storage implementation main.go wires
	// up: "local" (default, disk-based, no external account needed) or
	// "b2" (Backblaze B2, needs every B2* field below set).
	StorageBackend string
	B2Endpoint     string
	B2Region       string
	B2Bucket       string
	B2KeyID        string
	B2AppKey       string
}

// Load reads config from environment variables, falling back to sane
// defaults for local development. It returns an error instead of panicking
// so callers (main.go) decide how to handle a bad config.
func Load() (*Config, error) {
	if err := godotenv.Load(); err != nil {
		log.Println("warning: .env file not found")
	}

	cfg := &Config{
		Port:  getEnv("PORT", "8080"),
		Env:   getEnv("APP_ENV", "development"),
		Level: "debug",

		StorageBackend: getEnv("STORAGE_BACKEND", "local"),
		B2Endpoint:     getEnv("B2_ENDPOINT", ""),
		B2Region:       getEnv("B2_REGION", ""),
		B2Bucket:       getEnv("B2_BUCKET", ""),
		B2KeyID:        getEnv("B2_KEY_ID", ""),
		B2AppKey:       getEnv("B2_APPLICATION_KEY", ""),
	}

	if cfg.Port == "" {
		return nil, fmt.Errorf("config: PORT must not be empty")
	}

	// Fail fast at startup, not on the first upload — a missing B2
	// credential should never surface as a confusing runtime error deep
	// inside a worker goroutine three requests later.
	if cfg.StorageBackend == "b2" {
		missing := []string{}
		for name, val := range map[string]string{
			"B2_ENDPOINT": cfg.B2Endpoint, "B2_REGION": cfg.B2Region,
			"B2_BUCKET": cfg.B2Bucket, "B2_KEY_ID": cfg.B2KeyID,
			"B2_APPLICATION_KEY": cfg.B2AppKey,
		} {
			if val == "" {
				missing = append(missing, name)
			}
		}
		if len(missing) > 0 {
			return nil, fmt.Errorf("config: STORAGE_BACKEND=b2 requires %v", missing)
		}
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
