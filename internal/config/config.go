// Package config centralizes how the app reads its runtime configuration.
// Keeping this in one place means nothing else in the codebase calls
// os.Getenv directly — easier to see every knob the app has, in one file.
package config

import (
	"fmt"
	"os"
)

type Config struct {
	Port  string // HTTP port the server listens on
	Env   string // "development" | "production"
	Level string
}

// Load reads config from environment variables, falling back to sane
// defaults for local development. It returns an error instead of panicking
// so callers (main.go) decide how to handle a bad config.
func Load() (*Config, error) {
	cfg := &Config{
		Port:  getEnv("PORT", "8080"),
		Env:   getEnv("APP_ENV", "development"),
		Level: "debug",
	}

	if cfg.Port == "" {
		return nil, fmt.Errorf("config: PORT must not be empty")
	}

	return cfg, nil
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
