// Package logger ok
package logger

import (
	"log/slog"
	"os"
	"strings"

	"video-pipeline/internal/config"
)

func Init(cfg config.Config) {
	var level slog.Level
	switch strings.ToLower(cfg.Level) {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: level}
	var handler slog.Handler

	if strings.ToLower(cfg.Env) == "prod" {
		// JSON structured logging for production aggregation tools (Datadog, ELK)
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		// Human-readable text format for local development
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	// Set this handler as the global logger
	logger := slog.New(handler)
	slog.SetDefault(logger)
}
