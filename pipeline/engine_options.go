package pipeline

import (
	"log/slog"
	"os"
)

type engineOptions struct {
	debugLogger *slog.Logger
}

// EngineOption configures runtime-only engine behavior at construction time.
type EngineOption interface {
	apply(*engineOptions)
}

type engineOptionFunc func(*engineOptions)

func (fn engineOptionFunc) apply(opts *engineOptions) {
	fn(opts)
}

// WithDebug enables manifold debug logs using a default debug-level stderr logger.
func WithDebug() EngineOption {
	return engineOptionFunc(func(opts *engineOptions) {
		opts.debugLogger = defaultEngineDebugLogger()
	})
}

// WithDebugLogger enables manifold debug logs using the provided logger.
func WithDebugLogger(logger *slog.Logger) EngineOption {
	return engineOptionFunc(func(opts *engineOptions) {
		if logger == nil {
			opts.debugLogger = defaultEngineDebugLogger()
			return
		}
		opts.debugLogger = logger
	})
}

func collectEngineOptions(opts []EngineOption) engineOptions {
	var cfg engineOptions
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		opt.apply(&cfg)
	}
	return cfg
}

func defaultEngineDebugLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}
