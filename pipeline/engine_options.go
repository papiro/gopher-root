package pipeline

import (
	"context"
	"log/slog"
	"os"
)

type engineOptions struct {
	debugLogger *slog.Logger
	onRestart   RestartHook
}

// RestartHook runs after manifold-owned runtime state is reset and before
// Restart begins replaying from source zero.
type RestartHook func(ctx context.Context, pipelineID string) error

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

// WithOnRestart registers a callback that runs after runtime reset succeeds and
// before Restart begins replaying from source zero.
//
// This hook is intended for pipeline-author cleanup of app-owned state. It is
// outside the runtime reset transaction and should therefore be idempotent.
func WithOnRestart(hook RestartHook) EngineOption {
	return engineOptionFunc(func(opts *engineOptions) {
		opts.onRestart = hook
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
