package pipeline_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"

	"github.com/pierre/manifold/pipeline"
	"github.com/pierre/manifold/pipeline/golden_example"
	"github.com/pierre/manifold/pipeline/golden_example/couplings"
)

func TestEngineDebugLoggerEmitsLifecycleLogs(t *testing.T) {
	t.Parallel()

	plan, err := pipeline.NewBuilder().
		Through(golden_example.Segment1{}).
		Via(couplings.MessageToText{}).
		Through(golden_example.Segment2{}).
		Build()
	if err != nil {
		t.Fatalf("plan build failed: %v", err)
	}

	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))

	engine, err := pipeline.NewPullEngine(
		&golden_example.Source{},
		&golden_example.Sink{},
		plan,
		pipeline.NewInMemoryRuntime(),
		pipeline.WithDebugLogger(logger),
	)
	if err != nil {
		t.Fatalf("engine build failed: %v", err)
	}

	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	output := logs.String()
	if !strings.Contains(output, "manifold pull engine starting") {
		t.Fatalf("expected pull engine start log, got %q", output)
	}
	if !strings.Contains(output, "manifold processing segment") {
		t.Fatalf("expected segment processing log, got %q", output)
	}
	if !strings.Contains(output, "segment_id=segment1") {
		t.Fatalf("expected first segment ID in logs, got %q", output)
	}
	if !strings.Contains(output, "manifold committed terminal output") {
		t.Fatalf("expected terminal commit log, got %q", output)
	}
}
