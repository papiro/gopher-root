package pipeline_test

import (
	"context"
	"testing"

	"github.com/pierre/gopher-root/pipeline"
)

// These tests define expected runtime semantics for the future engine implementation.
// They are intentionally skipped for now so we can review coverage before coding runtime logic.

func TestEngineSpec_PausePersistsAndResumeContinues(t *testing.T) {
	t.Skip("spec-only: implement once Engine runtime exists")

	var eng pipeline.Engine[string, string]
	_ = eng.Pause(context.Background())
	_ = eng.Resume(context.Background())

	// Expected behavior:
	// 1) Pause requests the next resumable boundary.
	// 2) The runtime persists source position, in-flight frontier state, and optional segment snapshots.
	// 3) Acks up to the pause boundary are durable.
	// 4) Resume restores frontier state and continues without reprocessing already committed work.
}

func TestEngineSpec_RetryUsesCompensationForNonIdempotentSegment(t *testing.T) {
	t.Skip("spec-only: implement once Engine runtime exists")

	var eng pipeline.Engine[string, string]
	_ = eng.Retry(context.Background(), "src-42")

	// Expected behavior:
	// 1) Retry can target a record lineage branch.
	// 2) Non-idempotent segment runs Compensate before replay when partial side effects exist.
	// 3) Final commit reflects exactly-once best-effort with dedup by segment+record identity.
}

func TestEngineSpec_TraceSourceToZeroOrManyOutputs(t *testing.T) {
	t.Skip("spec-only: implement once Engine runtime exists")

	var eng pipeline.Engine[string, string]
	_, _ = eng.Trace(context.Background(), "src-123")

	// Expected behavior:
	// 1) Trace returns all terminal outputs derived from source record.
	// 2) Trace includes intermediate lineage metadata for explainability.
	// 3) Trace handles zero-output paths (e.g., filters) without being treated as errors.
}
