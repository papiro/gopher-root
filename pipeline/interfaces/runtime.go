package interfaces

import (
	"context"
	"encoding/json"

	pipelinetypes "github.com/pierre/manifold/pipeline/types"
)

// Runtime persists framework-owned pipeline durability state.
type Runtime interface {
	// LoadSourceResumeState retrieves the latest stored source resume state for one pipeline.
	LoadSourceResumeState(ctx context.Context, pipelineID string) (SourceResumeState, bool, error)
	// SaveSourceResumeState persists the latest source resume state for one pipeline.
	SaveSourceResumeState(ctx context.Context, recovery SourceResumeState) error
	// CommitSourceProgress atomically persists the latest source cursor together with
	// one newly started source record and its initial pending segment work item.
	CommitSourceProgress(ctx context.Context, progress SourceProgress) error
	// CommitProgressUpdate atomically persists one runtime delta.
	CommitProgressUpdate(ctx context.Context, update RuntimeDelta) error
	// DeterministicResult loads one reusable deterministic segment result if present.
	DeterministicResult(ctx context.Context, pipelineID string, segment pipelinetypes.SegmentID, compatibilityVersion string, inputChecksum string) (DeterministicSegmentResult, bool, error)
	// ResetPipeline removes framework-owned state for one pipeline so execution can
	// restart from source zero.
	ResetPipeline(ctx context.Context, pipelineID string) error
	// SourceRecords returns all source records that have durably started for one pipeline.
	SourceRecords(ctx context.Context, pipelineID string) ([]pipelinetypes.Envelope[json.RawMessage], error)
	// PendingWork returns all pending segment work items for one pipeline in queue order.
	PendingWork(ctx context.Context, pipelineID string) ([]PendingSegmentWork, error)
	// LoadSegmentProgress retrieves one segment progress row if present.
	LoadSegmentProgress(ctx context.Context, pipelineID string, segment pipelinetypes.SegmentID, record pipelinetypes.RecordID, attempt pipelinetypes.AttemptID) (SegmentProgress, bool, error)
	// Trace returns terminal outputs derived from one source record identity.
	Trace(ctx context.Context, pipelineID string, origin pipelinetypes.RecordID) ([]pipelinetypes.Envelope[json.RawMessage], error)
}

// SourceResumeState captures the source-side state needed to resume one pipeline.
//
// It does not describe segment frontier or in-flight work. Those live in
// PendingSegmentWork and SegmentProgress.
type SourceResumeState struct {
	PipelineID   string
	SourceCursor []byte
	Paused       bool
}

// SourceProgress captures one durable source advance.
type SourceProgress struct {
	ResumeState SourceResumeState
	Started     StartedRecord
	PendingWork PendingSegmentWork
}

// StartedRecord captures one source record that has durably entered the pipeline.
type StartedRecord struct {
	PipelineID string
	Item       pipelinetypes.Envelope[json.RawMessage]
}

// PendingSegmentWork captures one queued segment input awaiting processing.
type PendingSegmentWork struct {
	PipelineID    string
	NextSegmentID pipelinetypes.SegmentID
	Item          pipelinetypes.Envelope[json.RawMessage]
}

// PendingSegmentWorkKey identifies one queued segment input.
type PendingSegmentWorkKey struct {
	PipelineID    string
	NextSegmentID pipelinetypes.SegmentID
	RecordID      pipelinetypes.RecordID
}

// SegmentProgressStatus describes the durable processing state for one queued segment input.
type SegmentProgressStatus int

const (
	SegmentInProgress SegmentProgressStatus = iota
	SegmentPaused
	SegmentCompleted
	SegmentRetryableFailed
	SegmentTerminalFailed
)

// SegmentProgress captures the durable state of one segment input while it is being processed.
type SegmentProgress struct {
	PipelineID           string
	SegmentID            pipelinetypes.SegmentID
	CompatibilityVersion string
	RecordID             pipelinetypes.RecordID
	AttemptID            pipelinetypes.AttemptID
	InputChecksum        string
	Status               SegmentProgressStatus
	EmittedCount         int
}

// RuntimeDelta captures one atomic durability delta for pipeline runtime state.
//
// It is not a full checkpoint or snapshot of the entire pipeline. Instead, each
// value records one state transition, such as updating one segment progress row,
// deleting one pending work item, enqueueing downstream work, or persisting one
// emitted durable record.
type RuntimeDelta struct {
	Progress            *SegmentProgress
	DeletePendingWork   *PendingSegmentWorkKey
	EnqueuePendingWork  []PendingSegmentWork
	OutputRecord        *SegmentOutputRecord
	TerminalRecord      *TerminalRecord
	DeterministicResult *DeterministicSegmentResult
}

// SegmentOutputRecord captures one intermediate output emitted by one segment.
//
// Retaining intermediate outputs allows replay workflows to reuse prior segment
// results up to a changed stage rather than recomputing the entire pipeline.
type SegmentOutputRecord struct {
	PipelineID           string
	SegmentID            pipelinetypes.SegmentID
	CompatibilityVersion string
	InputChecksum        string
	Item                 pipelinetypes.Envelope[json.RawMessage]
}

// TerminalRecord captures one framework-owned terminal output.
type TerminalRecord struct {
	PipelineID string
	Item       pipelinetypes.Envelope[json.RawMessage]
}

// DeterministicSegmentResult captures reusable outputs for one deterministic segment input.
type DeterministicSegmentResult struct {
	PipelineID           string
	SegmentID            pipelinetypes.SegmentID
	CompatibilityVersion string
	InputChecksum        string
	Outputs              []DeterministicSegmentOutput
}

// DeterministicSegmentOutput captures one reusable deterministic output payload.
type DeterministicSegmentOutput struct {
	Payload  json.RawMessage
	Metadata map[string]string
}
