package interfaces

import (
	"context"
	"encoding/json"

	pipelinetypes "github.com/pierre/manifold/pipeline/types"
)

// Runtime persists framework-owned pipeline durability state.
type Runtime interface {
	// LoadCheckpoint retrieves the latest stored recovery boundary for one pipeline.
	LoadCheckpoint(ctx context.Context, pipelineID string) (Checkpoint, bool, error)
	// SaveCheckpoint persists the latest recovery boundary for one pipeline.
	SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error
	// SaveSegmentState persists one framework-managed in-flight segment snapshot.
	SaveSegmentState(ctx context.Context, state SegmentState) error
	// LoadSegmentState retrieves one framework-managed in-flight segment snapshot if present.
	LoadSegmentState(ctx context.Context, pipelineID string, segment pipelinetypes.SegmentID, record pipelinetypes.RecordID, attempt pipelinetypes.AttemptID) (SegmentState, bool, error)
	// DeleteSegmentState removes one framework-managed in-flight segment snapshot once it is no longer needed.
	DeleteSegmentState(ctx context.Context, pipelineID string, segment pipelinetypes.SegmentID, record pipelinetypes.RecordID, attempt pipelinetypes.AttemptID) error
	// CommitSegment persists one segment outcome plus lineage context for one record.
	CommitSegment(ctx context.Context, commit SegmentCommit) error
	// CommitSegmentOutput persists one intermediate segment output so prior stages can be recalled during replay workflows.
	CommitSegmentOutput(ctx context.Context, output SegmentOutputRecord) error
	// CommitTerminal persists one terminal output so traces survive resume and restart.
	CommitTerminal(ctx context.Context, terminal TerminalRecord) error
	// SegmentOutputs returns intermediate outputs emitted by one segment for one origin record.
	SegmentOutputs(ctx context.Context, pipelineID string, segment pipelinetypes.SegmentID, origin pipelinetypes.RecordID) ([]pipelinetypes.Envelope[json.RawMessage], error)
	// Trace returns terminal outputs derived from one source record identity.
	Trace(ctx context.Context, pipelineID string, origin pipelinetypes.RecordID) ([]pipelinetypes.Envelope[json.RawMessage], error)
	// Ack returns the latest durable segment acknowledgment for one record if present.
	Ack(ctx context.Context, pipelineID string, segment pipelinetypes.SegmentID, record pipelinetypes.RecordID) (pipelinetypes.SegmentAck, bool, error)
}

// Checkpoint is the latest recoverable boundary for one pipeline.
type Checkpoint struct {
	PipelineID   string
	SourceCursor []byte
	Frontier     []CheckpointFrame
	Paused       bool
}

// CheckpointFrame captures one in-flight frontier item that should resume at one specific segment.
//
// Payload is the original input that should be passed to NextSegmentID when the
// pipeline resumes from this checkpoint.
type CheckpointFrame struct {
	OriginRecordID pipelinetypes.RecordID
	RecordID       pipelinetypes.RecordID
	AttemptID      pipelinetypes.AttemptID
	ParentIDs      []pipelinetypes.RecordID
	SegmentPath    []pipelinetypes.SegmentID
	NextSegmentID  pipelinetypes.SegmentID
	Payload        json.RawMessage
	Metadata       map[string]string
}

// SegmentState captures framework-managed in-flight state for one segment at one resume boundary.
//
// This state is primarily used for pause/resume, but runtimes may also retain it
// for replay, auditing, or incremental recomputation workflows.
type SegmentState struct {
	PipelineID string
	SegmentID  pipelinetypes.SegmentID
	RecordID   pipelinetypes.RecordID
	AttemptID  pipelinetypes.AttemptID
	Snapshot   []byte
}

// SegmentCommit captures one framework-owned segment outcome.
type SegmentCommit struct {
	PipelineID     string
	OriginRecordID pipelinetypes.RecordID
	RecordID       pipelinetypes.RecordID
	ParentIDs      []pipelinetypes.RecordID
	SegmentID      pipelinetypes.SegmentID
	AttemptID      pipelinetypes.AttemptID
	Status         pipelinetypes.AckStatus
	Err            error
}

// SegmentOutputRecord captures one intermediate output emitted by one segment.
//
// Retaining intermediate outputs allows replay workflows to reuse prior segment
// results up to a changed stage rather than recomputing the entire pipeline.
type SegmentOutputRecord struct {
	PipelineID string
	SegmentID  pipelinetypes.SegmentID
	Item       pipelinetypes.Envelope[json.RawMessage]
}

// TerminalRecord captures one framework-owned terminal output.
type TerminalRecord struct {
	PipelineID string
	Item       pipelinetypes.Envelope[json.RawMessage]
}
