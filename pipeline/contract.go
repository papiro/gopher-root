package pipeline

import (
	"context"
	"errors"
)

// StageID uniquely identifies a stage within one pipeline topology.
type StageID string

// RecordID uniquely identifies one logical record across retries and lineage operations.
type RecordID string

// AttemptID identifies a processing attempt number for one logical record.
type AttemptID uint64

// IdempotencyKind classifies whether a stage can safely re-run the same input.
type IdempotencyKind int

const (
	// Idempotent means replaying the same record does not change externally visible results.
	Idempotent IdempotencyKind = iota
	// NonIdempotent means replay can duplicate or alter side effects unless compensated.
	NonIdempotent
)

// Envelope carries one record plus trace metadata as it flows through the pipeline.
type Envelope[T any] struct {
	// RecordID is the stable logical identity used for traceability and dedup.
	RecordID  RecordID
	// AttemptID is incremented when a record is retried or replayed.
	AttemptID AttemptID
	// ParentIDs links this record to upstream records for fan-out/fan-in lineage graphs.
	ParentIDs []RecordID
	// StagePath records the ordered stage traversal history for this record.
	StagePath []StageID
	// Payload contains the stage-specific business data for this record.
	Payload   T
	// Metadata stores optional key-value attributes for diagnostics and policy decisions.
	Metadata  map[string]string
}

// AckStatus describes the durable processing outcome of one stage for one record.
type AckStatus int

const (
	// AckCommitted means the stage completed successfully and committed durability boundary.
	AckCommitted AckStatus = iota
	// AckRetryableFail means processing failed but retry is allowed by policy.
	AckRetryableFail
	// AckTerminalFail means processing failed permanently and should not be retried.
	AckTerminalFail
)

// StageAck is the durable acknowledgment record for a stage/record attempt pair.
type StageAck struct {
	// Stage is the stage that owns this acknowledgment.
	Stage    StageID
	// RecordID is the logical record identity being acknowledged.
	RecordID RecordID
	// Attempt is the attempt number that produced this acknowledgment.
	Attempt  AttemptID
	// Status is the final outcome for this attempt at this stage.
	Status   AckStatus
	// Err is an optional attached error for failed outcomes.
	Err      error
}

// StageDescriptor declares static stage behavior used by orchestration and policy layers.
type StageDescriptor struct {
	// ID must be unique across the pipeline topology.
	ID          StageID
	// Idempotency declares replay safety for this stage.
	Idempotency IdempotencyKind
	// Version is stage-owned code/schema version used for state migrations.
	Version     string
}

// Producer yields envelopes in pull mode where downstream controls pace.
type Producer[T any] interface {
	// Next returns (item, true, nil) for data, (zero, false, nil) for end-of-stream, or error.
	Next(ctx context.Context) (Envelope[T], bool, error)
}

// StreamProducer yields envelopes in push mode where upstream controls emission cadence.
type StreamProducer[T any] interface {
	// Stream returns a channel that the producer closes on completion or fatal error.
	Stream(ctx context.Context) <-chan Envelope[T]
}

// Consumer is a sink where Consume behavior defines back-pressure semantics.
type Consumer[T any] interface {
	// Consume handles one envelope and should block when the sink wants to apply back-pressure.
	Consume(ctx context.Context, item Envelope[T]) error
}

// ConsumerWithDone extends Consumer with an explicit completion hook.
type ConsumerWithDone[T any] interface {
	// Consumer is embedded so all Consume semantics still apply.
	Consumer[T]
	// Done allows sink-specific flush/finalize work once upstream has finished.
	Done(ctx context.Context) error
}

// StageStateStore persists stage-owned snapshots used by pause/resume and restart recovery.
type StageStateStore interface {
	// Load retrieves the latest persisted snapshot for one stage.
	Load(ctx context.Context, stage StageID) ([]byte, error)
	// Save persists the latest snapshot bytes for one stage.
	Save(ctx context.Context, stage StageID, snapshot []byte) error
	// Migrate transforms previously persisted snapshot bytes across stage versions.
	Migrate(ctx context.Context, stage StageID, fromVersion, toVersion string, old []byte) ([]byte, error)
}

// AckGraphStore is the durable source of truth for per-record progress and lineage.
type AckGraphStore interface {
	// CommitAck stores one stage acknowledgment as an atomic durable update.
	CommitAck(ctx context.Context, ack StageAck) error
	// LinkParentChild records lineage edge(s) so source-to-output tracing is possible.
	LinkParentChild(ctx context.Context, parent RecordID, child RecordID) error
	// GetAck returns one stage acknowledgment if present.
	GetAck(ctx context.Context, stage StageID, record RecordID) (StageAck, bool, error)
	// Children returns direct descendants for fan-out tracing.
	Children(ctx context.Context, record RecordID) ([]RecordID, error)
	// Parents returns direct ancestors for explainability and replay strategies.
	Parents(ctx context.Context, record RecordID) ([]RecordID, error)
	// PendingByStage returns records observed but not yet committed for one stage.
	PendingByStage(ctx context.Context, stage StageID) ([]RecordID, error)
}

// Acker is a minimal acknowledgment writer used when full graph access is unnecessary.
type Acker interface {
	// Commit persists one stage acknowledgment.
	Commit(ctx context.Context, ack StageAck) error
}

// Compensator reverses or mitigates partial side effects for non-idempotent stages.
type Compensator interface {
	// Compensate executes stage-defined rollback semantics for one failed attempt.
	Compensate(ctx context.Context, rec RecordID, attempt AttemptID, reason error) error
}

// Stage transforms one input envelope into zero or more output envelopes.
type Stage[TIn, TOut any] interface {
	// Descriptor returns stage identity and replay policy metadata.
	Descriptor() StageDescriptor
	// Process handles one input and emits zero or more outputs through emit callback.
	Process(ctx context.Context, in Envelope[TIn], emit func(Envelope[TOut]) error) error
	// Flush asks the stage to durably finish partial artifacts at a safe boundary.
	Flush(ctx context.Context) error
	// Snapshot captures stage-owned resumable state.
	Snapshot(ctx context.Context) ([]byte, error)
	// Restore loads stage-owned resumable state.
	Restore(ctx context.Context, snapshot []byte) error
	// Compensator returns stage compensation logic and must be non-nil for non-idempotent stages.
	Compensator() Compensator
}

// Engine orchestrates source, stages, sinks, and durable progress APIs.
type Engine[TIn, TOut any] interface {
	// Run starts or continues processing until completion, cancellation, or fatal error.
	Run(ctx context.Context) error
	// Pause requests safe boundaries, flushes, and snapshots before returning.
	Pause(ctx context.Context) error
	// Resume continues processing from the next durable recovery boundary.
	Resume(ctx context.Context) error
	// Retry replays a specific logical record according to retry and compensation rules.
	Retry(ctx context.Context, record RecordID) error
	// Trace returns terminal outputs derived from one source record.
	Trace(ctx context.Context, source RecordID) ([]Envelope[TOut], error)
}

// ErrCompensatorRequired indicates a non-idempotent stage lacks required compensation support.
var ErrCompensatorRequired = errors.New("non-idempotent stage must provide compensator")

// ErrStageIDRequired indicates a stage descriptor omitted required stage identity.
var ErrStageIDRequired = errors.New("stage ID is required")

// ValidateStage performs baseline contract checks independent of engine implementation.
func ValidateStage[TIn, TOut any](s Stage[TIn, TOut]) error {
	// Read immutable stage descriptor once for consistent validation decisions.
	desc := s.Descriptor()
	// Require explicit stage identity for ack graph indexing and traceability.
	if desc.ID == "" {
		return ErrStageIDRequired
	}
	// Require compensator when replaying the stage can duplicate side effects.
	if desc.Idempotency == NonIdempotent && s.Compensator() == nil {
		return ErrCompensatorRequired
	}
	// Return success when baseline contract guarantees are satisfied.
	return nil
}
