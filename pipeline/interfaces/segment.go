package interfaces

import (
	"context"

	pipelinetypes "github.com/pierre/gopher-root/pipeline/types"
)

// Compensator reverses or mitigates partial side effects for non-idempotent segments.
type Compensator interface {
	// Compensate executes segment-defined rollback semantics for one failed attempt.
	Compensate(ctx context.Context, rec pipelinetypes.RecordID, attempt pipelinetypes.AttemptID, reason error) error
}

// Segment transforms one input record into zero or more output records.
type Segment[TIn, TOut any] interface {
	// Descriptor returns segment identity and replay policy metadata.
	Descriptor() pipelinetypes.SegmentDescriptor
	// Process handles one input and emits zero or more outputs through out callback.
	Process(ctx context.Context, in pipelinetypes.SegmentRecord[TIn], out func(pipelinetypes.SegmentRecord[TOut]) error) error
	// Flush asks the segment to durably finish partial artifacts at a safe boundary.
	Flush(ctx context.Context) error
	// Done allows optional segment-specific finalize work once upstream has finished.
	Done(ctx context.Context) error
	// Snapshot captures segment-owned resumable state.
	Snapshot(ctx context.Context) ([]byte, error)
	// Restore loads segment-owned resumable state.
	Restore(ctx context.Context, snapshot []byte) error
	// Compensator returns segment compensation logic and must be non-nil for non-idempotent segments.
	Compensator() Compensator
}
