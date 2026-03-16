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

// ProcessContext adds a cooperative pause signal to the standard context contract.
type ProcessContext interface {
	context.Context
	// PauseRequested reports whether the engine wants this segment to pause at the
	// next safe point inside Process.
	PauseRequested() bool
}

// ProcessStatus describes whether one Process call completed its logical work or
// paused cooperatively with resumable state.
type ProcessStatus int

const (
	// ProcessCompleted means the segment fully handled the logical input for this call.
	ProcessCompleted ProcessStatus = iota
	// ProcessPaused means the segment stopped at a safe resumable boundary and returned
	// snapshot bytes that allow Process to continue for the same logical input later.
	ProcessPaused
)

// ProcessResult is the cooperative outcome from one Process call.
type ProcessResult struct {
	Status ProcessStatus
	// Snapshot contains segment-owned resumable state. It should describe only the
	// remaining work not already committed through the out callback.
	Snapshot []byte
}

// Segment transforms one input record into zero or more output records.
type Segment[TIn, TOut any] interface {
	// Descriptor returns segment identity and replay policy metadata.
	Descriptor() pipelinetypes.SegmentDescriptor
	// Process handles one input and emits zero or more outputs through "out" callback.
	//
	// The out callback is framework-owned. Once out returns nil, that output is
	// durably accepted by the framework and should not be repeated in Snapshot.
	//
	// When PauseRequested becomes true, Process may stop at a segment-defined safe
	// point and return ProcessPaused plus resumable snapshot bytes.
	Process(ctx ProcessContext, in pipelinetypes.SegmentRecord[TIn], out func(pipelinetypes.SegmentRecord[TOut]) error) (ProcessResult, error)
	// Restore loads previously captured resumable state into the segment instance
	// before Process is invoked again for the same logical input.
	Restore(ctx context.Context, snapshot []byte) error
	// Done allows optional segment-specific finalize work once upstream has finished.
	Done(ctx context.Context) error
}

// CompensatingSegment adds rollback support for segments with non-idempotent side effects.
type CompensatingSegment[TIn, TOut any] interface {
	Segment[TIn, TOut]
	// Compensator returns segment compensation logic and must be non-nil for non-idempotent segments.
	Compensator() Compensator
}
