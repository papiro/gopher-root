package interfaces

import (
	"errors"

	pipelinetypes "github.com/pierre/gopher-root/pipeline/types"
)

// ErrCompensatorRequired indicates a non-idempotent segment lacks required compensation support.
var ErrCompensatorRequired = errors.New("non-idempotent segment must provide compensator")

// ErrSegmentIDRequired indicates a segment descriptor omitted required segment identity.
var ErrSegmentIDRequired = errors.New("segment ID is required")

// ValidateSegment performs baseline contract checks independent of engine implementation.
func ValidateSegment[TIn, TOut any](s Segment[TIn, TOut]) error {
	// Read immutable segment descriptor once for consistent validation decisions.
	desc := s.Descriptor()
	// Require explicit segment identity for ack graph indexing and traceability.
	if desc.ID == "" {
		return ErrSegmentIDRequired
	}
	// Require compensator when replaying the segment can duplicate side effects.
	if desc.Idempotency == pipelinetypes.NonIdempotent && s.Compensator() == nil {
		return ErrCompensatorRequired
	}
	// Return success when baseline contract guarantees are satisfied.
	return nil
}
