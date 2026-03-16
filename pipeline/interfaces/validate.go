package interfaces

import (
	"errors"
	"reflect"

	pipelinetypes "github.com/pierre/gopher-root/pipeline/types"
)

// ErrCompensatorRequired indicates a non-idempotent segment lacks required compensation support.
var ErrCompensatorRequired = errors.New("non-idempotent segment must provide compensator")

// ErrSourceRequired indicates a source implementation was not provided.
var ErrSourceRequired = errors.New("source is required")

// ErrStreamSourceRequired indicates a stream source implementation was not provided.
var ErrStreamSourceRequired = errors.New("stream source is required")

// ErrSinkRequired indicates a sink implementation was not provided.
var ErrSinkRequired = errors.New("sink is required")

// ErrRuntimeRequired indicates a runtime implementation was not provided.
var ErrRuntimeRequired = errors.New("runtime is required")

// ErrSegmentIDRequired indicates a segment descriptor omitted required segment identity.
var ErrSegmentIDRequired = errors.New("segment ID is required")

// ValidateSource performs baseline contract checks independent of engine implementation.
func ValidateSource[T any](s Source[T]) error {
	if isNilContract(s) {
		return ErrSourceRequired
	}
	return nil
}

// ValidateStreamSource performs baseline contract checks independent of engine implementation.
func ValidateStreamSource[T any](s StreamSource[T]) error {
	if isNilContract(s) {
		return ErrStreamSourceRequired
	}
	return nil
}

// ValidateSink performs baseline contract checks independent of engine implementation.
func ValidateSink[T any](s Sink[T]) error {
	if isNilContract(s) {
		return ErrSinkRequired
	}
	return nil
}

// ValidateRuntime performs baseline contract checks independent of engine implementation.
func ValidateRuntime(r Runtime) error {
	if isNilContract(r) {
		return ErrRuntimeRequired
	}
	return nil
}

// ValidateSegment performs baseline contract checks independent of engine implementation.
func ValidateSegment[TIn, TOut any](s Segment[TIn, TOut]) error {
	// Read immutable segment descriptor once for consistent validation decisions.
	desc := s.Descriptor()
	// Require explicit segment identity for ack graph indexing and traceability.
	if desc.ID == "" {
		return ErrSegmentIDRequired
	}
	// Require compensator when replaying the segment can duplicate side effects.
	if desc.Idempotency == pipelinetypes.NonIdempotent {
		compensating, ok := any(s).(CompensatingSegment[TIn, TOut])
		if !ok || compensating.Compensator() == nil {
			return ErrCompensatorRequired
		}
	}
	// Return success when baseline contract guarantees are satisfied.
	return nil
}

func isNilContract(v any) bool {
	if v == nil {
		return true
	}

	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
