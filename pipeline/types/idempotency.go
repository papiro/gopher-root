package types

// IdempotencyKind classifies whether a segment can safely re-run the same input.
type IdempotencyKind int

const (
	// Idempotent means replaying the same record does not change externally visible results.
	Idempotent IdempotencyKind = iota
	// NonIdempotent means replay can duplicate or alter side effects unless compensated.
	NonIdempotent
)
