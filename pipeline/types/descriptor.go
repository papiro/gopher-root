package types

// SegmentDescriptor declares static segment behavior used by orchestration and policy layers.
type SegmentDescriptor struct {
	// ID must be unique across the pipeline topology.
	ID SegmentID
	// Idempotency declares replay safety for this segment.
	Idempotency IdempotencyKind
	// Deterministic declares whether identical logical input always yields identical logical output.
	// Nil means default deterministic behavior (true).
	Deterministic *bool
	// CompatibilityVersion changes only when persisted inputs, snapshots, or outputs
	// from older runs are no longer safe to reuse.
	CompatibilityVersion string
}

// IsDeterministic returns true when descriptor behavior is deterministic.
// A nil value defaults to true.
func (d SegmentDescriptor) IsDeterministic() bool {
	if d.Deterministic == nil {
		return true
	}
	return *d.Deterministic
}

// DeterministicValue returns a pointer used to explicitly set deterministic behavior.
func DeterministicValue(v bool) *bool {
	return &v
}
