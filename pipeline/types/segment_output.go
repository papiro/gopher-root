package types

// SegmentOutput carries one segment output business record.
// The engine preserves source identity and assigns lineage metadata outside of this shape.
type SegmentOutput[T any] struct {
	// Payload contains the business value for this segment output.
	Payload T
	// Metadata stores optional key-value attributes for diagnostics and policy decisions.
	Metadata map[string]string
}
