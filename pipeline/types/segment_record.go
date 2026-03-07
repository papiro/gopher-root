package types

// SegmentRecord carries segment business data plus stable source correlation.
type SegmentRecord[T any] struct {
	// RecordID is the stable source identity propagated unchanged across segments.
	RecordID RecordID
	// Payload contains the segment-specific business data for this record.
	Payload T
	// Metadata stores optional key-value attributes for diagnostics and policy decisions.
	Metadata map[string]string
}
