package types

// SegmentInput carries one segment input business record plus the stable source
// identity that this input belongs to.
type SegmentInput[T any] struct {
	// SourceRecordID is the stable source identity propagated unchanged across segments.
	SourceRecordID RecordID
	// Payload contains the business value for this segment input.
	Payload T
	// Metadata stores optional key-value attributes for diagnostics and policy decisions.
	Metadata map[string]string
}
