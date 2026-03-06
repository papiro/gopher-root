package types

// Envelope carries one record plus trace metadata as it flows through the pipeline.
type Envelope[T any] struct {
	// RecordID is the stable logical identity used for traceability and dedup.
	RecordID RecordID
	// AttemptID is incremented when a record is retried or replayed.
	AttemptID AttemptID
	// ParentIDs links this record to upstream records for fan-out/fan-in lineage graphs.
	ParentIDs []RecordID
	// SegmentPath records the ordered segment traversal history for this record.
	SegmentPath []SegmentID
	// Payload contains the segment-specific business data for this record.
	Payload T
	// Metadata stores optional key-value attributes for diagnostics and policy decisions.
	Metadata map[string]string
}
