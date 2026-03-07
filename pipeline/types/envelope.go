package types

// Envelope carries one traced record plus engine-owned execution metadata.
type Envelope[T any] struct {
	// OriginRecordID is the stable source identity propagated end-to-end for correlation.
	OriginRecordID RecordID
	// RecordID uniquely identifies this specific lineage node for acks and parent/child links.
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
