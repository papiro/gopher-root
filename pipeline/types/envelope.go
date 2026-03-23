package types

// Envelope wraps one lineage node plus engine-owned execution metadata.
type Envelope[T any] struct {
	// OriginRecordID is the stable source identity propagated end-to-end for correlation.
	OriginRecordID RecordID
	// RecordID uniquely identifies this specific lineage node for acks and parent/child links.
	RecordID RecordID
	// AttemptID starts at 1 for a new source lineage and increments when that
	// lineage is retried or replayed by the engine.
	AttemptID AttemptID
	// ParentIDs links this record to upstream records for fan-out/fan-in lineage graphs.
	ParentIDs []RecordID
	// SegmentPath records the ordered segment traversal history for this record.
	SegmentPath []SegmentID
	// Payload contains the business value for this lineage node.
	Payload T
	// Metadata stores optional key-value attributes for diagnostics and policy decisions.
	Metadata map[string]string
}
