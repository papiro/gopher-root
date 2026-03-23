package types

// SourceRecord is one source-emitted business record.
// The engine propagates this stable source identity through segment records and wraps
// lineage-tracked values in an Envelope.
type SourceRecord[T any] struct {
	// RecordID is the stable source identity used for correlation and dedup.
	RecordID RecordID
	// Payload contains the business value emitted by the source.
	Payload T
	// Metadata stores optional source attributes (for example offsets or upstream keys).
	Metadata map[string]string
}
