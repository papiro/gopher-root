package types

// SourceRecord is the source-provided stable identity plus payload.
// The engine propagates this identity through segment records and wraps traced outputs in an Envelope.
type SourceRecord[T any] struct {
	// RecordID is the stable source identity used for correlation and dedup.
	RecordID RecordID
	// Payload contains business data emitted by the source.
	Payload T
	// Metadata stores optional source attributes (for example offsets or upstream keys).
	Metadata map[string]string
}
