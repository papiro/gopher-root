package types

// SourceRecord is the source-provided logical identity plus payload.
// The engine assigns attempt and lineage metadata when wrapping this into an Envelope.
type SourceRecord[T any] struct {
	// RecordID is the stable logical identity used for dedup and exactly-once behavior.
	RecordID RecordID
	// Payload contains business data emitted by the source.
	Payload T
	// Metadata stores optional source attributes (for example offsets or upstream keys).
	Metadata map[string]string
}
