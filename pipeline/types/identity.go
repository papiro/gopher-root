package types

// SegmentID uniquely identifies a segment within one pipeline topology.
type SegmentID string

// RecordID identifies either a stable source identity or a lineage node, depending on
// the contract that carries it.
// SourceRecord.RecordID and SegmentInput.SourceRecordID use it as the stable source
// identity, while Envelope.RecordID uses it for lineage nodes.
type RecordID string

// AttemptID identifies a processing attempt number for one logical record.
type AttemptID uint64
