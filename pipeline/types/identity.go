package types

// SegmentID uniquely identifies a segment within one pipeline topology.
type SegmentID string

// RecordID uniquely identifies one logical record across retries and lineage operations.
type RecordID string

// AttemptID identifies a processing attempt number for one logical record.
type AttemptID uint64
