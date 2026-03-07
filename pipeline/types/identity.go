package types

// SegmentID uniquely identifies a segment within one pipeline topology.
type SegmentID string

// RecordID identifies a pipeline record.
// Source and segment-facing contracts use it as the stable source identity, while envelopes use it for lineage nodes.
type RecordID string

// AttemptID identifies a processing attempt number for one logical record.
type AttemptID uint64
