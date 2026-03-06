package types

// AckStatus describes the durable processing outcome of one segment for one record.
type AckStatus int

const (
	// AckCommitted means the segment completed successfully and committed durability boundary.
	AckCommitted AckStatus = iota
	// AckRetryableFail means processing failed but retry is allowed by policy.
	AckRetryableFail
	// AckTerminalFail means processing failed permanently and should not be retried.
	AckTerminalFail
)

// SegmentAck is the durable acknowledgment record for a segment/record attempt pair.
type SegmentAck struct {
	// Segment is the segment that owns this acknowledgment.
	Segment SegmentID
	// RecordID is the logical record identity being acknowledged.
	RecordID RecordID
	// Attempt is the attempt number that produced this acknowledgment.
	Attempt AttemptID
	// Status is the final outcome for this attempt at this segment.
	Status AckStatus
	// Err is an optional attached error for failed outcomes.
	Err error
}
