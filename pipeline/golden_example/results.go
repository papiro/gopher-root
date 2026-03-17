package golden_example

import "github.com/pierre/manifold/pipeline"

type ValidationResult struct {
	Source   error
	Segment1 error
	Segment2 error
	Sink     error
}

type RunResult struct {
	SourceEmitted   bool
	CouplingApplied bool
	SinkOutput      string
	OriginRecordID  pipeline.RecordID
	SegmentPath     []pipeline.SegmentID
	SourceCompleted bool
}
