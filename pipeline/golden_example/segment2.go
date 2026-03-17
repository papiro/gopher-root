package golden_example

import (
	"context"

	"github.com/pierre/manifold/pipeline"
)

type Segment2Input struct {
	Text string `json:"text"`
}

type Segment2 struct{}

func (Segment2) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:          "segment2",
		Idempotency: pipeline.Idempotent,
		Version:     "v1",
	}
}

func (Segment2) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentRecord[Segment2Input],
	out func(pipeline.SegmentRecord[string]) error,
) (pipeline.ProcessResult, error) {
	if err := out(pipeline.SegmentRecord[string]{
		RecordID: in.RecordID,
		Payload:  in.Payload.Text,
		Metadata: in.Metadata,
	}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (Segment2) Restore(context.Context, []byte) error { return nil }
func (Segment2) Done(context.Context) error            { return nil }
