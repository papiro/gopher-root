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
		ID:                   "segment2",
		Idempotency:          pipeline.Idempotent,
		CompatibilityVersion: "v1",
	}
}

func (Segment2) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentInput[Segment2Input],
	out func(pipeline.SegmentOutput[string]) error,
) (pipeline.ProcessResult, error) {
	if err := out(pipeline.SegmentOutput[string]{
		Payload:  in.Payload.Text,
		Metadata: in.Metadata,
	}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (Segment2) Done(context.Context) error { return nil }
