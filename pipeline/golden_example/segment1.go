package golden_example

import (
	"context"
	"encoding/json"

	"github.com/pierre/manifold/pipeline"
)

type Segment1 struct{}

func (Segment1) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:                   "segment1",
		Idempotency:          pipeline.Idempotent,
		CompatibilityVersion: "v1",
	}
}

func (Segment1) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentInput[string],
	out func(pipeline.SegmentOutput[json.RawMessage]) error,
) (pipeline.ProcessResult, error) {
	payload, err := json.Marshal(struct {
		Message string `json:"message"`
	}{
		Message: in.Payload,
	})
	if err != nil {
		return pipeline.ProcessResult{}, err
	}

	if err := out(pipeline.SegmentOutput[json.RawMessage]{
		Payload:  payload,
		Metadata: in.Metadata,
	}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (Segment1) Done(context.Context) error { return nil }
