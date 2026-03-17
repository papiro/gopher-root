package golden_example

import (
	"context"
	"encoding/json"

	"github.com/pierre/manifold/pipeline"
)

type Segment1 struct{}

func (Segment1) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:          "segment1",
		Idempotency: pipeline.Idempotent,
		Version:     "v1",
	}
}

func (Segment1) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentRecord[string],
	out func(pipeline.SegmentRecord[json.RawMessage]) error,
) (pipeline.ProcessResult, error) {
	payload, err := json.Marshal(struct {
		Message string `json:"message"`
	}{
		Message: in.Payload,
	})
	if err != nil {
		return pipeline.ProcessResult{}, err
	}

	if err := out(pipeline.SegmentRecord[json.RawMessage]{
		RecordID: in.RecordID,
		Payload:  payload,
		Metadata: in.Metadata,
	}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (Segment1) Restore(context.Context, []byte) error { return nil }
func (Segment1) Done(context.Context) error            { return nil }
