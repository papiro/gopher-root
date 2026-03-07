package golden_example

import (
	"context"

	"github.com/pierre/gopher-root/pipeline"
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
	_ context.Context,
	in pipeline.SegmentRecord[Segment2Input],
	out func(pipeline.SegmentRecord[string]) error,
) error {
	return out(pipeline.SegmentRecord[string]{
		RecordID: in.RecordID,
		Payload:  in.Payload.Text,
		Metadata: in.Metadata,
	})
}

func (Segment2) Flush(context.Context) error               { return nil }
func (Segment2) Done(context.Context) error                { return nil }
func (Segment2) Snapshot(context.Context) ([]byte, error) { return nil, nil }
func (Segment2) Restore(context.Context, []byte) error    { return nil }
func (Segment2) Compensator() pipeline.Compensator        { return nil }
