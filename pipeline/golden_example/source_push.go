package golden_example

import (
	"context"

	"github.com/pierre/manifold/pipeline"
)

type PushSource struct{}

func (PushSource) Stream(ctx context.Context) <-chan pipeline.SourceRecord[string] {
	ch := make(chan pipeline.SourceRecord[string], 1)

	go func() {
		defer close(ch)

		select {
		case <-ctx.Done():
			return
		case ch <- pipeline.SourceRecord[string]{
			RecordID: "rec-1",
			Payload:  "hello world",
		}:
		}
	}()

	return ch
}
