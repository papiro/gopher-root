package golden_example

import (
	"context"

	"github.com/pierre/gopher-root/pipeline"
)

type Source struct {
	emitted bool
}

func (s *Source) Next(context.Context) (pipeline.SourceRecord[string], bool, error) {
	if s.emitted {
		return pipeline.SourceRecord[string]{}, false, nil
	}

	s.emitted = true
	return pipeline.SourceRecord[string]{
		RecordID: "rec-1",
		Payload:  "hello world",
	}, true, nil
}
