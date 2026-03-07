package golden_example

import (
	"context"

	"github.com/pierre/gopher-root/pipeline"
)

type Sink struct {
	items []pipeline.Envelope[string]
}

func (s *Sink) Consume(_ context.Context, item pipeline.Envelope[string]) error {
	s.items = append(s.items, item)
	return nil
}

func (s *Sink) Done(context.Context) error { return nil }
