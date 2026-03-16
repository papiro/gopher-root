package golden_example

import (
	"context"
	"encoding/json"

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

func (s *Source) SnapshotCursor(context.Context) ([]byte, error) {
	return json.Marshal(struct {
		Emitted bool `json:"emitted"`
	}{
		Emitted: s.emitted,
	})
}

func (s *Source) RestoreCursor(_ context.Context, cursor []byte) error {
	if len(cursor) == 0 {
		s.emitted = false
		return nil
	}

	var state struct {
		Emitted bool `json:"emitted"`
	}
	if err := json.Unmarshal(cursor, &state); err != nil {
		return err
	}
	s.emitted = state.Emitted
	return nil
}
