package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pierre/manifold/pipeline"
)

type noopCompensator struct{}

func (noopCompensator) Compensate(context.Context, pipeline.RecordID, pipeline.AttemptID, error) error {
	return nil
}

type fakeSource struct {
	items []pipeline.SourceRecord[string]
	idx   int
}

func (p *fakeSource) Next(_ context.Context) (pipeline.SourceRecord[string], bool, error) {
	if p.idx >= len(p.items) {
		return pipeline.SourceRecord[string]{}, false, nil
	}
	item := p.items[p.idx]
	p.idx++
	return item, true, nil
}

func (p *fakeSource) SnapshotCursor(context.Context) ([]byte, error) {
	return json.Marshal(struct {
		Index int `json:"index"`
	}{
		Index: p.idx,
	})
}

func (p *fakeSource) RestoreCursor(_ context.Context, cursor []byte) error {
	if len(cursor) == 0 {
		p.idx = 0
		return nil
	}

	var state struct {
		Index int `json:"index"`
	}
	if err := json.Unmarshal(cursor, &state); err != nil {
		return err
	}
	p.idx = state.Index
	return nil
}

type nilFakeSource struct{}

func (*nilFakeSource) Next(context.Context) (pipeline.SourceRecord[string], bool, error) {
	return pipeline.SourceRecord[string]{}, false, nil
}

func (*nilFakeSource) SnapshotCursor(context.Context) ([]byte, error) { return nil, nil }
func (*nilFakeSource) RestoreCursor(context.Context, []byte) error    { return nil }

type fakeStreamSource struct{}

func (fakeStreamSource) Stream(context.Context) <-chan pipeline.SourceRecord[string] {
	ch := make(chan pipeline.SourceRecord[string])
	close(ch)
	return ch
}

type nilFakeStreamSource struct{}

func (*nilFakeStreamSource) Stream(context.Context) <-chan pipeline.SourceRecord[string] {
	return nil
}

type fakeSink struct {
	received []pipeline.Envelope[string]
	doneCall int
}

type nilFakeSink struct{}

type fakeCoupling struct {
	out json.RawMessage
	err error
}

func (f fakeCoupling) Couple(json.RawMessage) (json.RawMessage, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.out, nil
}

func (c *fakeSink) Consume(_ context.Context, item pipeline.Envelope[string]) error {
	c.received = append(c.received, item)
	return nil
}

func (c *fakeSink) Done(context.Context) error {
	c.doneCall++
	return nil
}

func (*nilFakeSink) Consume(context.Context, pipeline.Envelope[string]) error { return nil }

type fakeSegment struct {
	desc        pipeline.SegmentDescriptor
	compensator pipeline.Compensator
}

func (f fakeSegment) Descriptor() pipeline.SegmentDescriptor { return f.desc }

func (f fakeSegment) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentInput[string],
	out func(pipeline.SegmentOutput[string]) error,
) (pipeline.ProcessResult, error) {
	if err := out(pipeline.SegmentOutput[string]{Payload: in.Payload, Metadata: in.Metadata}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (f fakeSegment) Done(context.Context) error        { return nil }
func (f fakeSegment) Compensator() pipeline.Compensator { return f.compensator }

type fakeSegmentNoCompensator struct {
	desc pipeline.SegmentDescriptor
}

func (f fakeSegmentNoCompensator) Descriptor() pipeline.SegmentDescriptor { return f.desc }

func (f fakeSegmentNoCompensator) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentInput[string],
	out func(pipeline.SegmentOutput[string]) error,
) (pipeline.ProcessResult, error) {
	if err := out(pipeline.SegmentOutput[string]{Payload: in.Payload, Metadata: in.Metadata}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (f fakeSegmentNoCompensator) Done(context.Context) error { return nil }

func TestValidateSegmentContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		segment pipeline.Segment[string, string]
		wantErr error
	}{
		{
			name: "idempotent segment without compensator method is allowed",
			segment: fakeSegmentNoCompensator{
				desc: pipeline.SegmentDescriptor{
					ID:          "segment-A",
					Idempotency: pipeline.Idempotent,
					CompatibilityVersion: "v1",
				},
			},
			wantErr: nil,
		},
		{
			name: "non-idempotent segment must provide compensator",
			segment: fakeSegment{
				desc: pipeline.SegmentDescriptor{
					ID:          "segment-B",
					Idempotency: pipeline.NonIdempotent,
					CompatibilityVersion: "v1",
				},
				compensator: nil,
			},
			wantErr: pipeline.ErrCompensatorRequired,
		},
		{
			name: "non-idempotent segment without compensator method is rejected",
			segment: fakeSegmentNoCompensator{
				desc: pipeline.SegmentDescriptor{
					ID:          "segment-B2",
					Idempotency: pipeline.NonIdempotent,
					CompatibilityVersion: "v1",
				},
			},
			wantErr: pipeline.ErrCompensatorRequired,
		},
		{
			name: "non-idempotent segment with compensator is allowed",
			segment: fakeSegment{
				desc: pipeline.SegmentDescriptor{
					ID:          "segment-C",
					Idempotency: pipeline.NonIdempotent,
					CompatibilityVersion: "v1",
				},
				compensator: noopCompensator{},
			},
			wantErr: nil,
		},
		{
			name: "segment ID is required",
			segment: fakeSegment{
				desc: pipeline.SegmentDescriptor{
					ID:          "",
					Idempotency: pipeline.Idempotent,
					CompatibilityVersion: "v1",
				},
			},
			wantErr: pipeline.ErrSegmentIDRequired,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := pipeline.ValidateSegment[string, string](tc.segment)

			if tc.wantErr == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != nil && (err == nil || err.Error() != tc.wantErr.Error()) {
				t.Fatalf("expected error %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestValidateSourceContract(t *testing.T) {
	t.Parallel()

	t.Run("nil source is rejected", func(t *testing.T) {
		t.Parallel()

		var source *nilFakeSource
		err := pipeline.ValidateSource[string](source)
		if !errors.Is(err, pipeline.ErrSourceRequired) {
			t.Fatalf("expected error %q, got %v", pipeline.ErrSourceRequired, err)
		}
	})

	t.Run("source implementation is accepted", func(t *testing.T) {
		t.Parallel()

		err := pipeline.ValidateSource[string](&fakeSource{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestValidateStreamSourceContract(t *testing.T) {
	t.Parallel()

	t.Run("nil stream source is rejected", func(t *testing.T) {
		t.Parallel()

		var source *nilFakeStreamSource
		err := pipeline.ValidateStreamSource[string](source)
		if !errors.Is(err, pipeline.ErrStreamSourceRequired) {
			t.Fatalf("expected error %q, got %v", pipeline.ErrStreamSourceRequired, err)
		}
	})

	t.Run("stream source implementation is accepted", func(t *testing.T) {
		t.Parallel()

		err := pipeline.ValidateStreamSource[string](fakeStreamSource{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestValidateSinkContract(t *testing.T) {
	t.Parallel()

	t.Run("nil sink is rejected", func(t *testing.T) {
		t.Parallel()

		var sink *nilFakeSink
		err := pipeline.ValidateSink[string](sink)
		if !errors.Is(err, pipeline.ErrSinkRequired) {
			t.Fatalf("expected error %q, got %v", pipeline.ErrSinkRequired, err)
		}
	})

	t.Run("sink implementation is accepted", func(t *testing.T) {
		t.Parallel()

		err := pipeline.ValidateSink[string](&fakeSink{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestValidateRuntimeContract(t *testing.T) {
	t.Parallel()

	t.Run("nil runtime is rejected", func(t *testing.T) {
		t.Parallel()

		var runtime pipeline.Runtime
		err := pipeline.ValidateRuntime(runtime)
		if !errors.Is(err, pipeline.ErrRuntimeRequired) {
			t.Fatalf("expected error %q, got %v", pipeline.ErrRuntimeRequired, err)
		}
	})

	t.Run("runtime implementation is accepted", func(t *testing.T) {
		t.Parallel()

		err := pipeline.ValidateRuntime(pipeline.NewInMemoryRuntime())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestEnvelopeTraceabilityFanOutShape(t *testing.T) {
	t.Parallel()

	root := pipeline.Envelope[string]{
		OriginRecordID: "src-1",
		RecordID:       "src-1",
		AttemptID:      1,
		SegmentPath:    []pipeline.SegmentID{"source"},
		Payload:        "input",
	}

	childA := pipeline.Envelope[string]{
		OriginRecordID: root.OriginRecordID,
		RecordID:       "child-A",
		AttemptID:      root.AttemptID,
		ParentIDs:      []pipeline.RecordID{root.RecordID},
		SegmentPath:    []pipeline.SegmentID{"source", "splitter"},
		Payload:        "left",
	}
	childB := pipeline.Envelope[string]{
		OriginRecordID: root.OriginRecordID,
		RecordID:       "child-B",
		AttemptID:      root.AttemptID,
		ParentIDs:      []pipeline.RecordID{root.RecordID},
		SegmentPath:    []pipeline.SegmentID{"source", "splitter"},
		Payload:        "right",
	}

	if childA.OriginRecordID != root.OriginRecordID {
		t.Fatalf("childA did not preserve origin record identity")
	}
	if childB.OriginRecordID != root.OriginRecordID {
		t.Fatalf("childB did not preserve origin record identity")
	}
	if len(childA.ParentIDs) != 1 || childA.ParentIDs[0] != root.RecordID {
		t.Fatalf("childA does not correctly link to parent record")
	}
	if len(childB.ParentIDs) != 1 || childB.ParentIDs[0] != root.RecordID {
		t.Fatalf("childB does not correctly link to parent record")
	}
}

func TestSourcePullContractShape(t *testing.T) {
	t.Parallel()

	p := &fakeSource{
		items: []pipeline.SourceRecord[string]{
			{RecordID: "r1", Payload: "one"},
			{RecordID: "r2", Payload: "two"},
		},
	}

	item, ok, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on first item: %v", err)
	}
	if !ok || item.RecordID != "r1" || item.Payload != "one" {
		t.Fatalf("unexpected first item: ok=%v item=%+v", ok, item)
	}

	_, ok, err = p.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on second item: %v", err)
	}
	if !ok {
		t.Fatalf("expected second item to be available")
	}

	_, ok, err = p.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error at end-of-stream: %v", err)
	}
	if ok {
		t.Fatalf("expected end-of-stream signal")
	}

	cursor, err := p.SnapshotCursor(context.Background())
	if err != nil {
		t.Fatalf("unexpected snapshot cursor error: %v", err)
	}
	p.idx = 0
	if err := p.RestoreCursor(context.Background(), cursor); err != nil {
		t.Fatalf("unexpected restore cursor error: %v", err)
	}
	if p.idx != len(p.items) {
		t.Fatalf("expected restore cursor to restore source position, got %d", p.idx)
	}
}

func TestSinkWithDoneLifecycleShape(t *testing.T) {
	t.Parallel()

	c := &fakeSink{}
	in := pipeline.Envelope[string]{
		OriginRecordID: "r1",
		RecordID:       "sink-r1",
		AttemptID:      1,
		Payload:        "payload",
	}

	if err := c.Consume(context.Background(), in); err != nil {
		t.Fatalf("unexpected consume error: %v", err)
	}
	if len(c.received) != 1 || c.received[0].OriginRecordID != "r1" {
		t.Fatalf("sink did not capture expected envelope")
	}

	if err := c.Done(context.Background()); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
	if c.doneCall != 1 {
		t.Fatalf("expected done to be called once, got %d", c.doneCall)
	}
}

func TestRuntimeContractShape(t *testing.T) {
	t.Parallel()

	runtime := pipeline.NewInMemoryRuntime()
	if err := runtime.SaveSourceResumeState(context.Background(), pipeline.SourceResumeState{
		PipelineID:   "pipe-1",
		SourceCursor: []byte("cursor"),
		Paused:       true,
	}); err != nil {
		t.Fatalf("save source resume state failed: %v", err)
	}

	resumeState, ok, err := runtime.LoadSourceResumeState(context.Background(), "pipe-1")
	if err != nil {
		t.Fatalf("load source resume state failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected source resume state to be found")
	}
	if !resumeState.Paused || string(resumeState.SourceCursor) != "cursor" {
		t.Fatalf("unexpected source resume state: %+v", resumeState)
	}

	if err := runtime.CommitSourceProgress(context.Background(), pipeline.SourceProgress{
		ResumeState: pipeline.SourceResumeState{
			PipelineID:   "pipe-1",
			SourceCursor: []byte("cursor-1b"),
			Paused:       false,
		},
		Started: pipeline.StartedRecord{
			PipelineID: "pipe-1",
			Item: pipeline.Envelope[json.RawMessage]{
				OriginRecordID: "rec-1",
				RecordID:       "rec-1",
				AttemptID:      1,
				Payload:        json.RawMessage(`{"source":"ok"}`),
			},
		},
		PendingWork: pipeline.PendingSegmentWork{
			PipelineID:    "pipe-1",
			NextSegmentID: "segment-a",
			Item: pipeline.Envelope[json.RawMessage]{
				OriginRecordID: "rec-1",
				RecordID:       "rec-1",
				AttemptID:      1,
				Payload:        json.RawMessage(`{"source":"ok"}`),
			},
		},
	}); err != nil {
		t.Fatalf("commit source progress failed: %v", err)
	}

	pending, err := runtime.PendingWork(context.Background(), "pipe-1")
	if err != nil {
		t.Fatalf("pending work failed: %v", err)
	}
	if len(pending) != 1 || pending[0].NextSegmentID != "segment-a" || pending[0].Item.RecordID != "rec-1" {
		t.Fatalf("unexpected pending work: %+v", pending)
	}

	started, err := runtime.SourceRecords(context.Background(), "pipe-1")
	if err != nil {
		t.Fatalf("source records failed: %v", err)
	}
	if len(started) != 1 || started[0].RecordID != "rec-1" {
		t.Fatalf("unexpected source records: %+v", started)
	}

	if err := runtime.CommitProgressUpdate(context.Background(), pipeline.RuntimeDelta{
		Progress: &pipeline.SegmentProgress{
			PipelineID:           "pipe-1",
			SegmentID:            "segment-a",
			CompatibilityVersion: "v1",
			RecordID:             "rec-1",
			AttemptID:            1,
			InputChecksum:        "checksum-1",
			Status:               pipeline.SegmentCompleted,
			EmittedCount:         1,
		},
		DeletePendingWork: &pipeline.PendingSegmentWorkKey{
			PipelineID:    "pipe-1",
			NextSegmentID: "segment-a",
			RecordID:      "rec-1",
		},
		OutputRecord: &pipeline.SegmentOutputRecord{
			PipelineID:           "pipe-1",
			SegmentID:            "segment-a",
			CompatibilityVersion: "v1",
			InputChecksum:        "checksum-1",
			Item: pipeline.Envelope[json.RawMessage]{
				OriginRecordID: "rec-1",
				RecordID:       "rec-1/segment-a",
				AttemptID:      1,
				ParentIDs:      []pipeline.RecordID{"rec-1"},
				Payload:        json.RawMessage(`{"value":"ok"}`),
			},
		},
	}); err != nil {
		t.Fatalf("commit progress update failed: %v", err)
	}

	progress, ok, err := runtime.LoadSegmentProgress(context.Background(), "pipe-1", "segment-a", "rec-1", 1)
	if err != nil {
		t.Fatalf("load segment progress failed: %v", err)
	}
	if !ok || progress.Status != pipeline.SegmentCompleted || progress.EmittedCount != 1 {
		t.Fatalf("unexpected segment progress: ok=%v progress=%+v", ok, progress)
	}

	if err := runtime.CommitSourceProgress(context.Background(), pipeline.SourceProgress{
		ResumeState: pipeline.SourceResumeState{
			PipelineID:   "pipe-2",
			SourceCursor: []byte("cursor-2"),
			Paused:       false,
		},
		Started: pipeline.StartedRecord{
			PipelineID: "pipe-2",
			Item: pipeline.Envelope[json.RawMessage]{
				OriginRecordID: "rec-2",
				RecordID:       "rec-2",
				AttemptID:      1,
				Payload:        json.RawMessage(`{"source":"recovery"}`),
			},
		},
	}); err != nil {
		t.Fatalf("commit source progress failed: %v", err)
	}

	if err := runtime.CommitProgressUpdate(context.Background(), pipeline.RuntimeDelta{
		Progress: &pipeline.SegmentProgress{
			PipelineID:           "pipe-2",
			SegmentID:            "segment-b",
			CompatibilityVersion: "v1",
			RecordID:             "rec-2",
			AttemptID:            1,
			InputChecksum:        "checksum-2",
			Status:               pipeline.SegmentCompleted,
			EmittedCount:         1,
		},
		OutputRecord: &pipeline.SegmentOutputRecord{
			PipelineID:           "pipe-2",
			SegmentID:            "segment-b",
			CompatibilityVersion: "v1",
			InputChecksum:        "checksum-2",
			Item: pipeline.Envelope[json.RawMessage]{
				OriginRecordID: "rec-2",
				RecordID:       "rec-2/segment-b",
				AttemptID:      1,
				ParentIDs:      []pipeline.RecordID{"rec-2"},
				Payload:        json.RawMessage(`{"value":"recovery"}`),
			},
		},
		TerminalRecord: &pipeline.TerminalRecord{
			PipelineID: "pipe-2",
			Item: pipeline.Envelope[json.RawMessage]{
				OriginRecordID: "rec-2",
				RecordID:       "rec-2/terminal",
				AttemptID:      1,
				Payload:        json.RawMessage(`"done"`),
			},
		},
		DeterministicResult: &pipeline.DeterministicSegmentResult{
			PipelineID:           "pipe-2",
			SegmentID:            "segment-b",
			CompatibilityVersion: "v1",
			InputChecksum:        "checksum-2",
			Outputs: []pipeline.DeterministicSegmentOutput{{
				Payload: json.RawMessage(`{"value":"recovery"}`),
			}},
		},
	}); err != nil {
		t.Fatalf("commit progress update failed: %v", err)
	}

	recoveryResumeState, ok, err := runtime.LoadSourceResumeState(context.Background(), "pipe-2")
	if err != nil {
		t.Fatalf("load source resume state failed: %v", err)
	}
	if !ok || string(recoveryResumeState.SourceCursor) != "cursor-2" {
		t.Fatalf("unexpected source resume state: ok=%v state=%+v", ok, recoveryResumeState)
	}

	trace, err := runtime.Trace(context.Background(), "pipe-2", "rec-2")
	if err != nil {
		t.Fatalf("trace failed: %v", err)
	}
	if len(trace) != 1 || string(trace[0].Payload) != `"done"` {
		t.Fatalf("unexpected trace outputs: %+v", trace)
	}

	deterministic, ok, err := runtime.DeterministicResult(context.Background(), "pipe-2", "segment-b", "v1", "checksum-2")
	if err != nil {
		t.Fatalf("deterministic result failed: %v", err)
	}
	if !ok || len(deterministic.Outputs) != 1 || string(deterministic.Outputs[0].Payload) != `{"value":"recovery"}` {
		t.Fatalf("unexpected deterministic result: ok=%v result=%+v", ok, deterministic)
	}

	if err := runtime.ResetPipeline(context.Background(), "pipe-2"); err != nil {
		t.Fatalf("reset pipeline failed: %v", err)
	}
	if _, ok, err := runtime.LoadSourceResumeState(context.Background(), "pipe-2"); err != nil {
		t.Fatalf("load reset source resume state failed: %v", err)
	} else if ok {
		t.Fatalf("expected reset pipeline source resume state to be removed")
	}
	if _, ok, err := runtime.DeterministicResult(context.Background(), "pipe-2", "segment-b", "v1", "checksum-2"); err != nil {
		t.Fatalf("load reset deterministic result failed: %v", err)
	} else if ok {
		t.Fatalf("expected reset pipeline deterministic cache to be removed")
	}
}

func TestApplyCouplingContractShape(t *testing.T) {
	t.Parallel()

	t.Run("successfully transforms valid json", func(t *testing.T) {
		t.Parallel()

		c := fakeCoupling{out: json.RawMessage(`{"to":"segment"}`)}
		got, err := pipeline.ApplyCoupling(c, json.RawMessage(`{"from":"segment"}`))
		if err != nil {
			t.Fatalf("unexpected apply coupling error: %v", err)
		}
		if string(got) != `{"to":"segment"}` {
			t.Fatalf("unexpected transformed json: %s", string(got))
		}
	})

	t.Run("rejects nil coupling", func(t *testing.T) {
		t.Parallel()

		_, err := pipeline.ApplyCoupling(nil, json.RawMessage(`{"from":"segment"}`))
		if !errors.Is(err, pipeline.ErrCouplingNil) {
			t.Fatalf("expected ErrCouplingNil, got %v", err)
		}
	})

	t.Run("rejects invalid input json", func(t *testing.T) {
		t.Parallel()

		c := fakeCoupling{out: json.RawMessage(`{"ok":true}`)}
		_, err := pipeline.ApplyCoupling(c, json.RawMessage(`{"broken"`))
		if !errors.Is(err, pipeline.ErrCouplingInputInvalidJSON) {
			t.Fatalf("expected ErrCouplingInputInvalidJSON, got %v", err)
		}
	})

	t.Run("rejects invalid output json", func(t *testing.T) {
		t.Parallel()

		c := fakeCoupling{out: json.RawMessage(`{"broken"`)}
		_, err := pipeline.ApplyCoupling(c, json.RawMessage(`{"from":"segment"}`))
		if !errors.Is(err, pipeline.ErrCouplingOutputInvalidJSON) {
			t.Fatalf("expected ErrCouplingOutputInvalidJSON, got %v", err)
		}
	})
}

func TestValidateTopologyContractShape(t *testing.T) {
	t.Parallel()

	baseSegments := []pipeline.SegmentDescriptor{
		{ID: "segment-source", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
		{ID: "segment-transform", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
		{ID: "segment-sink", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
	}
	baseCouplings := []pipeline.CouplingDescriptor{
		{ID: "c-source-transform", FromSegment: "segment-source", ToSegment: "segment-transform"},
		{ID: "c-transform-sink", FromSegment: "segment-transform", ToSegment: "segment-sink"},
	}

	t.Run("accepts valid linear topology", func(t *testing.T) {
		t.Parallel()

		cfg := pipeline.EngineConfig{
			Segments:  baseSegments,
			Couplings: baseCouplings,
			Topology: pipeline.Topology{
				Connections: []pipeline.Connection{
					{From: "segment-source", To: "segment-transform", CouplingID: "c-source-transform"},
					{From: "segment-transform", To: "segment-sink", CouplingID: "c-transform-sink"},
				},
			},
		}

		if err := pipeline.ValidateTopology(cfg); err != nil {
			t.Fatalf("unexpected topology validation error: %v", err)
		}
	})

	t.Run("rejects unknown segment", func(t *testing.T) {
		t.Parallel()

		cfg := pipeline.EngineConfig{
			Segments:  baseSegments,
			Couplings: baseCouplings,
			Topology: pipeline.Topology{
				Connections: []pipeline.Connection{
					{From: "segment-source", To: "segment-missing", CouplingID: "c-source-transform"},
				},
			},
		}

		err := pipeline.ValidateTopology(cfg)
		if !errors.Is(err, pipeline.ErrTopologyUnknownSegment) {
			t.Fatalf("expected ErrTopologyUnknownSegment, got %v", err)
		}
	})

	t.Run("rejects unknown coupling", func(t *testing.T) {
		t.Parallel()

		cfg := pipeline.EngineConfig{
			Segments:  baseSegments,
			Couplings: baseCouplings,
			Topology: pipeline.Topology{
				Connections: []pipeline.Connection{
					{From: "segment-source", To: "segment-transform", CouplingID: "c-missing"},
				},
			},
		}

		err := pipeline.ValidateTopology(cfg)
		if !errors.Is(err, pipeline.ErrTopologyUnknownCoupling) {
			t.Fatalf("expected ErrTopologyUnknownCoupling, got %v", err)
		}
	})

	t.Run("rejects coupling segment mismatch", func(t *testing.T) {
		t.Parallel()

		cfg := pipeline.EngineConfig{
			Segments:  baseSegments,
			Couplings: baseCouplings,
			Topology: pipeline.Topology{
				Connections: []pipeline.Connection{
					{From: "segment-source", To: "segment-sink", CouplingID: "c-source-transform"},
				},
			},
		}

		err := pipeline.ValidateTopology(cfg)
		if !errors.Is(err, pipeline.ErrTopologyCouplingSegmentMismatch) {
			t.Fatalf("expected ErrTopologyCouplingSegmentMismatch, got %v", err)
		}
	})

	t.Run("rejects ambiguous ordering when disallowed", func(t *testing.T) {
		t.Parallel()

		cfg := pipeline.EngineConfig{
			Segments: []pipeline.SegmentDescriptor{
				{ID: "segment-source", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
				{ID: "segment-a", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
				{ID: "segment-b", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
			},
			Couplings: []pipeline.CouplingDescriptor{
				{ID: "c-source-a"},
				{ID: "c-source-b"},
			},
			Topology: pipeline.Topology{
				AllowAmbiguousOrder: false,
				Connections: []pipeline.Connection{
					{From: "segment-source", To: "segment-a", CouplingID: "c-source-a"},
					{From: "segment-source", To: "segment-b", CouplingID: "c-source-b"},
				},
			},
		}

		err := pipeline.ValidateTopology(cfg)
		if !errors.Is(err, pipeline.ErrTopologyAmbiguousOrder) {
			t.Fatalf("expected ErrTopologyAmbiguousOrder, got %v", err)
		}
	})

	t.Run("rejects cyclic topology", func(t *testing.T) {
		t.Parallel()

		cfg := pipeline.EngineConfig{
			Segments: []pipeline.SegmentDescriptor{
				{ID: "segment-a", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
				{ID: "segment-b", Idempotency: pipeline.Idempotent, CompatibilityVersion: "v1"},
			},
			Couplings: []pipeline.CouplingDescriptor{
				{ID: "c-a-b"},
				{ID: "c-b-a"},
			},
			Topology: pipeline.Topology{
				Connections: []pipeline.Connection{
					{From: "segment-a", To: "segment-b", CouplingID: "c-a-b"},
					{From: "segment-b", To: "segment-a", CouplingID: "c-b-a"},
				},
			},
		}

		err := pipeline.ValidateTopology(cfg)
		if !errors.Is(err, pipeline.ErrTopologyCycle) {
			t.Fatalf("expected ErrTopologyCycle, got %v", err)
		}
	})
}
