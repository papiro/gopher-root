package pipeline_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/pierre/gopher-root/pipeline"
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

type fakeSink struct {
	received []pipeline.Envelope[string]
	doneCall int
}

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

type fakeSegment struct {
	desc        pipeline.SegmentDescriptor
	compensator pipeline.Compensator
}

func (f fakeSegment) Descriptor() pipeline.SegmentDescriptor { return f.desc }

func (f fakeSegment) Process(
	_ context.Context,
	in pipeline.Envelope[string],
	out func(pipeline.Envelope[string]) error,
) error {
	return out(in)
}

func (f fakeSegment) Flush(context.Context) error              { return nil }
func (f fakeSegment) Done(context.Context) error               { return nil }
func (f fakeSegment) Snapshot(context.Context) ([]byte, error) { return []byte("ok"), nil }
func (f fakeSegment) Restore(context.Context, []byte) error    { return nil }
func (f fakeSegment) Compensator() pipeline.Compensator        { return f.compensator }

type fakeAckGraphStore struct {
	acks map[pipeline.SegmentID]map[pipeline.RecordID]pipeline.SegmentAck
}

var _ pipeline.AckGraphStore = (*fakeAckGraphStore)(nil)

func newFakeAckGraphStore() *fakeAckGraphStore {
	return &fakeAckGraphStore{
		acks: map[pipeline.SegmentID]map[pipeline.RecordID]pipeline.SegmentAck{},
	}
}

func (s *fakeAckGraphStore) CommitAck(_ context.Context, ack pipeline.SegmentAck) error {
	if _, ok := s.acks[ack.Segment]; !ok {
		s.acks[ack.Segment] = map[pipeline.RecordID]pipeline.SegmentAck{}
	}
	s.acks[ack.Segment][ack.RecordID] = ack
	return nil
}

func (s *fakeAckGraphStore) LinkParentChild(context.Context, pipeline.RecordID, pipeline.RecordID) error {
	return nil
}

func (s *fakeAckGraphStore) GetAck(
	_ context.Context,
	segment pipeline.SegmentID,
	record pipeline.RecordID,
) (pipeline.SegmentAck, bool, error) {
	perSegment, ok := s.acks[segment]
	if !ok {
		return pipeline.SegmentAck{}, false, nil
	}
	ack, ok := perSegment[record]
	return ack, ok, nil
}

func (s *fakeAckGraphStore) Children(context.Context, pipeline.RecordID) ([]pipeline.RecordID, error) {
	return nil, nil
}

func (s *fakeAckGraphStore) Parents(context.Context, pipeline.RecordID) ([]pipeline.RecordID, error) {
	return nil, nil
}

func (s *fakeAckGraphStore) PendingBySegment(context.Context, pipeline.SegmentID) ([]pipeline.RecordID, error) {
	return nil, nil
}

func TestValidateSegmentContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		segment fakeSegment
		wantErr error
	}{
		{
			name: "idempotent segment without compensator is allowed",
			segment: fakeSegment{
				desc: pipeline.SegmentDescriptor{
					ID:          "segment-A",
					Idempotency: pipeline.Idempotent,
					Version:     "v1",
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
					Version:     "v1",
				},
				compensator: nil,
			},
			wantErr: pipeline.ErrCompensatorRequired,
		},
		{
			name: "non-idempotent segment with compensator is allowed",
			segment: fakeSegment{
				desc: pipeline.SegmentDescriptor{
					ID:          "segment-C",
					Idempotency: pipeline.NonIdempotent,
					Version:     "v1",
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
					Version:     "v1",
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

func TestEnvelopeTraceabilityFanOutShape(t *testing.T) {
	t.Parallel()

	root := pipeline.Envelope[string]{
		RecordID:    "src-1",
		AttemptID:   1,
		SegmentPath: []pipeline.SegmentID{"source"},
		Payload:     "input",
	}

	childA := pipeline.Envelope[string]{
		RecordID:    "child-A",
		AttemptID:   root.AttemptID,
		ParentIDs:   []pipeline.RecordID{root.RecordID},
		SegmentPath: []pipeline.SegmentID{"source", "splitter"},
		Payload:     "left",
	}
	childB := pipeline.Envelope[string]{
		RecordID:    "child-B",
		AttemptID:   root.AttemptID,
		ParentIDs:   []pipeline.RecordID{root.RecordID},
		SegmentPath: []pipeline.SegmentID{"source", "splitter"},
		Payload:     "right",
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
}

func TestSinkWithDoneLifecycleShape(t *testing.T) {
	t.Parallel()

	c := &fakeSink{}
	in := pipeline.Envelope[string]{RecordID: "r1", AttemptID: 1, Payload: "payload"}

	if err := c.Consume(context.Background(), in); err != nil {
		t.Fatalf("unexpected consume error: %v", err)
	}
	if len(c.received) != 1 || c.received[0].RecordID != "r1" {
		t.Fatalf("sink did not capture expected envelope")
	}

	if err := c.Done(context.Background()); err != nil {
		t.Fatalf("unexpected done error: %v", err)
	}
	if c.doneCall != 1 {
		t.Fatalf("expected done to be called once, got %d", c.doneCall)
	}
}

func TestAckGraphStoreContractShape(t *testing.T) {
	t.Parallel()

	store := newFakeAckGraphStore()
	ack := pipeline.SegmentAck{
		Segment:  "segment-a",
		RecordID: "rec-1",
		Attempt:  1,
		Status:   pipeline.AckCommitted,
	}

	if err := store.CommitAck(context.Background(), ack); err != nil {
		t.Fatalf("commit ack failed: %v", err)
	}

	got, ok, err := store.GetAck(context.Background(), "segment-a", "rec-1")
	if err != nil {
		t.Fatalf("get ack failed: %v", err)
	}
	if !ok {
		t.Fatalf("expected ack to be found")
	}
	if got.Status != pipeline.AckCommitted {
		t.Fatalf("unexpected ack status: %v", got.Status)
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
		{ID: "segment-source", Idempotency: pipeline.Idempotent, Version: "v1"},
		{ID: "segment-transform", Idempotency: pipeline.Idempotent, Version: "v1"},
		{ID: "segment-sink", Idempotency: pipeline.Idempotent, Version: "v1"},
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
				{ID: "segment-source", Idempotency: pipeline.Idempotent, Version: "v1"},
				{ID: "segment-a", Idempotency: pipeline.Idempotent, Version: "v1"},
				{ID: "segment-b", Idempotency: pipeline.Idempotent, Version: "v1"},
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
				{ID: "segment-a", Idempotency: pipeline.Idempotent, Version: "v1"},
				{ID: "segment-b", Idempotency: pipeline.Idempotent, Version: "v1"},
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
