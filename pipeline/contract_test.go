package pipeline_test

import (
	"context"
	"testing"

	"github.com/pierre/gopher-root/pipeline"
)

type noopCompensator struct{}

func (noopCompensator) Compensate(context.Context, pipeline.RecordID, pipeline.AttemptID, error) error {
	return nil
}

type fakeProducer struct {
	items []pipeline.Envelope[string]
	idx   int
}

func (p *fakeProducer) Next(_ context.Context) (pipeline.Envelope[string], bool, error) {
	if p.idx >= len(p.items) {
		return pipeline.Envelope[string]{}, false, nil
	}
	item := p.items[p.idx]
	p.idx++
	return item, true, nil
}

type fakeConsumer struct {
	received []pipeline.Envelope[string]
	doneCall int
}

func (c *fakeConsumer) Consume(_ context.Context, item pipeline.Envelope[string]) error {
	c.received = append(c.received, item)
	return nil
}

func (c *fakeConsumer) Done(context.Context) error {
	c.doneCall++
	return nil
}

type fakeStage struct {
	desc        pipeline.StageDescriptor
	compensator pipeline.Compensator
}

func (f fakeStage) Descriptor() pipeline.StageDescriptor { return f.desc }

func (f fakeStage) Process(
	_ context.Context,
	in pipeline.Envelope[string],
	emit func(pipeline.Envelope[string]) error,
) error {
	return emit(in)
}

func (f fakeStage) Flush(context.Context) error             { return nil }
func (f fakeStage) Snapshot(context.Context) ([]byte, error) { return []byte("ok"), nil }
func (f fakeStage) Restore(context.Context, []byte) error    { return nil }
func (f fakeStage) Compensator() pipeline.Compensator        { return f.compensator }

type fakeAckGraphStore struct {
	acks map[pipeline.StageID]map[pipeline.RecordID]pipeline.StageAck
}

var _ pipeline.AckGraphStore = (*fakeAckGraphStore)(nil)

func newFakeAckGraphStore() *fakeAckGraphStore {
	return &fakeAckGraphStore{
		acks: map[pipeline.StageID]map[pipeline.RecordID]pipeline.StageAck{},
	}
}

func (s *fakeAckGraphStore) CommitAck(_ context.Context, ack pipeline.StageAck) error {
	if _, ok := s.acks[ack.Stage]; !ok {
		s.acks[ack.Stage] = map[pipeline.RecordID]pipeline.StageAck{}
	}
	s.acks[ack.Stage][ack.RecordID] = ack
	return nil
}

func (s *fakeAckGraphStore) LinkParentChild(context.Context, pipeline.RecordID, pipeline.RecordID) error {
	return nil
}

func (s *fakeAckGraphStore) GetAck(
	_ context.Context,
	stage pipeline.StageID,
	record pipeline.RecordID,
) (pipeline.StageAck, bool, error) {
	perStage, ok := s.acks[stage]
	if !ok {
		return pipeline.StageAck{}, false, nil
	}
	ack, ok := perStage[record]
	return ack, ok, nil
}

func (s *fakeAckGraphStore) Children(context.Context, pipeline.RecordID) ([]pipeline.RecordID, error) {
	return nil, nil
}

func (s *fakeAckGraphStore) Parents(context.Context, pipeline.RecordID) ([]pipeline.RecordID, error) {
	return nil, nil
}

func (s *fakeAckGraphStore) PendingByStage(context.Context, pipeline.StageID) ([]pipeline.RecordID, error) {
	return nil, nil
}

func TestValidateStageContract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stage   fakeStage
		wantErr error
	}{
		{
			name: "idempotent stage without compensator is allowed",
			stage: fakeStage{
				desc: pipeline.StageDescriptor{
					ID:          "stage-A",
					Idempotency: pipeline.Idempotent,
					Version:     "v1",
				},
			},
			wantErr: nil,
		},
		{
			name: "non-idempotent stage must provide compensator",
			stage: fakeStage{
				desc: pipeline.StageDescriptor{
					ID:          "stage-B",
					Idempotency: pipeline.NonIdempotent,
					Version:     "v1",
				},
				compensator: nil,
			},
			wantErr: pipeline.ErrCompensatorRequired,
		},
		{
			name: "non-idempotent stage with compensator is allowed",
			stage: fakeStage{
				desc: pipeline.StageDescriptor{
					ID:          "stage-C",
					Idempotency: pipeline.NonIdempotent,
					Version:     "v1",
				},
				compensator: noopCompensator{},
			},
			wantErr: nil,
		},
		{
			name: "stage ID is required",
			stage: fakeStage{
				desc: pipeline.StageDescriptor{
					ID:          "",
					Idempotency: pipeline.Idempotent,
					Version:     "v1",
				},
			},
			wantErr: pipeline.ErrStageIDRequired,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := pipeline.ValidateStage[string, string](tc.stage)

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
		RecordID:  "src-1",
		AttemptID: 1,
		StagePath: []pipeline.StageID{"source"},
		Payload:   "input",
	}

	childA := pipeline.Envelope[string]{
		RecordID:  "child-A",
		AttemptID: root.AttemptID,
		ParentIDs: []pipeline.RecordID{root.RecordID},
		StagePath: []pipeline.StageID{"source", "splitter"},
		Payload:   "left",
	}
	childB := pipeline.Envelope[string]{
		RecordID:  "child-B",
		AttemptID: root.AttemptID,
		ParentIDs: []pipeline.RecordID{root.RecordID},
		StagePath: []pipeline.StageID{"source", "splitter"},
		Payload:   "right",
	}

	if len(childA.ParentIDs) != 1 || childA.ParentIDs[0] != root.RecordID {
		t.Fatalf("childA does not correctly link to parent record")
	}
	if len(childB.ParentIDs) != 1 || childB.ParentIDs[0] != root.RecordID {
		t.Fatalf("childB does not correctly link to parent record")
	}
}

func TestProducerPullContractShape(t *testing.T) {
	t.Parallel()

	p := &fakeProducer{
		items: []pipeline.Envelope[string]{
			{RecordID: "r1", AttemptID: 1, Payload: "one"},
			{RecordID: "r2", AttemptID: 1, Payload: "two"},
		},
	}

	item, ok, err := p.Next(context.Background())
	if err != nil {
		t.Fatalf("unexpected error on first item: %v", err)
	}
	if !ok || item.RecordID != "r1" {
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

func TestConsumerWithDoneLifecycleShape(t *testing.T) {
	t.Parallel()

	c := &fakeConsumer{}
	in := pipeline.Envelope[string]{RecordID: "r1", AttemptID: 1, Payload: "payload"}

	if err := c.Consume(context.Background(), in); err != nil {
		t.Fatalf("unexpected consume error: %v", err)
	}
	if len(c.received) != 1 || c.received[0].RecordID != "r1" {
		t.Fatalf("consumer did not capture expected envelope")
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
	ack := pipeline.StageAck{
		Stage:    "stage-a",
		RecordID: "rec-1",
		Attempt:  1,
		Status:   pipeline.AckCommitted,
	}

	if err := store.CommitAck(context.Background(), ack); err != nil {
		t.Fatalf("commit ack failed: %v", err)
	}

	got, ok, err := store.GetAck(context.Background(), "stage-a", "rec-1")
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
