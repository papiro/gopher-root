package pipeline

import (
	"context"
	"encoding/json"
	"sync"
)

type ackKey struct {
	pipelineID string
	segmentID  SegmentID
	recordID   RecordID
}

type segmentStateKey struct {
	pipelineID string
	segmentID  SegmentID
	recordID   RecordID
	attemptID  AttemptID
}

// InMemoryRuntime stores framework-owned durability state in process memory.
//
// This adapter is intended for development and tests. It does not survive
// process restarts.
type InMemoryRuntime struct {
	mu            sync.RWMutex
	checkpoints   map[string]Checkpoint
	segmentStates map[segmentStateKey]SegmentState
	acks          map[ackKey]SegmentAck
	segmentOutput map[string]map[SegmentID]map[RecordID][]Envelope[json.RawMessage]
	traces        map[string]map[RecordID][]Envelope[json.RawMessage]
}

// NewInMemoryRuntime returns the default dev/test runtime adapter.
func NewInMemoryRuntime() *InMemoryRuntime {
	return &InMemoryRuntime{
		checkpoints:   map[string]Checkpoint{},
		segmentStates: map[segmentStateKey]SegmentState{},
		acks:          map[ackKey]SegmentAck{},
		segmentOutput: map[string]map[SegmentID]map[RecordID][]Envelope[json.RawMessage]{},
		traces:        map[string]map[RecordID][]Envelope[json.RawMessage]{},
	}
}

func (r *InMemoryRuntime) LoadCheckpoint(_ context.Context, pipelineID string) (Checkpoint, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	checkpoint, ok := r.checkpoints[pipelineID]
	if !ok {
		return Checkpoint{}, false, nil
	}
	return cloneCheckpoint(checkpoint), true, nil
}

func (r *InMemoryRuntime) SaveCheckpoint(_ context.Context, checkpoint Checkpoint) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.checkpoints[checkpoint.PipelineID] = cloneCheckpoint(checkpoint)
	return nil
}

func (r *InMemoryRuntime) SaveSegmentState(_ context.Context, state SegmentState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.segmentStates[segmentStateKey{
		pipelineID: state.PipelineID,
		segmentID:  state.SegmentID,
		recordID:   state.RecordID,
		attemptID:  state.AttemptID,
	}] = cloneSegmentState(state)
	return nil
}

func (r *InMemoryRuntime) LoadSegmentState(_ context.Context, pipelineID string, segment SegmentID, record RecordID, attempt AttemptID) (SegmentState, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	state, ok := r.segmentStates[segmentStateKey{
		pipelineID: pipelineID,
		segmentID:  segment,
		recordID:   record,
		attemptID:  attempt,
	}]
	if !ok {
		return SegmentState{}, false, nil
	}
	return cloneSegmentState(state), true, nil
}

func (r *InMemoryRuntime) DeleteSegmentState(_ context.Context, pipelineID string, segment SegmentID, record RecordID, attempt AttemptID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.segmentStates, segmentStateKey{
		pipelineID: pipelineID,
		segmentID:  segment,
		recordID:   record,
		attemptID:  attempt,
	})
	return nil
}

func (r *InMemoryRuntime) CommitSegment(_ context.Context, commit SegmentCommit) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := ackKey{
		pipelineID: commit.PipelineID,
		segmentID:  commit.SegmentID,
		recordID:   commit.RecordID,
	}
	r.acks[key] = SegmentAck{
		Segment:  commit.SegmentID,
		RecordID: commit.RecordID,
		Attempt:  commit.AttemptID,
		Status:   commit.Status,
		Err:      commit.Err,
	}
	return nil
}

func (r *InMemoryRuntime) CommitSegmentOutput(_ context.Context, output SegmentOutputRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.segmentOutput[output.PipelineID]; !ok {
		r.segmentOutput[output.PipelineID] = map[SegmentID]map[RecordID][]Envelope[json.RawMessage]{}
	}
	if _, ok := r.segmentOutput[output.PipelineID][output.SegmentID]; !ok {
		r.segmentOutput[output.PipelineID][output.SegmentID] = map[RecordID][]Envelope[json.RawMessage]{}
	}
	item := cloneRawEnvelope(output.Item)
	r.segmentOutput[output.PipelineID][output.SegmentID][item.OriginRecordID] = append(
		r.segmentOutput[output.PipelineID][output.SegmentID][item.OriginRecordID],
		item,
	)
	return nil
}

func (r *InMemoryRuntime) CommitTerminal(_ context.Context, terminal TerminalRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.traces[terminal.PipelineID]; !ok {
		r.traces[terminal.PipelineID] = map[RecordID][]Envelope[json.RawMessage]{}
	}
	item := cloneRawEnvelope(terminal.Item)
	r.traces[terminal.PipelineID][item.OriginRecordID] = append(r.traces[terminal.PipelineID][item.OriginRecordID], item)
	return nil
}

func (r *InMemoryRuntime) SegmentOutputs(_ context.Context, pipelineID string, segment SegmentID, origin RecordID) ([]Envelope[json.RawMessage], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	items := r.segmentOutput[pipelineID][segment][origin]
	out := make([]Envelope[json.RawMessage], len(items))
	for i := range items {
		out[i] = cloneRawEnvelope(items[i])
	}
	return out, nil
}

func (r *InMemoryRuntime) Trace(_ context.Context, pipelineID string, origin RecordID) ([]Envelope[json.RawMessage], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	items := r.traces[pipelineID][origin]
	out := make([]Envelope[json.RawMessage], len(items))
	for i := range items {
		out[i] = cloneRawEnvelope(items[i])
	}
	return out, nil
}

func (r *InMemoryRuntime) Ack(_ context.Context, pipelineID string, segment SegmentID, record RecordID) (SegmentAck, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ack, ok := r.acks[ackKey{
		pipelineID: pipelineID,
		segmentID:  segment,
		recordID:   record,
	}]
	if !ok {
		return SegmentAck{}, false, nil
	}
	return ack, true, nil
}

func cloneCheckpoint(in Checkpoint) Checkpoint {
	frontier := make([]CheckpointFrame, len(in.Frontier))
	for i := range in.Frontier {
		frontier[i] = cloneCheckpointFrame(in.Frontier[i])
	}
	return Checkpoint{
		PipelineID:   in.PipelineID,
		SourceCursor: append([]byte(nil), in.SourceCursor...),
		Frontier:     frontier,
		Paused:       in.Paused,
	}
}

func cloneCheckpointFrame(in CheckpointFrame) CheckpointFrame {
	return CheckpointFrame{
		OriginRecordID: in.OriginRecordID,
		RecordID:       in.RecordID,
		AttemptID:      in.AttemptID,
		ParentIDs:      append([]RecordID(nil), in.ParentIDs...),
		SegmentPath:    append([]SegmentID(nil), in.SegmentPath...),
		NextSegmentID:  in.NextSegmentID,
		Payload:        append(json.RawMessage(nil), in.Payload...),
		Metadata:       cloneMetadata(in.Metadata),
	}
}

func cloneSegmentState(in SegmentState) SegmentState {
	return SegmentState{
		PipelineID: in.PipelineID,
		SegmentID:  in.SegmentID,
		RecordID:   in.RecordID,
		AttemptID:  in.AttemptID,
		Snapshot:   append([]byte(nil), in.Snapshot...),
	}
}

func cloneRawEnvelope(in Envelope[json.RawMessage]) Envelope[json.RawMessage] {
	return Envelope[json.RawMessage]{
		OriginRecordID: in.OriginRecordID,
		RecordID:       in.RecordID,
		AttemptID:      in.AttemptID,
		ParentIDs:      append([]RecordID(nil), in.ParentIDs...),
		SegmentPath:    append([]SegmentID(nil), in.SegmentPath...),
		Payload:        append(json.RawMessage(nil), in.Payload...),
		Metadata:       cloneMetadata(in.Metadata),
	}
}
