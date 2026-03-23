package pipeline

import (
	"context"
	"encoding/json"
	"sync"
)

type progressKey struct {
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
	resumeStates  map[string]SourceResumeState
	sourceRecords map[string][]Envelope[json.RawMessage]
	pendingWork   map[string][]PendingSegmentWork
	progress      map[progressKey]SegmentProgress
	segmentOutput map[string]map[SegmentID]map[RecordID][]Envelope[json.RawMessage]
	deterministic map[string]map[SegmentID]map[string]DeterministicSegmentResult
	traces        map[string]map[RecordID][]Envelope[json.RawMessage]
}

// NewInMemoryRuntime returns the default dev/test runtime adapter.
func NewInMemoryRuntime() *InMemoryRuntime {
	return &InMemoryRuntime{
		resumeStates:  map[string]SourceResumeState{},
		sourceRecords: map[string][]Envelope[json.RawMessage]{},
		pendingWork:   map[string][]PendingSegmentWork{},
		progress:      map[progressKey]SegmentProgress{},
		segmentOutput: map[string]map[SegmentID]map[RecordID][]Envelope[json.RawMessage]{},
		deterministic: map[string]map[SegmentID]map[string]DeterministicSegmentResult{},
		traces:        map[string]map[RecordID][]Envelope[json.RawMessage]{},
	}
}

func (r *InMemoryRuntime) LoadSourceResumeState(_ context.Context, pipelineID string) (SourceResumeState, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	resumeState, ok := r.resumeStates[pipelineID]
	if !ok {
		return SourceResumeState{}, false, nil
	}
	return cloneSourceResumeState(resumeState), true, nil
}

func (r *InMemoryRuntime) SaveSourceResumeState(_ context.Context, resumeState SourceResumeState) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resumeStates[resumeState.PipelineID] = cloneSourceResumeState(resumeState)
	return nil
}

func (r *InMemoryRuntime) CommitSourceProgress(_ context.Context, progress SourceProgress) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resumeStates[progress.ResumeState.PipelineID] = cloneSourceResumeState(progress.ResumeState)
	r.sourceRecords[progress.Started.PipelineID] = append(r.sourceRecords[progress.Started.PipelineID], cloneRawEnvelope(progress.Started.Item))
	if progress.PendingWork.PipelineID != "" {
		r.pendingWork[progress.PendingWork.PipelineID] = append(r.pendingWork[progress.PendingWork.PipelineID], clonePendingWork(progress.PendingWork))
	}
	return nil
}

func (r *InMemoryRuntime) CommitProgressUpdate(_ context.Context, update RuntimeDelta) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if update.Progress != nil {
		progress := cloneSegmentProgress(*update.Progress)
		r.progress[progressKey{
			pipelineID: progress.PipelineID,
			segmentID:  progress.SegmentID,
			recordID:   progress.RecordID,
			attemptID:  progress.AttemptID,
		}] = progress
	}
	if update.DeletePendingWork != nil {
		items := r.pendingWork[update.DeletePendingWork.PipelineID]
		filtered := items[:0]
		for _, item := range items {
			if item.NextSegmentID == update.DeletePendingWork.NextSegmentID && item.Item.RecordID == update.DeletePendingWork.RecordID {
				continue
			}
			filtered = append(filtered, item)
		}
		r.pendingWork[update.DeletePendingWork.PipelineID] = append([]PendingSegmentWork(nil), filtered...)
	}
	for _, item := range update.EnqueuePendingWork {
		r.pendingWork[item.PipelineID] = append(r.pendingWork[item.PipelineID], clonePendingWork(item))
	}
	if update.OutputRecord != nil {
		output := *update.OutputRecord
		if _, ok := r.segmentOutput[output.PipelineID]; !ok {
			r.segmentOutput[output.PipelineID] = map[SegmentID]map[RecordID][]Envelope[json.RawMessage]{}
		}
		if _, ok := r.segmentOutput[output.PipelineID][output.SegmentID]; !ok {
			r.segmentOutput[output.PipelineID][output.SegmentID] = map[RecordID][]Envelope[json.RawMessage]{}
		}
		item := cloneRawEnvelope(output.Item)
		parent := RecordID("")
		if len(item.ParentIDs) > 0 {
			parent = item.ParentIDs[0]
		}
		r.segmentOutput[output.PipelineID][output.SegmentID][parent] = append(r.segmentOutput[output.PipelineID][output.SegmentID][parent], item)
	}
	if update.TerminalRecord != nil {
		terminal := *update.TerminalRecord
		if _, ok := r.traces[terminal.PipelineID]; !ok {
			r.traces[terminal.PipelineID] = map[RecordID][]Envelope[json.RawMessage]{}
		}
		item := cloneRawEnvelope(terminal.Item)
		r.traces[terminal.PipelineID][item.OriginRecordID] = append(r.traces[terminal.PipelineID][item.OriginRecordID], item)
	}
	if update.DeterministicResult != nil {
		result := cloneDeterministicResult(*update.DeterministicResult)
		if _, ok := r.deterministic[result.PipelineID]; !ok {
			r.deterministic[result.PipelineID] = map[SegmentID]map[string]DeterministicSegmentResult{}
		}
		if _, ok := r.deterministic[result.PipelineID][result.SegmentID]; !ok {
			r.deterministic[result.PipelineID][result.SegmentID] = map[string]DeterministicSegmentResult{}
		}
		r.deterministic[result.PipelineID][result.SegmentID][deterministicCacheKey(result.CompatibilityVersion, result.InputChecksum)] = result
	}
	return nil
}

func (r *InMemoryRuntime) ResetPipeline(_ context.Context, pipelineID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.resumeStates, pipelineID)
	delete(r.sourceRecords, pipelineID)
	delete(r.pendingWork, pipelineID)
	for key := range r.progress {
		if key.pipelineID == pipelineID {
			delete(r.progress, key)
		}
	}
	delete(r.segmentOutput, pipelineID)
	delete(r.deterministic, pipelineID)
	delete(r.traces, pipelineID)
	return nil
}

func (r *InMemoryRuntime) PendingWork(_ context.Context, pipelineID string) ([]PendingSegmentWork, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	items := r.pendingWork[pipelineID]
	out := make([]PendingSegmentWork, len(items))
	for i := range items {
		out[i] = clonePendingWork(items[i])
	}
	return out, nil
}

func (r *InMemoryRuntime) LoadSegmentProgress(_ context.Context, pipelineID string, segment SegmentID, record RecordID, attempt AttemptID) (SegmentProgress, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	progress, ok := r.progress[progressKey{
		pipelineID: pipelineID,
		segmentID:  segment,
		recordID:   record,
		attemptID:  attempt,
	}]
	if !ok {
		return SegmentProgress{}, false, nil
	}
	return cloneSegmentProgress(progress), true, nil
}

func (r *InMemoryRuntime) DeterministicResult(_ context.Context, pipelineID string, segment SegmentID, compatibilityVersion string, inputChecksum string) (DeterministicSegmentResult, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result, ok := r.deterministic[pipelineID][segment][deterministicCacheKey(compatibilityVersion, inputChecksum)]
	if !ok {
		return DeterministicSegmentResult{}, false, nil
	}
	return cloneDeterministicResult(result), true, nil
}

func (r *InMemoryRuntime) SourceRecords(_ context.Context, pipelineID string) ([]Envelope[json.RawMessage], error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	items := r.sourceRecords[pipelineID]
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

func cloneSourceResumeState(in SourceResumeState) SourceResumeState {
	return SourceResumeState{
		PipelineID:   in.PipelineID,
		SourceCursor: append([]byte(nil), in.SourceCursor...),
		Paused:       in.Paused,
	}
}

func clonePendingWork(in PendingSegmentWork) PendingSegmentWork {
	return PendingSegmentWork{
		PipelineID:    in.PipelineID,
		NextSegmentID: in.NextSegmentID,
		Item:          cloneRawEnvelope(in.Item),
	}
}

func cloneSegmentProgress(in SegmentProgress) SegmentProgress {
	return SegmentProgress{
		PipelineID:           in.PipelineID,
		SegmentID:            in.SegmentID,
		CompatibilityVersion: in.CompatibilityVersion,
		RecordID:             in.RecordID,
		AttemptID:            in.AttemptID,
		InputChecksum:        in.InputChecksum,
		Status:               in.Status,
		EmittedCount:         in.EmittedCount,
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

func cloneDeterministicResult(in DeterministicSegmentResult) DeterministicSegmentResult {
	return DeterministicSegmentResult{
		PipelineID:           in.PipelineID,
		SegmentID:            in.SegmentID,
		CompatibilityVersion: in.CompatibilityVersion,
		InputChecksum:        in.InputChecksum,
		Outputs:              cloneDeterministicOutputs(in.Outputs),
	}
}

func deterministicCacheKey(compatibilityVersion string, inputChecksum string) string {
	return compatibilityVersion + "\x00" + inputChecksum
}

func cloneDeterministicOutputs(in []DeterministicSegmentOutput) []DeterministicSegmentOutput {
	out := make([]DeterministicSegmentOutput, len(in))
	for i := range in {
		out[i] = DeterministicSegmentOutput{
			Payload:  append(json.RawMessage(nil), in[i].Payload...),
			Metadata: cloneMetadata(in[i].Metadata),
		}
	}
	return out
}
