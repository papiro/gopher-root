package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
)

type runtimeFrame struct {
	originRecordID RecordID
	recordID       RecordID
	attemptID      AttemptID
	parentIDs      []RecordID
	segmentPath    []SegmentID
	payload        json.RawMessage
	metadata       map[string]string
}

type runtimeFrontierFrame struct {
	frame          runtimeFrame
	nextStageIndex int
}

// linearEngineCore executes one compiled linear plan and records terminal outputs by source record.
type linearEngineCore[TSink any] struct {
	plan       runtimePlan
	sink       Sink[TSink]
	sinkDone   SinkWithDone[TSink]
	runtime    Runtime
	pipelineID string
	options    engineOptions
}

func newLinearEngineCore[TSink any](plan runtimePlan, sink Sink[TSink], sinkDone SinkWithDone[TSink], runtime Runtime, pipelineID string, options engineOptions) linearEngineCore[TSink] {
	return linearEngineCore[TSink]{
		plan:       plan,
		sink:       sink,
		sinkDone:   sinkDone,
		runtime:    runtime,
		pipelineID: pipelineID,
		options:    options,
	}
}

func (c *linearEngineCore[TSink]) reset() {}

func (c *linearEngineCore[TSink]) debug(message string, args ...any) {
	if c.options.debugLogger == nil {
		return
	}
	args = append([]any{"pipeline_id", c.pipelineID}, args...)
	c.options.debugLogger.Debug(message, args...)
}

func (c *linearEngineCore[TSink]) sourceFrontier(recordID RecordID, payload any, metadata map[string]string) ([]runtimeFrontierFrame, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("source payload encode failed: %w", err)
	}
	c.debug("manifold source emitted frontier frame", "origin_record_id", recordID, "record_id", recordID)

	return []runtimeFrontierFrame{{
		frame: runtimeFrame{
			originRecordID: recordID,
			recordID:       recordID,
			attemptID:      1,
			payload:        payloadJSON,
			metadata:       cloneMetadata(metadata),
		},
		nextStageIndex: 0,
	}}, nil
}

func (c *linearEngineCore[TSink]) processFrontierFrame(ctx context.Context, item runtimeFrontierFrame, pauseRequested func() bool) ([]runtimeFrontierFrame, error) {
	frame := item.frame
	if item.nextStageIndex >= len(c.plan.stages) {
		c.debug("manifold delivering terminal output", "origin_record_id", frame.originRecordID, "record_id", frame.recordID)
		var sinkPayload TSink
		if err := json.Unmarshal(frame.payload, &sinkPayload); err != nil {
			return nil, fmt.Errorf("sink input decode failed: %w", err)
		}

		item := Envelope[TSink]{
			OriginRecordID: frame.originRecordID,
			RecordID:       frame.recordID,
			AttemptID:      frame.attemptID,
			ParentIDs:      append([]RecordID(nil), frame.parentIDs...),
			SegmentPath:    append([]SegmentID(nil), frame.segmentPath...),
			Payload:        sinkPayload,
			Metadata:       cloneMetadata(frame.metadata),
		}
		if err := c.sink.Consume(ctx, item); err != nil {
			return nil, err
		}
		payloadJSON, err := json.Marshal(sinkPayload)
		if err != nil {
			return nil, fmt.Errorf("sink payload encode failed: %w", err)
		}
		if err := c.runtime.CommitTerminal(ctx, TerminalRecord{
			PipelineID: c.pipelineID,
			Item: Envelope[json.RawMessage]{
				OriginRecordID: item.OriginRecordID,
				RecordID:       item.RecordID,
				AttemptID:      item.AttemptID,
				ParentIDs:      append([]RecordID(nil), item.ParentIDs...),
				SegmentPath:    append([]SegmentID(nil), item.SegmentPath...),
				Payload:        payloadJSON,
				Metadata:       cloneMetadata(item.Metadata),
			},
		}); err != nil {
			return nil, err
		}
		c.debug("manifold committed terminal output", "origin_record_id", item.OriginRecordID, "record_id", item.RecordID)
		return nil, nil
	}

	stage := c.plan.stages[item.nextStageIndex]
	c.debug(
		"manifold processing segment",
		"segment_id", stage.segment.desc.ID,
		"origin_record_id", frame.originRecordID,
		"record_id", frame.recordID,
		"attempt_id", frame.attemptID,
	)
	state, ok, err := c.runtime.LoadSegmentState(ctx, c.pipelineID, stage.segment.desc.ID, frame.recordID, frame.attemptID)
	if err != nil {
		return nil, err
	}
	if ok {
		c.debug("manifold restoring segment state", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID)
		if err := stage.segment.restore(ctx, state.Snapshot); err != nil {
			return nil, err
		}
	}
	outputs, result, err := stage.segment.processJSON(ctx, runtimeSegmentInput{
		originRecordID: frame.originRecordID,
		payload:        frame.payload,
		metadata:       frame.metadata,
		pauseRequested: pauseRequested,
	})
	if err != nil {
		c.debug("manifold segment failed", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID, "err", err)
		_ = c.runtime.CommitSegment(ctx, SegmentCommit{
			PipelineID:     c.pipelineID,
			OriginRecordID: frame.originRecordID,
			RecordID:       frame.recordID,
			ParentIDs:      append([]RecordID(nil), frame.parentIDs...),
			SegmentID:      stage.segment.desc.ID,
			AttemptID:      frame.attemptID,
			Status:         AckRetryableFail,
			Err:            err,
		})
		return nil, err
	}
	if result.Status == ProcessPaused && !pauseRequested() {
		return nil, fmt.Errorf("segment %q paused without an active pause request", stage.segment.desc.ID)
	}
	if len(outputs) == 0 {
		if result.Status == ProcessPaused {
			if err := c.runtime.SaveSegmentState(ctx, SegmentState{
				PipelineID: c.pipelineID,
				SegmentID:  stage.segment.desc.ID,
				RecordID:   frame.recordID,
				AttemptID:  frame.attemptID,
				Snapshot:   append([]byte(nil), result.Snapshot...),
			}); err != nil {
				return nil, err
			}
			c.debug("manifold segment paused without outputs", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID)
			return []runtimeFrontierFrame{item}, nil
		}
		if err := c.runtime.DeleteSegmentState(ctx, c.pipelineID, stage.segment.desc.ID, frame.recordID, frame.attemptID); err != nil {
			return nil, err
		}
		if err := c.runtime.CommitSegment(ctx, SegmentCommit{
			PipelineID:     c.pipelineID,
			OriginRecordID: frame.originRecordID,
			RecordID:       frame.recordID,
			ParentIDs:      append([]RecordID(nil), frame.parentIDs...),
			SegmentID:      stage.segment.desc.ID,
			AttemptID:      frame.attemptID,
			Status:         AckCommitted,
		}); err != nil {
			return nil, err
		}
		c.debug("manifold segment completed without outputs", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID)
		return nil, nil
	}

	nextFrames := make([]runtimeFrontierFrame, 0, len(outputs))
	for idx, out := range outputs {
		child := runtimeFrame{
			originRecordID: frame.originRecordID,
			recordID:       childRecordID(frame.recordID, stage.segment.desc.ID, idx, len(outputs)),
			attemptID:      frame.attemptID,
			parentIDs:      []RecordID{frame.recordID},
			segmentPath:    appendSegmentPath(frame.segmentPath, stage.segment.desc.ID),
			payload:        out.payload,
			metadata:       cloneMetadata(out.metadata),
		}
		if err := c.runtime.CommitSegmentOutput(ctx, SegmentOutputRecord{
			PipelineID: c.pipelineID,
			SegmentID:  stage.segment.desc.ID,
			Item: Envelope[json.RawMessage]{
				OriginRecordID: child.originRecordID,
				RecordID:       child.recordID,
				AttemptID:      child.attemptID,
				ParentIDs:      append([]RecordID(nil), child.parentIDs...),
				SegmentPath:    append([]SegmentID(nil), child.segmentPath...),
				Payload:        append(json.RawMessage(nil), out.payload...),
				Metadata:       cloneMetadata(out.metadata),
			},
		}); err != nil {
			return nil, err
		}
		if stage.couplingAfter != nil {
			coupled, err := ApplyCoupling(stage.couplingAfter, child.payload)
			if err != nil {
				return nil, err
			}
			child.payload = coupled
			c.debug("manifold applied coupling", "segment_id", stage.segment.desc.ID, "record_id", child.recordID)
		}
		if err := c.runtime.CommitSegment(ctx, SegmentCommit{
			PipelineID:     c.pipelineID,
			OriginRecordID: child.originRecordID,
			RecordID:       child.recordID,
			ParentIDs:      append([]RecordID(nil), child.parentIDs...),
			SegmentID:      stage.segment.desc.ID,
			AttemptID:      child.attemptID,
			Status:         AckCommitted,
		}); err != nil {
			return nil, err
		}
		nextFrames = append(nextFrames, runtimeFrontierFrame{
			frame:          child,
			nextStageIndex: item.nextStageIndex + 1,
		})
	}
	c.debug("manifold segment emitted outputs", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID, "outputs", len(outputs), "paused", result.Status == ProcessPaused)

	if result.Status == ProcessPaused {
		if err := c.runtime.SaveSegmentState(ctx, SegmentState{
			PipelineID: c.pipelineID,
			SegmentID:  stage.segment.desc.ID,
			RecordID:   frame.recordID,
			AttemptID:  frame.attemptID,
			Snapshot:   append([]byte(nil), result.Snapshot...),
		}); err != nil {
			return nil, err
		}
		c.debug("manifold segment saved pause state", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID)
		return append([]runtimeFrontierFrame{item}, nextFrames...), nil
	}
	if err := c.runtime.DeleteSegmentState(ctx, c.pipelineID, stage.segment.desc.ID, frame.recordID, frame.attemptID); err != nil {
		return nil, err
	}

	return nextFrames, nil
}
func (c *linearEngineCore[TSink]) frontierFromCheckpoint(frames []CheckpointFrame) ([]runtimeFrontierFrame, error) {
	if len(frames) == 0 {
		return nil, nil
	}
	stageIndexByID := make(map[SegmentID]int, len(c.plan.stages))
	for idx, stage := range c.plan.stages {
		stageIndexByID[stage.segment.desc.ID] = idx
	}

	out := make([]runtimeFrontierFrame, 0, len(frames))
	for _, frame := range frames {
		nextStageIndex, ok := stageIndexByID[frame.NextSegmentID]
		if !ok {
			return nil, fmt.Errorf("checkpoint references unknown segment %q", frame.NextSegmentID)
		}
		out = append(out, runtimeFrontierFrame{
			frame: runtimeFrame{
				originRecordID: frame.OriginRecordID,
				recordID:       frame.RecordID,
				attemptID:      frame.AttemptID,
				parentIDs:      append([]RecordID(nil), frame.ParentIDs...),
				segmentPath:    append([]SegmentID(nil), frame.SegmentPath...),
				payload:        append(json.RawMessage(nil), frame.Payload...),
				metadata:       cloneMetadata(frame.Metadata),
			},
			nextStageIndex: nextStageIndex,
		})
	}
	return out, nil
}

func checkpointFrontierFromRuntime(in []runtimeFrontierFrame, plan runtimePlan) []CheckpointFrame {
	if len(in) == 0 {
		return nil
	}
	out := make([]CheckpointFrame, 0, len(in))
	for _, item := range in {
		if item.nextStageIndex >= len(plan.stages) {
			continue
		}
		out = append(out, CheckpointFrame{
			OriginRecordID: item.frame.originRecordID,
			RecordID:       item.frame.recordID,
			AttemptID:      item.frame.attemptID,
			ParentIDs:      append([]RecordID(nil), item.frame.parentIDs...),
			SegmentPath:    append([]SegmentID(nil), item.frame.segmentPath...),
			NextSegmentID:  plan.stages[item.nextStageIndex].segment.desc.ID,
			Payload:        append(json.RawMessage(nil), item.frame.payload...),
			Metadata:       cloneMetadata(item.frame.metadata),
		})
	}
	return out
}

func (c *linearEngineCore[TSink]) finish(ctx context.Context) error {
	// Segment Done hooks run in plan order after source consumption completes.
	for _, stage := range c.plan.stages {
		c.debug("manifold running segment done hook", "segment_id", stage.segment.desc.ID)
		if err := stage.segment.done(ctx); err != nil {
			return err
		}
	}
	if c.sinkDone != nil {
		c.debug("manifold running sink done hook")
		if err := c.sinkDone.Done(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *linearEngineCore[TSink]) trace(ctx context.Context, source RecordID) ([]Envelope[TSink], error) {
	items, err := c.runtime.Trace(ctx, c.pipelineID, source)
	if err != nil {
		return nil, err
	}

	out := make([]Envelope[TSink], 0, len(items))
	for _, item := range items {
		var payload TSink
		if err := json.Unmarshal(item.Payload, &payload); err != nil {
			return nil, fmt.Errorf("trace payload decode failed: %w", err)
		}
		out = append(out, Envelope[TSink]{
			OriginRecordID: item.OriginRecordID,
			RecordID:       item.RecordID,
			AttemptID:      item.AttemptID,
			ParentIDs:      append([]RecordID(nil), item.ParentIDs...),
			SegmentPath:    append([]SegmentID(nil), item.SegmentPath...),
			Payload:        payload,
			Metadata:       cloneMetadata(item.Metadata),
		})
	}
	return out, nil
}

type linearPullEngine[TSource, TSink any] struct {
	source Source[TSource]
	core   linearEngineCore[TSink]

	mu             sync.Mutex
	running        bool
	paused         bool
	pauseRequest   bool
	pauseWaiters   []chan error
	resumeFrontier []runtimeFrontierFrame
}

func (e *linearPullEngine[TSource, TSink]) Run(ctx context.Context) error {
	e.mu.Lock()
	e.running = true
	e.paused = false
	e.pauseRequest = false
	frontier := e.resumeFrontier
	e.resumeFrontier = nil
	e.mu.Unlock()
	defer e.finishRun(nil)

	e.core.reset()
	e.core.debug("manifold pull engine starting", "resuming", len(frontier) > 0)
	if len(frontier) > 0 {
		remaining, paused, err := e.runFrontier(ctx, frontier)
		if err != nil {
			e.finishRun(err)
			return err
		}
		if paused {
			e.mu.Lock()
			e.resumeFrontier = remaining
			e.mu.Unlock()
			return nil
		}
	}

	// Pull engines repeatedly ask the source for the next record until exhaustion.
	for {
		requested := e.isPauseRequested()
		if requested {
			if err := e.pauseWithFrontier(ctx, nil); err != nil {
				e.finishRun(err)
				return err
			}
			return nil
		}

		record, ok, err := e.source.Next(ctx)
		if err != nil {
			e.finishRun(err)
			return err
		}
		if !ok {
			break
		}
		frontier, err := e.core.sourceFrontier(record.RecordID, record.Payload, record.Metadata)
		if err != nil {
			e.finishRun(err)
			return err
		}
		remaining, paused, err := e.runFrontier(ctx, frontier)
		if err != nil {
			e.finishRun(err)
			return err
		}
		if paused {
			e.mu.Lock()
			e.resumeFrontier = remaining
			e.mu.Unlock()
			return nil
		}
	}
	if err := e.core.finish(ctx); err != nil {
		e.finishRun(err)
		return err
	}
	if err := e.saveCheckpoint(ctx, false, nil); err != nil {
		e.finishRun(err)
		return err
	}
	e.core.debug("manifold pull engine finished")
	return nil
}

func (e *linearPullEngine[TSource, TSink]) Pause(ctx context.Context) error {
	e.mu.Lock()
	if e.paused {
		e.mu.Unlock()
		return nil
	}
	if !e.running {
		frontier := append([]runtimeFrontierFrame(nil), e.resumeFrontier...)
		e.mu.Unlock()
		if err := e.saveCheckpoint(ctx, true, checkpointFrontierFromRuntime(frontier, e.core.plan)); err != nil {
			return err
		}
		e.mu.Lock()
		e.paused = true
		e.mu.Unlock()
		return nil
	}

	waiter := make(chan error, 1)
	e.pauseRequest = true
	e.pauseWaiters = append(e.pauseWaiters, waiter)
	e.mu.Unlock()
	e.core.debug("manifold pause requested")

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-waiter:
		return err
	}
}

func (e *linearPullEngine[TSource, TSink]) Resume(ctx context.Context) error {
	e.core.debug("manifold resume requested")
	checkpoint, ok, err := e.core.runtime.LoadCheckpoint(ctx, e.core.pipelineID)
	if err != nil {
		return err
	}
	if ok {
		e.core.debug("manifold loaded checkpoint", "frontier", len(checkpoint.Frontier), "paused", checkpoint.Paused)
		if err := e.source.RestoreCursor(ctx, checkpoint.SourceCursor); err != nil {
			return err
		}
		frontier, err := e.core.frontierFromCheckpoint(checkpoint.Frontier)
		if err != nil {
			return err
		}
		e.mu.Lock()
		e.resumeFrontier = frontier
		e.mu.Unlock()
	}

	e.mu.Lock()
	e.paused = false
	e.pauseRequest = false
	e.mu.Unlock()
	return e.Run(ctx)
}

func (e *linearPullEngine[TSource, TSink]) Retry(_ context.Context, _ RecordID) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPullEngine[TSource, TSink]) Trace(ctx context.Context, source RecordID) ([]Envelope[TSink], error) {
	return e.core.trace(ctx, source)
}

type linearPushEngine[TSource, TSink any] struct {
	source StreamSource[TSource]
	core   linearEngineCore[TSink]
}

func (e *linearPushEngine[TSource, TSink]) Run(ctx context.Context) error {
	e.core.reset()
	e.core.debug("manifold push engine starting")
	stream := e.source.Stream(ctx)
	// Push engines drain the source stream until it closes or the context is canceled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-stream:
			if !ok {
				e.core.debug("manifold push source completed")
				return e.core.finish(ctx)
			}
			frontier, err := e.core.sourceFrontier(record.RecordID, record.Payload, record.Metadata)
			if err != nil {
				return err
			}
			if _, _, err := (&linearPullEngine[TSource, TSink]{core: e.core}).runFrontier(ctx, frontier); err != nil {
				return err
			}
		}
	}
}

func (e *linearPushEngine[TSource, TSink]) Pause(_ context.Context) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPushEngine[TSource, TSink]) Resume(_ context.Context) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPushEngine[TSource, TSink]) Retry(_ context.Context, _ RecordID) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPushEngine[TSource, TSink]) Trace(ctx context.Context, source RecordID) ([]Envelope[TSink], error) {
	return e.core.trace(ctx, source)
}

func (e *linearPullEngine[TSource, TSink]) saveCheckpoint(ctx context.Context, paused bool, frontier []CheckpointFrame) error {
	cursor, err := e.source.SnapshotCursor(ctx)
	if err != nil {
		return err
	}
	e.core.debug("manifold saving checkpoint", "paused", paused, "frontier", len(frontier))
	return e.core.runtime.SaveCheckpoint(ctx, Checkpoint{
		PipelineID:   e.core.pipelineID,
		SourceCursor: cursor,
		Frontier:     cloneCheckpointFrames(frontier),
		Paused:       paused,
	})
}

func (e *linearPullEngine[TSource, TSink]) runFrontier(ctx context.Context, frontier []runtimeFrontierFrame) ([]runtimeFrontierFrame, bool, error) {
	queue := append([]runtimeFrontierFrame(nil), frontier...)
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]

		nextFrames, err := e.core.processFrontierFrame(ctx, item, e.isPauseRequested)
		if err != nil {
			return nil, false, err
		}
		queue = append(queue, nextFrames...)

		requested := e.isPauseRequested()
		if !requested {
			continue
		}

		if err := e.pauseWithFrontier(ctx, checkpointFrontierFromRuntime(queue, e.core.plan)); err != nil {
			return nil, false, err
		}
		return queue, true, nil
	}
	return nil, false, nil
}

func (e *linearPullEngine[TSource, TSink]) isPauseRequested() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pauseRequest
}

func (e *linearPullEngine[TSource, TSink]) pauseWithFrontier(ctx context.Context, frontier []CheckpointFrame) error {
	if err := e.saveCheckpoint(ctx, true, frontier); err != nil {
		return err
	}
	e.core.debug("manifold pause checkpoint saved", "frontier", len(frontier))

	e.mu.Lock()
	e.paused = true
	e.pauseRequest = false
	waiters := e.pauseWaiters
	e.pauseWaiters = nil
	e.mu.Unlock()
	for _, waiter := range waiters {
		waiter <- nil
	}
	return nil
}

func (e *linearPullEngine[TSource, TSink]) finishRun(err error) {
	e.mu.Lock()
	if !e.running && len(e.pauseWaiters) == 0 {
		e.mu.Unlock()
		return
	}
	e.running = false
	if err != nil {
		e.paused = false
	}
	waiters := e.pauseWaiters
	e.pauseWaiters = nil
	e.mu.Unlock()
	for _, waiter := range waiters {
		waiter <- err
	}
}

func cloneCheckpointFrames(in []CheckpointFrame) []CheckpointFrame {
	if len(in) == 0 {
		return nil
	}
	out := make([]CheckpointFrame, len(in))
	for i := range in {
		out[i] = CheckpointFrame{
			OriginRecordID: in[i].OriginRecordID,
			RecordID:       in[i].RecordID,
			AttemptID:      in[i].AttemptID,
			ParentIDs:      append([]RecordID(nil), in[i].ParentIDs...),
			SegmentPath:    append([]SegmentID(nil), in[i].SegmentPath...),
			NextSegmentID:  in[i].NextSegmentID,
			Payload:        append(json.RawMessage(nil), in[i].Payload...),
			Metadata:       cloneMetadata(in[i].Metadata),
		}
	}
	return out
}
