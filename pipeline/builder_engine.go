package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
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
	resumeReason   ResumeReason
	previousAttemptID AttemptID
}

type frontierProgress struct {
	nextFrames []runtimeFrontierFrame
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

func (c *linearEngineCore[TSink]) nextSegmentIDForIndex(nextStageIndex int) SegmentID {
	if nextStageIndex < 0 || nextStageIndex >= len(c.plan.stages) {
		return SegmentID("")
	}
	return c.plan.stages[nextStageIndex].segment.desc.ID
}

func (c *linearEngineCore[TSink]) stageIndexForPendingWork(item PendingSegmentWork) (int, error) {
	if item.NextSegmentID == "" {
		if len(c.plan.stages) == 0 {
			return 0, nil
		}
		if len(item.Item.SegmentPath) == 0 || item.Item.SegmentPath[len(item.Item.SegmentPath)-1] != c.plan.stages[len(c.plan.stages)-1].segment.desc.ID {
			return 0, fmt.Errorf("%w: terminal work for record %q does not match current plan", ErrBuilderResumePlanMismatch, item.Item.RecordID)
		}
		return len(c.plan.stages), nil
	}

	for idx := range c.plan.stages {
		if c.plan.stages[idx].segment.desc.ID != item.NextSegmentID {
			continue
		}
		if idx == 0 {
			if len(item.Item.SegmentPath) != 0 {
				return 0, fmt.Errorf("%w: queued work for segment %q has unexpected predecessor path", ErrBuilderResumePlanMismatch, item.NextSegmentID)
			}
			return idx, nil
		}
		if len(item.Item.SegmentPath) == 0 || item.Item.SegmentPath[len(item.Item.SegmentPath)-1] != c.plan.stages[idx-1].segment.desc.ID {
			return 0, fmt.Errorf("%w: queued work for segment %q no longer matches current plan order", ErrBuilderResumePlanMismatch, item.NextSegmentID)
		}
		return idx, nil
	}

	return 0, fmt.Errorf("%w: queued segment %q is not present in the current plan", ErrBuilderResumePlanMismatch, item.NextSegmentID)
}

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
		resumeReason:   ResumeReasonUnknown,
	}}, nil
}

func (c *linearEngineCore[TSink]) processFrontierFrame(ctx context.Context, item runtimeFrontierFrame, pauseRequested func() bool) (frontierProgress, error) {
	frame := item.frame
	if item.nextStageIndex >= len(c.plan.stages) {
		c.debug("manifold delivering terminal output", "origin_record_id", frame.originRecordID, "record_id", frame.recordID)
		var sinkPayload TSink
		if err := json.Unmarshal(frame.payload, &sinkPayload); err != nil {
			return frontierProgress{}, fmt.Errorf("sink input decode failed: %w", err)
		}

		envelope := Envelope[TSink]{
			OriginRecordID: frame.originRecordID,
			RecordID:       frame.recordID,
			AttemptID:      frame.attemptID,
			ParentIDs:      append([]RecordID(nil), frame.parentIDs...),
			SegmentPath:    append([]SegmentID(nil), frame.segmentPath...),
			Payload:        sinkPayload,
			Metadata:       cloneMetadata(frame.metadata),
		}
		if err := c.sink.Consume(ctx, envelope); err != nil {
			return frontierProgress{nextFrames: []runtimeFrontierFrame{itemForRetry(frame, item.nextStageIndex)}}, err
		}
		payloadJSON, err := json.Marshal(sinkPayload)
		if err != nil {
			return frontierProgress{}, fmt.Errorf("sink payload encode failed: %w", err)
		}
		c.debug("manifold committed terminal output", "origin_record_id", envelope.OriginRecordID, "record_id", envelope.RecordID)
		if err := c.runtime.CommitProgressUpdate(recoveryContext(ctx), RuntimeDelta{
			DeletePendingWork: &PendingSegmentWorkKey{
				PipelineID:    c.pipelineID,
				NextSegmentID: c.nextSegmentIDForIndex(item.nextStageIndex),
				RecordID:      frame.recordID,
			},
			TerminalRecord: &TerminalRecord{
				PipelineID: c.pipelineID,
				Item: Envelope[json.RawMessage]{
					OriginRecordID: envelope.OriginRecordID,
					RecordID:       envelope.RecordID,
					AttemptID:      envelope.AttemptID,
					ParentIDs:      append([]RecordID(nil), envelope.ParentIDs...),
					SegmentPath:    append([]SegmentID(nil), envelope.SegmentPath...),
					Payload:        payloadJSON,
					Metadata:       cloneMetadata(envelope.Metadata),
				},
			},
		}); err != nil {
			return frontierProgress{}, err
		}
		return frontierProgress{}, nil
	}

	stage := c.plan.stages[item.nextStageIndex]
	compatibilityVersion := stage.segment.desc.CompatibilityVersion
	inputChecksum, err := segmentInputChecksum(frame.payload, frame.metadata)
	if err != nil {
		return frontierProgress{}, err
	}
	c.debug(
		"manifold processing segment",
		"segment_id", stage.segment.desc.ID,
		"origin_record_id", frame.originRecordID,
		"record_id", frame.recordID,
		"attempt_id", frame.attemptID,
	)
	progressAttemptID := frame.attemptID
	if item.previousAttemptID != 0 {
		progressAttemptID = item.previousAttemptID
	}
	progress, ok, err := c.runtime.LoadSegmentProgress(ctx, c.pipelineID, stage.segment.desc.ID, frame.recordID, progressAttemptID)
	if err != nil {
		return frontierProgress{}, err
	}
	if ok && progress.CompatibilityVersion != "" && progress.CompatibilityVersion != compatibilityVersion {
		ok = false
	}
	if ok && progress.InputChecksum != "" && progress.InputChecksum != inputChecksum {
		ok = false
	}
	if ok && progress.Status == SegmentCompleted {
		if err := c.runtime.CommitProgressUpdate(recoveryContext(ctx), RuntimeDelta{
			DeletePendingWork: &PendingSegmentWorkKey{
				PipelineID:    c.pipelineID,
				NextSegmentID: c.nextSegmentIDForIndex(item.nextStageIndex),
				RecordID:      frame.recordID,
			},
		}); err != nil {
			return frontierProgress{}, err
		}
		return frontierProgress{}, nil
	}
	if stage.segment.desc.IsDeterministic() && stage.segment.desc.Idempotency == Idempotent {
		cached, ok, err := c.runtime.DeterministicResult(ctx, c.pipelineID, stage.segment.desc.ID, compatibilityVersion, inputChecksum)
		if err != nil {
			return frontierProgress{}, err
		}
		if ok {
			c.debug("manifold reusing deterministic segment result", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID)
			return c.cachedSegmentProgress(ctx, stage, item, cached, compatibilityVersion, inputChecksum, progress.EmittedCount)
		}
	}
	if item.resumeReason == ResumeAfterCrash && stage.segment.desc.Idempotency == NonIdempotent && !stage.segment.compensatorNil() {
		c.debug("manifold compensating recovered frontier", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID)
		compensateAttemptID := frame.attemptID
		if item.previousAttemptID != 0 {
			compensateAttemptID = item.previousAttemptID
		}
		if err := stage.segment.compensate(ctx, frame.recordID, compensateAttemptID, errors.New("manifold resumed after unclean shutdown")); err != nil {
			return frontierProgress{}, err
		}
	}
	if item.resumeReason != ResumeReasonUnknown && stage.segment.canRecover() {
		c.debug("manifold recovering resumed segment input", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID, "reason", item.resumeReason)
		if err := stage.segment.recover(ctx, runtimeSegmentInput{
			originRecordID: frame.originRecordID,
			payload:        frame.payload,
			metadata:       frame.metadata,
			pauseRequested: pauseRequested,
		}, ResumeInfo{
			PipelineID: c.pipelineID,
			SegmentID:  stage.segment.desc.ID,
			RecordID:   frame.recordID,
			AttemptID:  frame.attemptID,
			Reason:     item.resumeReason,
		}); err != nil {
			return frontierProgress{}, err
		}
	}

	persistedEmitted := 0
	if ok {
		persistedEmitted = progress.EmittedCount
	}
	emittedThisRun := 0
	allOutputs := make([]runtimeSegmentOutput, 0, persistedEmitted+1)
	nextFrames := make([]runtimeFrontierFrame, 0)

	result, err := stage.segment.processJSON(ctx, runtimeSegmentInput{
		originRecordID: frame.originRecordID,
		payload:        frame.payload,
		metadata:       frame.metadata,
		pauseRequested: pauseRequested,
	}, func(out runtimeSegmentOutput) error {
		allOutputs = append(allOutputs, runtimeSegmentOutput{
			payload:  append(json.RawMessage(nil), out.payload...),
			metadata: cloneMetadata(out.metadata),
		})
		emittedThisRun++
		if emittedThisRun <= persistedEmitted {
			return nil
		}

		child := runtimeFrame{
			originRecordID: frame.originRecordID,
			recordID:       childRecordID(frame.recordID, stage.segment.desc.ID, emittedThisRun-1, emittedThisRun),
			attemptID:      frame.attemptID,
			parentIDs:      []RecordID{frame.recordID},
			segmentPath:    appendSegmentPath(frame.segmentPath, stage.segment.desc.ID),
			payload:        append(json.RawMessage(nil), out.payload...),
			metadata:       cloneMetadata(out.metadata),
		}
		rawPayload := append(json.RawMessage(nil), child.payload...)
		rawMetadata := cloneMetadata(child.metadata)
		if stage.couplingAfter != nil {
			coupled, err := ApplyCoupling(stage.couplingAfter, child.payload)
			if err != nil {
				return err
			}
			child.payload = coupled
			c.debug("manifold applied coupling", "segment_id", stage.segment.desc.ID, "record_id", child.recordID)
		}

		update := RuntimeDelta{
			Progress: &SegmentProgress{
				PipelineID:           c.pipelineID,
				SegmentID:            stage.segment.desc.ID,
				CompatibilityVersion: compatibilityVersion,
				RecordID:             frame.recordID,
				AttemptID:            frame.attemptID,
				InputChecksum:        inputChecksum,
				Status:               SegmentInProgress,
				EmittedCount:         emittedThisRun,
			},
			EnqueuePendingWork: []PendingSegmentWork{{
				PipelineID:    c.pipelineID,
				NextSegmentID: c.nextSegmentIDForIndex(item.nextStageIndex + 1),
				Item: Envelope[json.RawMessage]{
					OriginRecordID: child.originRecordID,
					RecordID:       child.recordID,
					AttemptID:      child.attemptID,
					ParentIDs:      append([]RecordID(nil), child.parentIDs...),
					SegmentPath:    append([]SegmentID(nil), child.segmentPath...),
					Payload:        append(json.RawMessage(nil), child.payload...),
					Metadata:       cloneMetadata(child.metadata),
				},
			}},
			OutputRecord: &SegmentOutputRecord{
				PipelineID:           c.pipelineID,
				SegmentID:            stage.segment.desc.ID,
				CompatibilityVersion: compatibilityVersion,
				InputChecksum:        inputChecksum,
				Item: Envelope[json.RawMessage]{
					OriginRecordID: child.originRecordID,
					RecordID:       child.recordID,
					AttemptID:      child.attemptID,
					ParentIDs:      append([]RecordID(nil), child.parentIDs...),
					SegmentPath:    append([]SegmentID(nil), child.segmentPath...),
					Payload:        rawPayload,
					Metadata:       rawMetadata,
				},
			},
		}
		if err := c.runtime.CommitProgressUpdate(recoveryContext(ctx), update); err != nil {
			return err
		}
		nextFrames = append(nextFrames, runtimeFrontierFrame{
			frame:          child,
			nextStageIndex: item.nextStageIndex + 1,
		})
		return nil
	})
	if err != nil {
		c.debug("manifold segment failed", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID, "err", err)
		if updateErr := c.runtime.CommitProgressUpdate(recoveryContext(ctx), RuntimeDelta{
			Progress: &SegmentProgress{
				PipelineID:           c.pipelineID,
				SegmentID:            stage.segment.desc.ID,
				CompatibilityVersion: compatibilityVersion,
				RecordID:             frame.recordID,
				AttemptID:            frame.attemptID,
				InputChecksum:        inputChecksum,
				Status:               SegmentRetryableFailed,
				EmittedCount:         maxInt(persistedEmitted, emittedThisRun),
			},
		}); updateErr != nil {
			return frontierProgress{}, updateErr
		}
		return frontierProgress{nextFrames: []runtimeFrontierFrame{itemForRetry(frame, item.nextStageIndex)}}, err
	}
	if result.Status == ProcessPaused && !pauseRequested() {
		return frontierProgress{}, fmt.Errorf("segment %q paused without an active pause request", stage.segment.desc.ID)
	}
	c.debug("manifold segment emitted outputs", "segment_id", stage.segment.desc.ID, "record_id", frame.recordID, "attempt_id", frame.attemptID, "outputs", len(allOutputs), "paused", result.Status == ProcessPaused)

	finalStatus := SegmentCompleted
	if result.Status == ProcessPaused {
		finalStatus = SegmentPaused
	}
	update := RuntimeDelta{
		Progress: &SegmentProgress{
			PipelineID:           c.pipelineID,
			SegmentID:            stage.segment.desc.ID,
			CompatibilityVersion: compatibilityVersion,
			RecordID:             frame.recordID,
			AttemptID:            frame.attemptID,
			InputChecksum:        inputChecksum,
			Status:               finalStatus,
			EmittedCount:         maxInt(persistedEmitted, emittedThisRun),
		},
	}
	if finalStatus == SegmentCompleted {
		update.DeletePendingWork = &PendingSegmentWorkKey{
			PipelineID:    c.pipelineID,
			NextSegmentID: c.nextSegmentIDForIndex(item.nextStageIndex),
			RecordID:      frame.recordID,
		}
		if stage.segment.desc.IsDeterministic() && stage.segment.desc.Idempotency == Idempotent {
			update.DeterministicResult = &DeterministicSegmentResult{
				PipelineID:           c.pipelineID,
				SegmentID:            stage.segment.desc.ID,
				CompatibilityVersion: compatibilityVersion,
				InputChecksum:        inputChecksum,
				Outputs:              outputsForDeterministicCache(allOutputs),
			}
		}
	}
	if err := c.runtime.CommitProgressUpdate(recoveryContext(ctx), update); err != nil {
		return frontierProgress{}, err
	}
	if finalStatus == SegmentPaused {
		return frontierProgress{nextFrames: append([]runtimeFrontierFrame{itemForRetry(frame, item.nextStageIndex)}, nextFrames...)}, nil
	}
	return frontierProgress{nextFrames: nextFrames}, nil
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

	mu           sync.Mutex
	running      bool
	paused       bool
	pauseRequest bool
	pauseWaiters []chan error
}

func (e *linearPullEngine[TSource, TSink]) Run(ctx context.Context) error {
	return e.runPull(ctx, false)
}

func (e *linearPullEngine[TSource, TSink]) Pause(ctx context.Context) error {
	e.mu.Lock()
	if e.paused {
		e.mu.Unlock()
		return nil
	}
	if !e.running {
		e.mu.Unlock()
		if err := e.saveSourceResumeState(ctx, true); err != nil {
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
	return e.runPull(ctx, true)
}

func (e *linearPullEngine[TSource, TSink]) Restart(ctx context.Context) error {
	e.core.debug("manifold restart requested")
	writeCtx := recoveryContext(ctx)
	if err := e.core.runtime.ResetPipeline(writeCtx, e.core.pipelineID); err != nil {
		return err
	}
	if e.core.options.onRestart != nil {
		e.core.debug("manifold invoking restart hook")
		if err := e.core.options.onRestart(writeCtx, e.core.pipelineID); err != nil {
			return err
		}
	}
	if err := e.source.RestoreCursor(writeCtx, nil); err != nil {
		return err
	}
	return e.runFresh(ctx)
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
			queue := append([]runtimeFrontierFrame(nil), frontier...)
			for len(queue) > 0 {
				item := queue[0]
				queue = queue[1:]

				progress, err := e.core.processFrontierFrame(ctx, item, func() bool { return false })
				if err != nil {
					return err
				}
				queue = append(progress.nextFrames, queue...)
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

func (e *linearPushEngine[TSource, TSink]) Restart(_ context.Context) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPushEngine[TSource, TSink]) Retry(_ context.Context, _ RecordID) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPushEngine[TSource, TSink]) Trace(ctx context.Context, source RecordID) ([]Envelope[TSink], error) {
	return e.core.trace(ctx, source)
}

func (e *linearPullEngine[TSource, TSink]) saveSourceResumeState(ctx context.Context, paused bool) error {
	writeCtx := recoveryContext(ctx)
	cursor, err := e.source.SnapshotCursor(writeCtx)
	if err != nil {
		return err
	}
	e.core.debug("manifold saving source resume state", "paused", paused)
	return e.core.runtime.SaveSourceResumeState(writeCtx, SourceResumeState{
		PipelineID:   e.core.pipelineID,
		SourceCursor: cursor,
		Paused:       paused,
	})
}

func (e *linearPullEngine[TSource, TSink]) runFrontier(ctx context.Context, frontier []runtimeFrontierFrame) ([]runtimeFrontierFrame, bool, error) {
	queue := append([]runtimeFrontierFrame(nil), frontier...)
	for len(queue) > 0 {
		if e.shouldPause(ctx) {
			if err := e.pauseWithFrontier(ctx); err != nil {
				return nil, false, err
			}
			return queue, true, nil
		}

		item := queue[0]
		queue = queue[1:]

		progress, err := e.core.processFrontierFrame(ctx, item, func() bool {
			return e.shouldPause(ctx)
		})
		nextQueue := append(append([]runtimeFrontierFrame(nil), progress.nextFrames...), queue...)
		if err != nil {
			return nil, false, err
		}
		queue = nextQueue
	}
	return nil, false, nil
}

func (e *linearPullEngine[TSource, TSink]) isPauseRequested() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.pauseRequest
}

func (e *linearPullEngine[TSource, TSink]) pauseWithFrontier(ctx context.Context) error {
	if err := e.saveSourceResumeState(recoveryContext(ctx), true); err != nil {
		return err
	}
	e.core.debug("manifold pause source resume state saved")

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

func (e *linearPullEngine[TSource, TSink]) runPull(ctx context.Context, requireRecovery bool) error {
	frontier, recovered, err := e.loadRecovery(ctx, requireRecovery)
	if err != nil {
		return err
	}
	return e.runWithFrontier(ctx, frontier, recovered)
}

func (e *linearPullEngine[TSource, TSink]) runFresh(ctx context.Context) error {
	return e.runWithFrontier(ctx, nil, false)
}

func (e *linearPullEngine[TSource, TSink]) runWithFrontier(ctx context.Context, frontier []runtimeFrontierFrame, recovered bool) error {
	e.mu.Lock()
	e.running = true
	e.paused = false
	e.pauseRequest = false
	e.mu.Unlock()
	defer e.finishRun(nil)

	e.core.reset()
	e.core.debug("manifold pull engine starting", "resuming", recovered)
	if recovered && len(frontier) > 0 && frontier[0].resumeReason == ResumeAfterPause {
		if err := e.saveSourceResumeState(recoveryContext(ctx), false); err != nil {
			e.finishRun(err)
			return err
		}
	}
	if len(frontier) > 0 {
		if _, paused, err := e.runFrontier(ctx, frontier); err != nil {
			e.finishRun(err)
			return err
		} else if paused {
			return nil
		}
	}

	for {
		if e.shouldPause(ctx) {
			if err := e.pauseWithFrontier(ctx); err != nil {
				e.finishRun(err)
				return err
			}
			return nil
		}

		record, ok, err := e.source.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				if saveErr := e.pauseWithFrontier(ctx); saveErr != nil {
					e.finishRun(saveErr)
					return saveErr
				}
				return nil
			}
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
		if err := e.commitSourceProgress(ctx, frontier[0]); err != nil {
			e.finishRun(err)
			return err
		}
		if _, paused, err := e.runFrontier(ctx, frontier); err != nil {
			e.finishRun(err)
			return err
		} else if paused {
			return nil
		}
	}
	if err := e.core.finish(ctx); err != nil {
		e.finishRun(err)
		return err
	}
	if err := e.saveSourceResumeState(ctx, false); err != nil {
		e.finishRun(err)
		return err
	}
	e.core.debug("manifold pull engine finished")
	return nil
}

func (e *linearPullEngine[TSource, TSink]) loadRecovery(ctx context.Context, requireRecovery bool) ([]runtimeFrontierFrame, bool, error) {
	resumeState, ok, err := e.core.runtime.LoadSourceResumeState(ctx, e.core.pipelineID)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		if requireRecovery {
			return nil, false, ErrBuilderResumeStateRequired
		}
		return nil, false, nil
	}

	e.core.debug("manifold loaded source resume state", "paused", resumeState.Paused)
	if err := e.source.RestoreCursor(ctx, resumeState.SourceCursor); err != nil {
		return nil, false, err
	}
	work, err := e.core.runtime.PendingWork(ctx, e.core.pipelineID)
	if err != nil {
		return nil, false, err
	}
	frontier := make([]runtimeFrontierFrame, 0, len(work))
	resumeReason := ResumeAfterCrash
	if resumeState.Paused {
		resumeReason = ResumeAfterPause
	}
	for _, item := range work {
		stageIndex, err := e.core.stageIndexForPendingWork(item)
		if err != nil {
			return nil, false, err
		}
		frontier = append(frontier, runtimeFrontierFrame{
			frame: runtimeFrame{
				originRecordID: item.Item.OriginRecordID,
				recordID:       item.Item.RecordID,
				attemptID:      item.Item.AttemptID + 1,
				parentIDs:      append([]RecordID(nil), item.Item.ParentIDs...),
				segmentPath:    append([]SegmentID(nil), item.Item.SegmentPath...),
				payload:        append(json.RawMessage(nil), item.Item.Payload...),
				metadata:       cloneMetadata(item.Item.Metadata),
			},
			nextStageIndex:     stageIndex,
			resumeReason:       resumeReason,
			previousAttemptID:  item.Item.AttemptID,
		})
	}
	return frontier, true, nil
}

func (e *linearPullEngine[TSource, TSink]) shouldPause(ctx context.Context) bool {
	return e.isPauseRequested() || ctx.Err() != nil
}

func recoveryContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	if ctx.Err() == nil {
		return ctx
	}
	return context.WithoutCancel(ctx)
}

func (e *linearPullEngine[TSource, TSink]) commitSourceProgress(ctx context.Context, item runtimeFrontierFrame) error {
	writeCtx := recoveryContext(ctx)
	cursor, err := e.source.SnapshotCursor(writeCtx)
	if err != nil {
		return err
	}
	return e.core.runtime.CommitSourceProgress(writeCtx, SourceProgress{
		ResumeState: SourceResumeState{
			PipelineID:   e.core.pipelineID,
			SourceCursor: cursor,
			Paused:       false,
		},
		Started: StartedRecord{
			PipelineID: e.core.pipelineID,
			Item: Envelope[json.RawMessage]{
				OriginRecordID: item.frame.originRecordID,
				RecordID:       item.frame.recordID,
				AttemptID:      item.frame.attemptID,
				ParentIDs:      append([]RecordID(nil), item.frame.parentIDs...),
				SegmentPath:    append([]SegmentID(nil), item.frame.segmentPath...),
				Payload:        append(json.RawMessage(nil), item.frame.payload...),
				Metadata:       cloneMetadata(item.frame.metadata),
			},
		},
		PendingWork: PendingSegmentWork{
			PipelineID:    e.core.pipelineID,
			NextSegmentID: e.core.nextSegmentIDForIndex(item.nextStageIndex),
			Item: Envelope[json.RawMessage]{
				OriginRecordID: item.frame.originRecordID,
				RecordID:       item.frame.recordID,
				AttemptID:      item.frame.attemptID,
				ParentIDs:      append([]RecordID(nil), item.frame.parentIDs...),
				SegmentPath:    append([]SegmentID(nil), item.frame.segmentPath...),
				Payload:        append(json.RawMessage(nil), item.frame.payload...),
				Metadata:       cloneMetadata(item.frame.metadata),
			},
		},
	})
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

func itemForRetry(frame runtimeFrame, nextStageIndex int) runtimeFrontierFrame {
	return runtimeFrontierFrame{
		frame: runtimeFrame{
			originRecordID: frame.originRecordID,
			recordID:       frame.recordID,
			attemptID:      frame.attemptID + 1,
			parentIDs:      append([]RecordID(nil), frame.parentIDs...),
			segmentPath:    append([]SegmentID(nil), frame.segmentPath...),
			payload:        append(json.RawMessage(nil), frame.payload...),
			metadata:       cloneMetadata(frame.metadata),
		},
		nextStageIndex:    nextStageIndex,
		previousAttemptID: frame.attemptID,
	}
}

func segmentInputChecksum(payload json.RawMessage, metadata map[string]string) (string, error) {
	metadataJSON, err := json.Marshal(cloneMetadata(metadata))
	if err != nil {
		return "", fmt.Errorf("marshal metadata for checksum: %w", err)
	}
	sum := sha256.Sum256(append(append([]byte(nil), payload...), metadataJSON...))
	return fmt.Sprintf("%x", sum[:]), nil
}

func outputsForDeterministicCache(outputs []runtimeSegmentOutput) []DeterministicSegmentOutput {
	cached := make([]DeterministicSegmentOutput, len(outputs))
	for i := range outputs {
		cached[i] = DeterministicSegmentOutput{
			Payload:  append(json.RawMessage(nil), outputs[i].payload...),
			Metadata: cloneMetadata(outputs[i].metadata),
		}
	}
	return cached
}

func (c *linearEngineCore[TSink]) cachedSegmentProgress(ctx context.Context, stage runtimeStage, item runtimeFrontierFrame, cached DeterministicSegmentResult, compatibilityVersion string, inputChecksum string, persistedEmitted int) (frontierProgress, error) {
	frame := item.frame
	nextFrames := make([]runtimeFrontierFrame, 0, len(cached.Outputs))

	for index, cachedOutput := range cached.Outputs {
		if index < persistedEmitted {
			continue
		}
		child := runtimeFrame{
			originRecordID: frame.originRecordID,
			recordID:       childRecordID(frame.recordID, stage.segment.desc.ID, index, len(cached.Outputs)),
			attemptID:      frame.attemptID,
			parentIDs:      []RecordID{frame.recordID},
			segmentPath:    appendSegmentPath(frame.segmentPath, stage.segment.desc.ID),
			payload:        append(json.RawMessage(nil), cachedOutput.Payload...),
			metadata:       cloneMetadata(cachedOutput.Metadata),
		}
		rawPayload := append(json.RawMessage(nil), child.payload...)
		rawMetadata := cloneMetadata(child.metadata)
		if stage.couplingAfter != nil {
			coupled, err := ApplyCoupling(stage.couplingAfter, child.payload)
			if err != nil {
				return frontierProgress{}, err
			}
			child.payload = coupled
		}
		if err := c.runtime.CommitProgressUpdate(recoveryContext(ctx), RuntimeDelta{
			Progress: &SegmentProgress{
				PipelineID:           c.pipelineID,
				SegmentID:            stage.segment.desc.ID,
				CompatibilityVersion: compatibilityVersion,
				RecordID:             frame.recordID,
				AttemptID:            frame.attemptID,
				InputChecksum:        inputChecksum,
				Status:               SegmentInProgress,
				EmittedCount:         index + 1,
			},
			EnqueuePendingWork: []PendingSegmentWork{{
				PipelineID:    c.pipelineID,
				NextSegmentID: c.nextSegmentIDForIndex(item.nextStageIndex + 1),
				Item: Envelope[json.RawMessage]{
					OriginRecordID: child.originRecordID,
					RecordID:       child.recordID,
					AttemptID:      child.attemptID,
					ParentIDs:      append([]RecordID(nil), child.parentIDs...),
					SegmentPath:    append([]SegmentID(nil), child.segmentPath...),
					Payload:        append(json.RawMessage(nil), child.payload...),
					Metadata:       cloneMetadata(child.metadata),
				},
			}},
			OutputRecord: &SegmentOutputRecord{
				PipelineID:           c.pipelineID,
				SegmentID:            stage.segment.desc.ID,
				CompatibilityVersion: compatibilityVersion,
				InputChecksum:        inputChecksum,
				Item: Envelope[json.RawMessage]{
					OriginRecordID: child.originRecordID,
					RecordID:       child.recordID,
					AttemptID:      child.attemptID,
					ParentIDs:      append([]RecordID(nil), child.parentIDs...),
					SegmentPath:    append([]SegmentID(nil), child.segmentPath...),
					Payload:        rawPayload,
					Metadata:       rawMetadata,
				},
			},
		}); err != nil {
			return frontierProgress{}, err
		}
		nextFrames = append(nextFrames, runtimeFrontierFrame{
			frame:          child,
			nextStageIndex: item.nextStageIndex + 1,
		})
	}

	if err := c.runtime.CommitProgressUpdate(recoveryContext(ctx), RuntimeDelta{
			Progress: &SegmentProgress{
			PipelineID:           c.pipelineID,
			SegmentID:            stage.segment.desc.ID,
			CompatibilityVersion: compatibilityVersion,
			RecordID:             frame.recordID,
			AttemptID:            frame.attemptID,
			InputChecksum:        inputChecksum,
			Status:               SegmentCompleted,
			EmittedCount:         maxInt(persistedEmitted, len(cached.Outputs)),
		},
		DeletePendingWork: &PendingSegmentWorkKey{
			PipelineID:    c.pipelineID,
			NextSegmentID: c.nextSegmentIDForIndex(item.nextStageIndex),
			RecordID:      frame.recordID,
		},
	}); err != nil {
		return frontierProgress{}, err
	}

	return frontierProgress{nextFrames: nextFrames}, nil
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
