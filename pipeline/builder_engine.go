package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
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

// linearEngineCore executes one compiled linear plan and records terminal outputs by source record.
type linearEngineCore[TSink any] struct {
	plan     runtimePlan
	sink     Sink[TSink]
	sinkDone SinkWithDone[TSink]
	traces   map[RecordID][]Envelope[TSink]
}

func newLinearEngineCore[TSink any](plan runtimePlan, sink Sink[TSink], sinkDone SinkWithDone[TSink]) linearEngineCore[TSink] {
	return linearEngineCore[TSink]{
		plan:     plan,
		sink:     sink,
		sinkDone: sinkDone,
		traces:   map[RecordID][]Envelope[TSink]{},
	}
}

func (c *linearEngineCore[TSink]) reset() {
	c.traces = map[RecordID][]Envelope[TSink]{}
}

func (c *linearEngineCore[TSink]) runSourceRecord(ctx context.Context, recordID RecordID, payload any, metadata map[string]string) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("source payload encode failed: %w", err)
	}

	frames := []runtimeFrame{{
		originRecordID: recordID,
		recordID:       recordID,
		attemptID:      1,
		payload:        payloadJSON,
		metadata:       cloneMetadata(metadata),
	}}

	// Each stage consumes the current frontier of frames and produces the next
	// frontier, allowing one input record to fan out into multiple descendants.
	for _, stage := range c.plan.stages {
		nextFrames := make([]runtimeFrame, 0, len(frames))
		for _, frame := range frames {
			outputs, err := stage.segment.processJSON(ctx, runtimeSegmentInput{
				originRecordID: frame.originRecordID,
				payload:        frame.payload,
				metadata:       frame.metadata,
			})
			if err != nil {
				return err
			}

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
				if stage.couplingAfter != nil {
					coupled, err := ApplyCoupling(stage.couplingAfter, child.payload)
					if err != nil {
						return err
					}
					child.payload = coupled
				}
				nextFrames = append(nextFrames, child)
			}
		}
		frames = nextFrames
		if len(frames) == 0 {
			return nil
		}
	}

	// Only terminal frames that survive every stage are decoded into the sink type.
	for _, frame := range frames {
		var sinkPayload TSink
		if err := json.Unmarshal(frame.payload, &sinkPayload); err != nil {
			return fmt.Errorf("sink input decode failed: %w", err)
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
			return err
		}
		c.traces[item.OriginRecordID] = append(c.traces[item.OriginRecordID], item)
	}

	return nil
}

func (c *linearEngineCore[TSink]) finish(ctx context.Context) error {
	// Segment Done hooks run in plan order after source consumption completes.
	for _, stage := range c.plan.stages {
		if err := stage.segment.done(ctx); err != nil {
			return err
		}
	}
	if c.sinkDone != nil {
		if err := c.sinkDone.Done(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (c *linearEngineCore[TSink]) trace(source RecordID) []Envelope[TSink] {
	items := c.traces[source]
	// Return a copy so callers cannot mutate stored trace history.
	out := make([]Envelope[TSink], len(items))
	copy(out, items)
	return out
}

type linearPullEngine[TSource, TSink any] struct {
	source Source[TSource]
	core   linearEngineCore[TSink]
}

func (e *linearPullEngine[TSource, TSink]) Run(ctx context.Context) error {
	e.core.reset()
	// Pull engines repeatedly ask the source for the next record until exhaustion.
	for {
		record, ok, err := e.source.Next(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		if err := e.core.runSourceRecord(ctx, record.RecordID, record.Payload, record.Metadata); err != nil {
			return err
		}
	}
	return e.core.finish(ctx)
}

func (e *linearPullEngine[TSource, TSink]) Pause(_ context.Context) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPullEngine[TSource, TSink]) Resume(_ context.Context) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPullEngine[TSource, TSink]) Retry(_ context.Context, _ RecordID) error {
	return ErrBuilderUnsupportedLifecycleOperation
}

func (e *linearPullEngine[TSource, TSink]) Trace(_ context.Context, source RecordID) ([]Envelope[TSink], error) {
	return e.core.trace(source), nil
}

type linearPushEngine[TSource, TSink any] struct {
	source StreamSource[TSource]
	core   linearEngineCore[TSink]
}

func (e *linearPushEngine[TSource, TSink]) Run(ctx context.Context) error {
	e.core.reset()
	stream := e.source.Stream(ctx)
	// Push engines drain the source stream until it closes or the context is canceled.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case record, ok := <-stream:
			if !ok {
				return e.core.finish(ctx)
			}
			if err := e.core.runSourceRecord(ctx, record.RecordID, record.Payload, record.Metadata); err != nil {
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

func (e *linearPushEngine[TSource, TSink]) Trace(_ context.Context, source RecordID) ([]Envelope[TSink], error) {
	return e.core.trace(source), nil
}
