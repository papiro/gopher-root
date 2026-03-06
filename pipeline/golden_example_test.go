package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/pierre/gopher-root/pipeline"
)

type input struct {
	Text string `json:"text"`
}

// --- Source interface setup ---
type sourceType struct {
	emitted bool
}

func (s *sourceType) Next(context.Context) (pipeline.SourceRecord[string], bool, error) {
	if s.emitted {
		return pipeline.SourceRecord[string]{}, false, nil
	}
	s.emitted = true
	return pipeline.SourceRecord[string]{
		RecordID: "rec-1",
		Payload:  "hello world",
	}, true, nil
}

// --- Segment interface setup: segment1 (string -> json.RawMessage) ---
type segment1Type struct{}

func (segment1Type) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:          "source",
		Idempotency: pipeline.Idempotent,
		Version:     "v1",
	}
}

func (segment1Type) Process(
	_ context.Context,
	in pipeline.Envelope[string],
	out func(pipeline.Envelope[json.RawMessage]) error,
) error {
	payload, err := json.Marshal(struct {
		Message string `json:"message"`
	}{
		Message: in.Payload,
	})
	if err != nil {
		return err
	}
	return out(pipeline.Envelope[json.RawMessage]{
		RecordID:    in.RecordID,
		AttemptID:   in.AttemptID,
		ParentIDs:   in.ParentIDs,
		SegmentPath: append(append([]pipeline.SegmentID{}, in.SegmentPath...), "source"),
		Payload:     payload,
		Metadata:    in.Metadata,
	})
}

func (segment1Type) Flush(context.Context) error              { return nil }
func (segment1Type) Done(context.Context) error               { return nil }
func (segment1Type) Snapshot(context.Context) ([]byte, error) { return nil, nil }
func (segment1Type) Restore(context.Context, []byte) error    { return nil }
func (segment1Type) Compensator() pipeline.Compensator        { return nil }

// --- Segment interface setup: segment2 (input -> string) ---
type segment2Type struct{}

func (segment2Type) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:          "sink",
		Idempotency: pipeline.Idempotent,
		Version:     "v1",
	}
}

func (segment2Type) Process(
	_ context.Context,
	in pipeline.Envelope[input],
	out func(pipeline.Envelope[string]) error,
) error {
	return out(pipeline.Envelope[string]{
		RecordID:    in.RecordID,
		AttemptID:   in.AttemptID,
		ParentIDs:   in.ParentIDs,
		SegmentPath: append(append([]pipeline.SegmentID{}, in.SegmentPath...), "sink"),
		Payload:     in.Payload.Text,
		Metadata:    in.Metadata,
	})
}

func (segment2Type) Flush(context.Context) error              { return nil }
func (segment2Type) Done(context.Context) error               { return nil }
func (segment2Type) Snapshot(context.Context) ([]byte, error) { return nil, nil }
func (segment2Type) Restore(context.Context, []byte) error    { return nil }
func (segment2Type) Compensator() pipeline.Compensator        { return nil }

// --- Sink interface setup ---
type sinkType struct {
	items []pipeline.Envelope[string]
}

func (c *sinkType) Consume(_ context.Context, item pipeline.Envelope[string]) error {
	c.items = append(c.items, item)
	return nil
}

func (c *sinkType) Done(context.Context) error { return nil }

func Example_golden() {
	ctx := context.Background()

	// --- Instantiate Source, Segments, Coupling, and Sink ---
	source := &sourceType{}
	segment1 := segment1Type{}
	segment2 := segment2Type{}
	coupling := messageToTextCoupling{}

	sourceContractErr := pipeline.ValidateSegment[string, json.RawMessage](segment1)
	sinkContractErr := pipeline.ValidateSegment[input, string](segment2)
	fmt.Println("source contract valid:", sourceContractErr == nil)
	fmt.Println("sink contract valid:", sinkContractErr == nil)

	// --- Source.Next (no upstream input) ---
	record, ok, nextErr := source.Next(ctx)
	fmt.Println("source emitted:", ok && nextErr == nil)

	// --- Engine assigns envelope metadata ---
	in := pipeline.Envelope[string]{
		RecordID:  record.RecordID,
		AttemptID: 1,
		Payload:   record.Payload,
		Metadata:  record.Metadata,
	}

	// --- Segment1.Process ---
	var sourceOut pipeline.Envelope[json.RawMessage]
	_ = segment1.Process(ctx, in, func(out pipeline.Envelope[json.RawMessage]) error {
		sourceOut = out
		return nil
	})

	// --- Coupling.Apply ---
	coupledJSON, couplingErr := pipeline.ApplyCoupling(coupling, sourceOut.Payload)
	fmt.Println("coupling applied:", couplingErr == nil)

	// --- Segment2 input decode ---
	var inputPayload input
	_ = json.Unmarshal(coupledJSON, &inputPayload)

	// --- Segment2.Process and Sink.Consume ---
	sink := &sinkType{}
	_ = segment2.Process(
		ctx,
		pipeline.Envelope[input]{
			RecordID:    sourceOut.RecordID,
			AttemptID:   sourceOut.AttemptID,
			ParentIDs:   sourceOut.ParentIDs,
			SegmentPath: sourceOut.SegmentPath,
			Payload:     inputPayload,
			Metadata:    sourceOut.Metadata,
		},
		func(out pipeline.Envelope[string]) error {
			return sink.Consume(ctx, out)
		},
	)
	_ = sink.Done(ctx)

	fmt.Println("sink output:", sink.items[0].Payload)
	fmt.Println("segment path:", sink.items[0].SegmentPath)

	// --- Source completion check ---
	_, done, doneErr := source.Next(ctx)
	fmt.Println("source completed:", !done && doneErr == nil)

	// Output:
	// source contract valid: true
	// sink contract valid: true
	// source emitted: true
	// coupling applied: true
	// sink output: hello world
	// segment path: [source sink]
	// source completed: true
}
