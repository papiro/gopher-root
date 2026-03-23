package golden_example

import (
	"context"
	"encoding/json"

	"github.com/pierre/manifold/pipeline"
	"github.com/pierre/manifold/pipeline/golden_example/couplings"
)

// EnginePull is a temporary manual reference implementation kept while the
// framework-owned builder runtime expands beyond the initial linear path.
type EnginePull struct {
	source   *Source
	segment1 Segment1
	segment2 Segment2
	coupling pipeline.Coupling
	sink     *Sink
}

func NewEnginePull(c pipeline.Coupling) *EnginePull {
	if c == nil {
		c = couplings.MessageToText{}
	}

	return &EnginePull{
		source:   &Source{},
		segment1: Segment1{},
		segment2: Segment2{},
		coupling: c,
		sink:     &Sink{},
	}
}

func (e *EnginePull) Validate() ValidationResult {
	return ValidationResult{
		Source:   pipeline.ValidateSource(e.source),
		Segment1: pipeline.ValidateSegment(e.segment1),
		Segment2: pipeline.ValidateSegment(e.segment2),
		Sink:     pipeline.ValidateSink(e.sink),
	}
}

func (e *EnginePull) Run(ctx context.Context) (RunResult, error) {
	record, ok, nextErr := e.source.Next(ctx)
	if nextErr != nil {
		return RunResult{}, nextErr
	}

	result := RunResult{
		SourceEmitted: ok,
	}

	sourceEnvelope := pipeline.Envelope[string]{
		OriginRecordID: record.RecordID,
		RecordID:       record.RecordID,
		AttemptID:      1,
		Payload:        record.Payload,
		Metadata:       record.Metadata,
	}

	var segment1Output pipeline.SegmentOutput[json.RawMessage]
	processCtx := noPauseProcessContext{Context: ctx}

	result1, err := e.segment1.Process(processCtx, pipeline.SegmentInput[string]{
		SourceRecordID: sourceEnvelope.OriginRecordID,
		Payload:        sourceEnvelope.Payload,
		Metadata:       sourceEnvelope.Metadata,
	}, func(out pipeline.SegmentOutput[json.RawMessage]) error {
		segment1Output = out
		return nil
	})
	if err != nil {
		return result, err
	}
	if result1.Status != pipeline.ProcessCompleted {
		return result, context.Canceled
	}

	segment1OutID := pipeline.RecordID(string(sourceEnvelope.RecordID) + "/segment1")
	segment1Out := pipeline.Envelope[json.RawMessage]{
		OriginRecordID: sourceEnvelope.OriginRecordID,
		RecordID:       segment1OutID,
		AttemptID:      sourceEnvelope.AttemptID,
		ParentIDs:      []pipeline.RecordID{sourceEnvelope.RecordID},
		SegmentPath:    []pipeline.SegmentID{"segment1"},
		Payload:        segment1Output.Payload,
		Metadata:       segment1Output.Metadata,
	}

	coupledJSON, err := pipeline.ApplyCoupling(e.coupling, segment1Out.Payload)
	if err != nil {
		return result, err
	}
	result.CouplingApplied = true

	var segment2Input Segment2Input
	if err := json.Unmarshal(coupledJSON, &segment2Input); err != nil {
		return result, err
	}

	result2, err := e.segment2.Process(
		processCtx,
		pipeline.SegmentInput[Segment2Input]{
			SourceRecordID: segment1Out.OriginRecordID,
			Payload:        segment2Input,
			Metadata:       segment1Out.Metadata,
		},
		func(out pipeline.SegmentOutput[string]) error {
			sinkOutID := pipeline.RecordID(string(segment1Out.RecordID) + "/segment2")
			return e.sink.Consume(ctx, pipeline.Envelope[string]{
				OriginRecordID: segment1Out.OriginRecordID,
				RecordID:       sinkOutID,
				AttemptID:      segment1Out.AttemptID,
				ParentIDs:      []pipeline.RecordID{segment1Out.RecordID},
				SegmentPath:    []pipeline.SegmentID{"segment1", "segment2"},
				Payload:        out.Payload,
				Metadata:       out.Metadata,
			})
		},
	)
	if err != nil {
		return result, err
	}
	if result2.Status != pipeline.ProcessCompleted {
		return result, context.Canceled
	}

	if err := e.sink.Done(ctx); err != nil {
		return result, err
	}

	if len(e.sink.items) > 0 {
		result.SinkOutput = e.sink.items[0].Payload
		result.OriginRecordID = e.sink.items[0].OriginRecordID
		result.SegmentPath = e.sink.items[0].SegmentPath
	}

	_, done, doneErr := e.source.Next(ctx)
	if doneErr != nil {
		return result, doneErr
	}
	result.SourceCompleted = !done

	return result, nil
}
