package golden_example

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/pierre/manifold/pipeline"
	"github.com/pierre/manifold/pipeline/golden_example/couplings"
)

// EnginePush is a temporary manual reference implementation kept while the
// framework-owned builder runtime expands beyond the initial linear path.
type EnginePush struct {
	source   PushSource
	segment1 Segment1
	segment2 Segment2
	coupling pipeline.Coupling
	sink     *Sink
}

func NewEnginePush(c pipeline.Coupling) *EnginePush {
	if c == nil {
		c = couplings.MessageToText{}
	}

	return &EnginePush{
		source:   PushSource{},
		segment1: Segment1{},
		segment2: Segment2{},
		coupling: c,
		sink:     &Sink{},
	}
}

func (e *EnginePush) Validate() ValidationResult {
	return ValidationResult{
		Source:   pipeline.ValidateStreamSource(e.source),
		Segment1: pipeline.ValidateSegment(e.segment1),
		Segment2: pipeline.ValidateSegment(e.segment2),
		Sink:     pipeline.ValidateSink(e.sink),
	}
}

func (e *EnginePush) Run(ctx context.Context) (RunResult, error) {
	result := RunResult{}

	srcCh := e.source.Stream(ctx)
	segOutCh := make(chan pipeline.Envelope[json.RawMessage], 1)
	errCh := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(segOutCh)

		for record := range srcCh {
			result.SourceEmitted = true

			sourceEnvelope := pipeline.Envelope[string]{
				OriginRecordID: record.RecordID,
				RecordID:       record.RecordID,
				AttemptID:      1,
				Payload:        record.Payload,
				Metadata:       record.Metadata,
			}

			result1, err := e.segment1.Process(noPauseProcessContext{Context: ctx}, pipeline.SegmentRecord[string]{
				RecordID: sourceEnvelope.OriginRecordID,
				Payload:  sourceEnvelope.Payload,
				Metadata: sourceEnvelope.Metadata,
			}, func(out pipeline.SegmentRecord[json.RawMessage]) error {
				segment1OutID := pipeline.RecordID(string(sourceEnvelope.RecordID) + "/segment1")
				segOutCh <- pipeline.Envelope[json.RawMessage]{
					OriginRecordID: out.RecordID,
					RecordID:       segment1OutID,
					AttemptID:      sourceEnvelope.AttemptID,
					ParentIDs:      []pipeline.RecordID{sourceEnvelope.RecordID},
					SegmentPath:    []pipeline.SegmentID{"segment1"},
					Payload:        out.Payload,
					Metadata:       out.Metadata,
				}
				return nil
			})
			if err != nil {
				errCh <- err
				return
			}
			if result1.Status != pipeline.ProcessCompleted {
				errCh <- context.Canceled
				return
			}
		}
	}()

	go func() {
		defer wg.Done()

		for in := range segOutCh {
			coupledJSON, err := pipeline.ApplyCoupling(e.coupling, in.Payload)
			if err != nil {
				errCh <- err
				return
			}
			result.CouplingApplied = true

			var segment2Input Segment2Input
			if err := json.Unmarshal(coupledJSON, &segment2Input); err != nil {
				errCh <- err
				return
			}

			var result2 pipeline.ProcessResult
			result2, err = e.segment2.Process(noPauseProcessContext{Context: ctx}, pipeline.SegmentRecord[Segment2Input]{
				RecordID: in.OriginRecordID,
				Payload:  segment2Input,
				Metadata: in.Metadata,
			}, func(out pipeline.SegmentRecord[string]) error {
				sinkOutID := pipeline.RecordID(string(in.RecordID) + "/segment2")
				return e.sink.Consume(ctx, pipeline.Envelope[string]{
					OriginRecordID: out.RecordID,
					RecordID:       sinkOutID,
					AttemptID:      in.AttemptID,
					ParentIDs:      []pipeline.RecordID{in.RecordID},
					SegmentPath:    []pipeline.SegmentID{"segment1", "segment2"},
					Payload:        out.Payload,
					Metadata:       out.Metadata,
				})
			})
			if err != nil {
				errCh <- err
				return
			}
			if result2.Status != pipeline.ProcessCompleted {
				errCh <- context.Canceled
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return result, err
		}
	}

	if err := e.segment1.Done(ctx); err != nil {
		return result, err
	}
	if err := e.segment2.Done(ctx); err != nil {
		return result, err
	}
	if err := e.sink.Done(ctx); err != nil {
		return result, err
	}

	if len(e.sink.items) > 0 {
		result.SinkOutput = e.sink.items[0].Payload
		result.OriginRecordID = e.sink.items[0].OriginRecordID
		result.SegmentPath = e.sink.items[0].SegmentPath
	}
	result.SourceCompleted = true

	return result, nil
}
