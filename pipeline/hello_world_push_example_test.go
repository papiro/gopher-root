package pipeline_test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pierre/gopher-root/pipeline"
)

type pushInput struct {
	Text string `json:"text"`
}

type pushSource struct{}

func (pushSource) Stream(ctx context.Context) <-chan pipeline.SourceRecord[string] {
	ch := make(chan pipeline.SourceRecord[string], 1)
	go func() {
		defer close(ch)
		select {
		case <-ctx.Done():
			return
		case ch <- pipeline.SourceRecord[string]{
			RecordID: "rec-1",
			Payload:  "hello world",
		}:
		}
	}()
	return ch
}

type sourceSegment struct{}

func (sourceSegment) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:          "source",
		Idempotency: pipeline.Idempotent,
		Version:     "v1",
	}
}

func (sourceSegment) Process(
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

func (sourceSegment) Flush(context.Context) error              { return nil }
func (sourceSegment) Done(context.Context) error               { return nil }
func (sourceSegment) Snapshot(context.Context) ([]byte, error) { return nil, nil }
func (sourceSegment) Restore(context.Context, []byte) error    { return nil }
func (sourceSegment) Compensator() pipeline.Compensator        { return nil }

type sinkSegment struct{}

func (sinkSegment) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:          "sink",
		Idempotency: pipeline.Idempotent,
		Version:     "v1",
	}
}

func (sinkSegment) Process(
	_ context.Context,
	in pipeline.Envelope[pushInput],
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

func (sinkSegment) Flush(context.Context) error              { return nil }
func (sinkSegment) Done(context.Context) error               { return nil }
func (sinkSegment) Snapshot(context.Context) ([]byte, error) { return nil, nil }
func (sinkSegment) Restore(context.Context, []byte) error    { return nil }
func (sinkSegment) Compensator() pipeline.Compensator        { return nil }

type outputSink struct {
	items []pipeline.Envelope[string]
}

func (s *outputSink) Consume(_ context.Context, item pipeline.Envelope[string]) error {
	s.items = append(s.items, item)
	return nil
}

func (s *outputSink) Done(context.Context) error { return nil }

func Example_userHelloWorldPush() {
	ctx := context.Background()

	source := pushSource{}
	sourceSeg := sourceSegment{}
	sinkSeg := sinkSegment{}
	coupling := messageToTextCoupling{}
	sink := &outputSink{}

	sourceContractErr := pipeline.ValidateSegment[string, json.RawMessage](sourceSeg)
	sinkContractErr := pipeline.ValidateSegment[pushInput, string](sinkSeg)
	fmt.Println("source contract valid:", sourceContractErr == nil)
	fmt.Println("sink contract valid:", sinkContractErr == nil)

	srcCh := source.Stream(ctx)
	segOutCh := make(chan pipeline.Envelope[json.RawMessage], 1)
	errCh := make(chan error, 2)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer close(segOutCh)
		for record := range srcCh {
			in := pipeline.Envelope[string]{
				RecordID:  record.RecordID,
				AttemptID: 1,
				Payload:   record.Payload,
				Metadata:  record.Metadata,
			}
			if err := sourceSeg.Process(ctx, in, func(out pipeline.Envelope[json.RawMessage]) error {
				segOutCh <- out
				return nil
			}); err != nil {
				errCh <- err
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for in := range segOutCh {
			coupledJSON, err := pipeline.ApplyCoupling(coupling, in.Payload)
			if err != nil {
				errCh <- err
				return
			}
			var sinkPayload pushInput
			if err := json.Unmarshal(coupledJSON, &sinkPayload); err != nil {
				errCh <- err
				return
			}
			err = sinkSeg.Process(ctx, pipeline.Envelope[pushInput]{
				RecordID:    in.RecordID,
				AttemptID:   in.AttemptID,
				ParentIDs:   in.ParentIDs,
				SegmentPath: in.SegmentPath,
				Payload:     sinkPayload,
				Metadata:    in.Metadata,
			}, func(out pipeline.Envelope[string]) error {
				return sink.Consume(ctx, out)
			})
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)

	var runErr error
	for err := range errCh {
		if err != nil {
			runErr = err
			break
		}
	}

	_ = sourceSeg.Done(ctx)
	_ = sinkSeg.Done(ctx)
	_ = sink.Done(ctx)

	fmt.Println("push completed:", runErr == nil)
	fmt.Println("sink output:", sink.items[0].Payload)
	fmt.Println("segment path:", sink.items[0].SegmentPath)

	// Output:
	// source contract valid: true
	// sink contract valid: true
	// push completed: true
	// sink output: hello world
	// segment path: [source sink]
}
