package golden_example

import (
	"context"
	"fmt"

	"github.com/pierre/gopher-root/pipeline"
	"github.com/pierre/gopher-root/pipeline/golden_example/couplings"
)

func Example_engine_push() {
	source := PushSource{}
	sink := &Sink{}
	plan, buildErr := pipeline.NewBuilder().
		Through(Segment1{}).
		Via(couplings.MessageToText{}).
		Through(Segment2{}).
		Build()
	fmt.Println("build valid:", buildErr == nil)
	if buildErr != nil {
		return
	}
	engine, engineErr := pipeline.NewPushEngine(source, sink, plan)
	fmt.Println("engine valid:", engineErr == nil)
	if engineErr != nil {
		return
	}

	runErr := engine.Run(context.Background())
	trace, traceErr := engine.Trace(context.Background(), "rec-1")
	fmt.Println("source emitted:", runErr == nil && len(sink.items) == 1)
	fmt.Println("trace available:", traceErr == nil && len(trace) == 1)
	if len(trace) > 0 {
		fmt.Println("sink output:", trace[0].Payload)
		fmt.Println("origin record:", trace[0].OriginRecordID)
		fmt.Println("segment path:", trace[0].SegmentPath)
	}
	fmt.Println("source completed:", runErr == nil)

	// Output:
	// build valid: true
	// engine valid: true
	// source emitted: true
	// trace available: true
	// sink output: hello world
	// origin record: rec-1
	// segment path: [segment1 segment2]
	// source completed: true
}
