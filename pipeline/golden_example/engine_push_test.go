package golden_example

import (
	"context"
	"fmt"

	"github.com/pierre/gopher-root/pipeline/golden_example/couplings"
)

func Example_engine_push() {
	// Target end-user shape once the framework owns orchestration:
	//
	//	engine := pipeline.NewPushBuilder(PushSource{}, &Sink{}).
	//		Through(Segment1{}).
	//		Via(couplings.MessageToText{}).
	//		Through(Segment2{}).
	//		Build()
	//
	// The current example still uses a hand-wired reference engine while the builder API
	// is being designed and implemented.
	engine := NewEnginePush(couplings.MessageToText{})

	validation := engine.Validate()
	fmt.Println("source contract valid:", validation.Source == nil)
	fmt.Println("segment1 contract valid:", validation.Segment1 == nil)
	fmt.Println("segment2 contract valid:", validation.Segment2 == nil)
	fmt.Println("sink contract valid:", validation.Sink == nil)

	result, runErr := engine.Run(context.Background())
	fmt.Println("source emitted:", runErr == nil && result.SourceEmitted)
	fmt.Println("coupling applied:", runErr == nil && result.CouplingApplied)
	fmt.Println("sink output:", result.SinkOutput)
	fmt.Println("origin record:", result.OriginRecordID)
	fmt.Println("segment path:", result.SegmentPath)
	fmt.Println("source completed:", runErr == nil && result.SourceCompleted)

	// Output:
	// source contract valid: true
	// segment1 contract valid: true
	// segment2 contract valid: true
	// sink contract valid: true
	// source emitted: true
	// coupling applied: true
	// sink output: hello world
	// origin record: rec-1
	// segment path: [segment1 segment2]
	// source completed: true
}
