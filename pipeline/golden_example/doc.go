// Package golden_example contains the canonical end-user example for the pipeline package.
//
// It also illustrates the reference Go implementation of the manifold spec defined in
// spec/README.md and spec/manifold/v1/*.proto.
//
// The recommended end-user shape is a framework-provided builder so users configure topology
// instead of implementing their own engines. The minimal linear builder API now reads like:
//
//	plan := pipeline.NewBuilder().
//		Through(Segment1{}).
//		Via(couplings.MessageToText{}).
//		Through(Segment2{}).
//		Build()
//
//	runtime := pipeline.NewInMemoryRuntime()
//	pullEngine, err := pipeline.NewPullEngine(&Source{}, &Sink{}, plan, runtime, pipeline.WithDebug())
//
//	pushEngine, err := pipeline.NewPushEngine(PushSource{}, &Sink{}, plan, runtime, pipeline.WithDebug())
//
// Run resumes from the latest durable recovery boundary when one exists.
// Pause/resume restores work by inferring the next resumable lineage step from
// durable source records, segment outputs, segment commits, terminal outputs,
// and any in-flight segment snapshots. Restart is the explicit same-plan replay path.
//
// Future topology examples under this builder shape include:
//
//	parallelPlan := pipeline.NewBuilder().
//		Through(Segment1{}).
//		Parallelism(4).
//		Via(couplings.MessageToText{}).
//		Through(Segment2{}).
//		Build()
//
//	partitionByRecordID := pipeline.NewBuilder().
//		Through(Segment1{}).
//		Partition(4, pipeline.ByRecordID()).
//		Via(couplings.MessageToText{}).
//		Through(Segment2{}).
//		Build()
//
//	partitionByMetadata := pipeline.NewBuilder().
//		Through(Segment1{}).
//		Partition(4, pipeline.ByMetadataKey("customer_id")).
//		Via(couplings.MessageToText{}).
//		Through(Segment2{}).
//		Build()
//
//	partitionByKey := pipeline.NewBuilder().
//		Through(Segment1{}).
//		Partition(4, pipeline.ByKey(func(in SomeInput) string {
//			return in.CustomerID
//		})).
//		Via(couplings.MessageToText{}).
//		Through(Segment2{}).
//		Build()
//
//	roundRobin := pipeline.RoundRobin()
//	_ = roundRobin // intended for future worker distribution policies
//
// The older pull and push engine files in this package remain as reference implementations
// while the builder runtime grows beyond the initial linear path.
//
// Additional topology vocabulary reserved for follow-up implementation:
//
// - Broadcast: duplicate one upstream record onto every downstream branch.
// - Parallelism: distribute records across N worker lanes, for example round-robin.
// - Partition: deterministically route each record to exactly one lane by policy or key.
package golden_example
