// Package golden_example contains the canonical end-user example for the pipeline package.
//
// The current pull and push engines in this package are temporary reference implementations
// that manually wire source, segments, coupling, lineage, and sink behavior.
//
// The intended end-user shape is a framework-provided builder so users configure topology
// instead of implementing their own engines. The target fluent API should read roughly like:
//
//	pullEngine := pipeline.NewPullBuilder(&Source{}, &Sink{}).
//		Through(Segment1{}).
//		Via(couplings.MessageToText{}).
//		Through(Segment2{}).
//		Build()
//
//	pushEngine := pipeline.NewPushBuilder(PushSource{}, &Sink{}).
//		Through(Segment1{}).
//		Via(couplings.MessageToText{}).
//		Through(Segment2{}).
//		Build()
//
// Additional topology vocabulary under consideration:
//
// - Broadcast: duplicate one upstream record onto every downstream branch.
// - Parallelism: distribute records across N worker lanes, for example round-robin.
// - Partition: deterministically route each record to exactly one lane by policy or key.
package golden_example
