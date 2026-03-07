package pipeline_test

import "testing"

func TestBuilderContractLinearGoldenExample(t *testing.T) {
	t.Parallel()
	t.Skip("enable when the builder API exists")

	// Intended golden-path usage:
	//
	//	engine := pipeline.NewPullBuilder(&golden_example.Source{}, &golden_example.Sink{}).
	//		Through(golden_example.Segment1{}).
	//		Via(couplings.MessageToText{}).
	//		Through(golden_example.Segment2{}).
	//		Build()
	//
	// Contract to enforce:
	// - the builder owns topology, coupling placement, and runtime orchestration
	// - users provide typed source, segments, coupling, and sink implementations
	// - users do not manually marshal or unmarshal between segments
	// - the built engine preserves origin record identity and segment path in sink output
}

func TestBuilderContractBroadcastDuplicatesOneRecordToEveryBranch(t *testing.T) {
	t.Parallel()
	t.Skip("enable when Broadcast is implemented")

	// Intended usage:
	//
	//	engine := pipeline.NewPullBuilder(source, sink).
	//		Through(Segment1{}).
	//		Broadcast().
	//		Via(couplings.MessageToText{}).
	//		Through(LeftBranch{}).
	//		Via(couplings.MessageToTaggedText{}).
	//		Through(RightBranch{}).
	//		Build()
	//
	// Contract to enforce:
	// - one upstream record is delivered to every downstream branch
	// - every child output preserves the same OriginRecordID
	// - every child output records the broadcast parent in ParentIDs
	// - output cardinality scales with number of downstream branches
}

func TestBuilderContractParallelismDistributesRecordsAcrossWorkers(t *testing.T) {
	t.Parallel()
	t.Skip("enable when Parallelism is implemented")

	// Intended usage:
	//
	//	engine := pipeline.NewPullBuilder(source, sink).
	//		Through(Segment1{}).
	//		Parallelism(4).
	//		Via(couplings.MessageToText{}).
	//		Through(WorkerSegment{}).
	//		Build()
	//
	// Contract to enforce:
	// - each input record is routed to exactly one worker lane
	// - no record is duplicated purely because parallelism increased
	// - total output cardinality matches input cardinality absent segment fan-out
	// - default routing policy is explicit in docs and stable in tests
}

func TestBuilderContractPartitionRoutesDeterministicallyByPolicy(t *testing.T) {
	t.Parallel()
	t.Skip("enable when Partition is implemented")

	// Intended usage:
	//
	//	engine := pipeline.NewPullBuilder(source, sink).
	//		Through(Segment1{}).
	//		Partition(4, pipeline.ByKey(func(in SomeInput) string {
	//			return in.CustomerID
	//		})).
	//		Via(couplings.MessageToText{}).
	//		Through(WorkerSegment{}).
	//		Build()
	//
	// Contract to enforce:
	// - each input record is routed to exactly one partition
	// - records with the same partition key always route to the same lane
	// - routing is deterministic across retries and resume boundaries
	// - partitioning changes placement, not record lineage semantics
}
