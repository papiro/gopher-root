package pipeline_test

import (
	"context"
	"errors"
	"testing"

	"github.com/pierre/manifold/pipeline"
	"github.com/pierre/manifold/pipeline/golden_example"
	"github.com/pierre/manifold/pipeline/golden_example/couplings"
)

type stringPassthroughSegment struct {
	id pipeline.SegmentID
}

func (s stringPassthroughSegment) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:                   s.id,
		Idempotency:          pipeline.Idempotent,
		CompatibilityVersion: "v1",
	}
}

func (s stringPassthroughSegment) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentInput[string],
	out func(pipeline.SegmentOutput[string]) error,
) (pipeline.ProcessResult, error) {
	if err := out(pipeline.SegmentOutput[string]{
		Payload:  in.Payload,
		Metadata: in.Metadata,
	}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (stringPassthroughSegment) Done(context.Context) error { return nil }

type intSinkSegment struct {
	id pipeline.SegmentID
}

func (s intSinkSegment) Descriptor() pipeline.SegmentDescriptor {
	return pipeline.SegmentDescriptor{
		ID:                   s.id,
		Idempotency:          pipeline.Idempotent,
		CompatibilityVersion: "v1",
	}
}

func (s intSinkSegment) Process(
	_ pipeline.ProcessContext,
	in pipeline.SegmentInput[int],
	out func(pipeline.SegmentOutput[int]) error,
) (pipeline.ProcessResult, error) {
	if err := out(pipeline.SegmentOutput[int]{
		Payload:  in.Payload,
		Metadata: in.Metadata,
	}); err != nil {
		return pipeline.ProcessResult{}, err
	}
	return pipeline.ProcessResult{Status: pipeline.ProcessCompleted}, nil
}

func (intSinkSegment) Done(context.Context) error { return nil }

func TestBuilderContractLinearGoldenExample(t *testing.T) {
	t.Parallel()

	plan, err := pipeline.NewBuilder().
		Through(golden_example.Segment1{}).
		Via(couplings.MessageToText{}).
		Through(golden_example.Segment2{}).
		Build()
	if err != nil {
		t.Fatalf("plan build failed: %v", err)
	}

	runtime := pipeline.NewInMemoryRuntime()
	engine, err := pipeline.NewPullEngine(&golden_example.Source{}, &golden_example.Sink{}, plan, runtime)
	if err != nil {
		t.Fatalf("engine build failed: %v", err)
	}

	if err := engine.Run(context.Background()); err != nil {
		t.Fatalf("run failed: %v", err)
	}

	trace, err := engine.Trace(context.Background(), "rec-1")
	if err != nil {
		t.Fatalf("trace failed: %v", err)
	}
	if len(trace) != 1 {
		t.Fatalf("expected one terminal output, got %d", len(trace))
	}
	if trace[0].Payload != "hello world" {
		t.Fatalf("unexpected sink payload: %q", trace[0].Payload)
	}
	if trace[0].OriginRecordID != "rec-1" {
		t.Fatalf("unexpected origin record ID: %q", trace[0].OriginRecordID)
	}
	if len(trace[0].SegmentPath) != 2 || trace[0].SegmentPath[0] != "segment1" || trace[0].SegmentPath[1] != "segment2" {
		t.Fatalf("unexpected segment path: %v", trace[0].SegmentPath)
	}
	if len(trace[0].ParentIDs) != 1 || trace[0].ParentIDs[0] != "rec-1/segment1" {
		t.Fatalf("unexpected parent IDs: %v", trace[0].ParentIDs)
	}
}

func TestBuilderContractPartitionRequiresPolicy(t *testing.T) {
	t.Parallel()

	_, err := pipeline.NewBuilder().
		Through(golden_example.Segment1{}).
		Partition(4, nil).
		Build()
	if !errors.Is(err, pipeline.ErrBuilderPartitionPolicyRequired) {
		t.Fatalf("expected ErrBuilderPartitionPolicyRequired, got %v", err)
	}
}

func TestBuilderContractImplicitIdentityCouplingForExactAdjacentTypes(t *testing.T) {
	t.Parallel()

	plan, err := pipeline.NewBuilder().
		Through(stringPassthroughSegment{id: "segment-a"}).
		Through(stringPassthroughSegment{id: "segment-b"}).
		Build()
	if err != nil {
		t.Fatalf("expected implicit identity coupling build to succeed, got %v", err)
	}

	cfg := plan.Config()
	if len(cfg.Couplings) != 1 {
		t.Fatalf("expected one synthetic coupling, got %d", len(cfg.Couplings))
	}
	if cfg.Couplings[0].FromSegment != "segment-a" || cfg.Couplings[0].ToSegment != "segment-b" {
		t.Fatalf("unexpected synthetic coupling descriptor: %+v", cfg.Couplings[0])
	}
	if len(cfg.Topology.Connections) != 1 {
		t.Fatalf("expected one topology connection, got %d", len(cfg.Topology.Connections))
	}
	if cfg.Topology.Connections[0].CouplingID == "" {
		t.Fatal("expected synthetic topology connection to reference a coupling ID")
	}
}

func TestBuilderContractAdjacentTypeMismatchRequiresExplicitCoupling(t *testing.T) {
	t.Parallel()

	_, err := pipeline.NewBuilder().
		Through(stringPassthroughSegment{id: "segment-a"}).
		Through(intSinkSegment{id: "segment-b"}).
		Build()
	if !errors.Is(err, pipeline.ErrBuilderImplicitCouplingRequired) {
		t.Fatalf("expected ErrBuilderImplicitCouplingRequired, got %v", err)
	}
}

func TestBuilderContractBroadcastDuplicatesOneRecordToEveryBranch(t *testing.T) {
	t.Parallel()
	t.Skip("enable when Broadcast is implemented")

	// Intended usage:
	//
	//	plan := pipeline.NewBuilder().
	//		Through(Segment1{}).
	//		Broadcast().
	//		Via(couplings.MessageToText{}).
	//		Through(LeftBranch{}).
	//		Via(couplings.MessageToTaggedText{}).
	//		Through(RightBranch{}).
	//		Build()
	//	engine, err := pipeline.NewPullEngine(source, sink, plan)
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
	//	plan := pipeline.NewBuilder().
	//		Through(Segment1{}).
	//		Parallelism(4).
	//		Via(couplings.MessageToText{}).
	//		Through(WorkerSegment{}).
	//		Build()
	//	roundRobin := pipeline.RoundRobin()
	//	_ = roundRobin // intended routing policy once Parallelism accepts explicit policies
	//	engine, err := pipeline.NewPullEngine(source, sink, plan)
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
	//	plan := pipeline.NewBuilder().
	//		Through(Segment1{}).
	//		Partition(4, pipeline.ByRecordID()).
	//		Via(couplings.MessageToText{}).
	//		Through(WorkerSegment{}).
	//		Build()
	//
	//	plan := pipeline.NewBuilder().
	//		Through(Segment1{}).
	//		Partition(4, pipeline.ByMetadataKey("customer_id")).
	//		Via(couplings.MessageToText{}).
	//		Through(WorkerSegment{}).
	//		Build()
	//
	//	plan := pipeline.NewBuilder().
	//		Through(Segment1{}).
	//		Partition(4, pipeline.ByKey(func(in SomeInput) string {
	//			return in.CustomerID
	//		})).
	//		Via(couplings.MessageToText{}).
	//		Through(WorkerSegment{}).
	//		Build()
	//	engine, err := pipeline.NewPullEngine(source, sink, plan)
	//
	// Contract to enforce:
	// - each input record is routed to exactly one partition
	// - records with the same partition key always route to the same lane
	// - routing is deterministic across retries and resume boundaries
	// - partitioning changes placement, not record lineage semantics
}
