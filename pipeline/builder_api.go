package pipeline

import "errors"

var (
	ErrBuilderSegmentRequired               = errors.New("builder segment is required")
	ErrBuilderCouplingRequired              = errors.New("builder coupling is required")
	ErrBuilderSegmentExpected               = errors.New("builder expected a segment at this position")
	ErrBuilderCouplingExpected              = errors.New("builder expected a coupling at this position")
	ErrBuilderNoSegments                    = errors.New("builder requires at least one segment")
	ErrBuilderPartitionPolicyRequired       = errors.New("builder partition policy is required")
	ErrBuilderTopologyModifierUnsupported   = errors.New("builder topology modifier is not implemented yet")
	ErrBuilderParallelismPositive           = errors.New("builder parallelism must be greater than zero")
	ErrBuilderPartitionCountPositive        = errors.New("builder partition count must be greater than zero")
	ErrBuilderUnsupportedSegmentShape       = errors.New("builder segment does not expose a supported Process signature")
	ErrBuilderSegmentRecordIDMismatch       = errors.New("segment output record ID must preserve source identity")
	ErrBuilderUnsupportedLifecycleOperation = errors.New("engine lifecycle operation is not implemented yet")
)

type builderOpKind string

const (
	builderOpSegment     builderOpKind = "segment"
	builderOpCoupling    builderOpKind = "coupling"
	builderOpBroadcast   builderOpKind = "broadcast"
	builderOpParallelism builderOpKind = "parallelism"
	builderOpPartition   builderOpKind = "partition"
)

type builderOp struct {
	kind    builderOpKind
	segment any
	coupler Coupling
	lanes   int
	policy  PartitionPolicy
}

type Builder struct {
	ops []builderOp
	err error
}

type Plan struct {
	runtime runtimePlan
	config  EngineConfig
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Through(segment any) *Builder {
	if b.err != nil {
		return b
	}
	if isNilBuilderValue(segment) {
		b.err = ErrBuilderSegmentRequired
		return b
	}
	b.ops = append(b.ops, builderOp{
		kind:    builderOpSegment,
		segment: segment,
	})
	return b
}

func (b *Builder) Via(coupling Coupling) *Builder {
	if b.err != nil {
		return b
	}
	if isNilBuilderValue(coupling) {
		b.err = ErrBuilderCouplingRequired
		return b
	}
	b.ops = append(b.ops, builderOp{
		kind:    builderOpCoupling,
		coupler: coupling,
	})
	return b
}

func (b *Builder) Broadcast() *Builder {
	if b.err != nil {
		return b
	}
	b.ops = append(b.ops, builderOp{kind: builderOpBroadcast})
	return b
}

func (b *Builder) Parallelism(workers int) *Builder {
	if b.err != nil {
		return b
	}
	if workers < 1 {
		b.err = ErrBuilderParallelismPositive
		return b
	}
	b.ops = append(b.ops, builderOp{
		kind:  builderOpParallelism,
		lanes: workers,
	})
	return b
}

func (b *Builder) Partition(partitions int, policy PartitionPolicy) *Builder {
	if b.err != nil {
		return b
	}
	if partitions < 1 {
		b.err = ErrBuilderPartitionCountPositive
		return b
	}
	if isNilBuilderValue(policy) {
		b.err = ErrBuilderPartitionPolicyRequired
		return b
	}
	b.ops = append(b.ops, builderOp{
		kind:   builderOpPartition,
		lanes:  partitions,
		policy: policy,
	})
	return b
}

func (b *Builder) Build() (*Plan, error) {
	if b.err != nil {
		return nil, b.err
	}

	plan, cfg, err := compileLinearPlan(b.ops)
	if err != nil {
		return nil, err
	}
	if err := ValidateTopology(cfg); err != nil {
		return nil, err
	}

	return &Plan{
		runtime: plan,
		config:  cfg,
	}, nil
}

func (p *Plan) Config() EngineConfig {
	return p.config
}

func NewPullEngine[TSource, TSink any](source Source[TSource], sink Sink[TSink], plan *Plan, runtime Runtime, opts ...EngineOption) (Engine[TSource, TSink], error) {
	if plan == nil {
		return nil, ErrBuilderNoSegments
	}
	if err := ValidateSource(source); err != nil {
		return nil, err
	}
	if err := ValidateSink(sink); err != nil {
		return nil, err
	}
	if err := ValidateRuntime(runtime); err != nil {
		return nil, err
	}
	pipelineID, err := planPipelineID(plan)
	if err != nil {
		return nil, err
	}
	options := collectEngineOptions(opts)

	engine := &linearPullEngine[TSource, TSink]{
		source: source,
		core: newLinearEngineCore(
			plan.runtime,
			sink,
			asSinkWithDone(sink),
			runtime,
			pipelineID,
			options,
		),
	}
	return engine, nil
}

func NewPushEngine[TSource, TSink any](source StreamSource[TSource], sink Sink[TSink], plan *Plan, runtime Runtime, opts ...EngineOption) (Engine[TSource, TSink], error) {
	if plan == nil {
		return nil, ErrBuilderNoSegments
	}
	if err := ValidateStreamSource(source); err != nil {
		return nil, err
	}
	if err := ValidateSink(sink); err != nil {
		return nil, err
	}
	if err := ValidateRuntime(runtime); err != nil {
		return nil, err
	}
	pipelineID, err := planPipelineID(plan)
	if err != nil {
		return nil, err
	}
	options := collectEngineOptions(opts)

	engine := &linearPushEngine[TSource, TSink]{
		source: source,
		core: newLinearEngineCore(
			plan.runtime,
			sink,
			asSinkWithDone(sink),
			runtime,
			pipelineID,
			options,
		),
	}
	return engine, nil
}
