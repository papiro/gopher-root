package pipeline

import (
	"fmt"
	"reflect"
)

type runtimeStage struct {
	segment       runtimeSegment
	couplingAfter Coupling
}

type runtimePlan struct {
	stages []runtimeStage
}

func compileLinearPlan(ops []builderOp) (runtimePlan, EngineConfig, error) {
	if len(ops) == 0 {
		return runtimePlan{}, EngineConfig{}, ErrBuilderNoSegments
	}

	segments := make([]runtimeSegment, 0, len(ops))
	couplings := make([]Coupling, 0, len(ops))

	for _, op := range ops {
		switch op.kind {
		case builderOpBroadcast:
			return runtimePlan{}, EngineConfig{}, fmt.Errorf("%w: Broadcast", ErrBuilderTopologyModifierUnsupported)
		case builderOpParallelism:
			return runtimePlan{}, EngineConfig{}, fmt.Errorf("%w: Parallelism(%d)", ErrBuilderTopologyModifierUnsupported, op.lanes)
		case builderOpPartition:
			return runtimePlan{}, EngineConfig{}, fmt.Errorf("%w: Partition(%d)", ErrBuilderTopologyModifierUnsupported, op.lanes)
		case builderOpSegment:
			segment, err := newRuntimeSegment(op.segment)
			if err != nil {
				return runtimePlan{}, EngineConfig{}, err
			}
			if err := validateRuntimeSegment(segment); err != nil {
				return runtimePlan{}, EngineConfig{}, err
			}
			if len(segments) > 0 && len(couplings) == len(segments)-1 {
				implicit, err := implicitCouplingBetween(segments[len(segments)-1], segment)
				if err != nil {
					return runtimePlan{}, EngineConfig{}, err
				}
				couplings = append(couplings, implicit)
			}
			segments = append(segments, segment)
		case builderOpCoupling:
			if len(segments) == 0 || len(couplings) >= len(segments) {
				return runtimePlan{}, EngineConfig{}, ErrBuilderSegmentExpected
			}
			couplings = append(couplings, op.coupler)
		default:
			return runtimePlan{}, EngineConfig{}, fmt.Errorf("unknown builder op kind %q", op.kind)
		}
	}

	if len(segments) == 0 {
		return runtimePlan{}, EngineConfig{}, ErrBuilderNoSegments
	}
	if len(couplings) >= len(segments) {
		return runtimePlan{}, EngineConfig{}, ErrBuilderSegmentExpected
	}
	if len(segments) > 1 && len(couplings) != len(segments)-1 {
		return runtimePlan{}, EngineConfig{}, ErrBuilderCouplingExpected
	}
	if len(segments) == 1 && len(couplings) > 0 {
		return runtimePlan{}, EngineConfig{}, ErrBuilderCouplingExpected
	}

	plan := runtimePlan{
		stages: make([]runtimeStage, len(segments)),
	}
	segmentDescriptors := make([]SegmentDescriptor, len(segments))
	couplingDescriptors := make([]CouplingDescriptor, 0, len(couplings))
	connections := make([]Connection, 0, len(couplings))

	for idx := range segments {
		plan.stages[idx].segment = segments[idx]
		segmentDescriptors[idx] = segments[idx].desc
		if idx < len(couplings) {
			plan.stages[idx].couplingAfter = couplings[idx]

			couplingID := syntheticCouplingID(idx, couplings[idx])
			couplingDescriptors = append(couplingDescriptors, CouplingDescriptor{
				ID:          couplingID,
				FromSegment: segments[idx].desc.ID,
				ToSegment:   segments[idx+1].desc.ID,
			})
			connections = append(connections, Connection{
				From:       segments[idx].desc.ID,
				To:         segments[idx+1].desc.ID,
				CouplingID: couplingID,
			})
		}
	}

	return plan, EngineConfig{
		Segments:  segmentDescriptors,
		Couplings: couplingDescriptors,
		Topology: Topology{
			Connections: connections,
		},
	}, nil
}

func implicitCouplingBetween(from, to runtimeSegment) (Coupling, error) {
	fromPayloadType := from.outputPayloadType()
	toPayloadType := to.inputPayloadType()
	if fromPayloadType != toPayloadType {
		return nil, fmt.Errorf(
			"%w: %q outputs %s but %q expects %s",
			ErrBuilderImplicitCouplingRequired,
			from.desc.ID,
			reflectTypeString(fromPayloadType),
			to.desc.ID,
			reflectTypeString(toPayloadType),
		)
	}
	return identityCoupling{}, nil
}

func reflectTypeString(t reflect.Type) string {
	if t == nil {
		return "<nil>"
	}
	return t.String()
}
