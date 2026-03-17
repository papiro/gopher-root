package interfaces

import (
	"errors"
	"fmt"

	pipelinetypes "github.com/pierre/manifold/pipeline/types"
)

var (
	// ErrEngineConfigSegmentIDRequired indicates a segment descriptor is missing required ID.
	ErrEngineConfigSegmentIDRequired = errors.New("engine config segment ID is required")
	// ErrEngineConfigDuplicateSegmentID indicates two segment descriptors share one ID.
	ErrEngineConfigDuplicateSegmentID = errors.New("engine config contains duplicate segment ID")
	// ErrEngineConfigCouplingIDRequired indicates a coupling descriptor is missing required ID.
	ErrEngineConfigCouplingIDRequired = errors.New("engine config coupling ID is required")
	// ErrEngineConfigDuplicateCouplingID indicates two coupling descriptors share one ID.
	ErrEngineConfigDuplicateCouplingID = errors.New("engine config contains duplicate coupling ID")
	// ErrTopologyConnectionFromRequired indicates one connection omitted upstream segment.
	ErrTopologyConnectionFromRequired = errors.New("topology connection from segment is required")
	// ErrTopologyConnectionToRequired indicates one connection omitted downstream segment.
	ErrTopologyConnectionToRequired = errors.New("topology connection to segment is required")
	// ErrTopologyConnectionCouplingRequired indicates one connection omitted coupling identity.
	ErrTopologyConnectionCouplingRequired = errors.New("topology connection coupling ID is required")
	// ErrTopologyUnknownSegment indicates one connection references a segment not in engine config.
	ErrTopologyUnknownSegment = errors.New("topology references unknown segment")
	// ErrTopologyUnknownCoupling indicates one connection references a coupling not in engine config.
	ErrTopologyUnknownCoupling = errors.New("topology references unknown coupling")
	// ErrTopologyCouplingSegmentMismatch indicates connection endpoints violate coupling compatibility hints.
	ErrTopologyCouplingSegmentMismatch = errors.New("topology connection does not satisfy coupling segment compatibility")
	// ErrTopologyAmbiguousOrder indicates one segment fans out to multiple downstream segments when disallowed.
	ErrTopologyAmbiguousOrder = errors.New("topology has ambiguous segment ordering")
	// ErrTopologyCycle indicates segment ordering is cyclic.
	ErrTopologyCycle = errors.New("topology contains a cycle")
)

// ValidateTopology enforces engine-owned ordering and coupling assignment constraints.
func ValidateTopology(cfg pipelinetypes.EngineConfig) error {
	segmentSet := map[pipelinetypes.SegmentID]struct{}{}
	for _, seg := range cfg.Segments {
		if seg.ID == "" {
			return ErrEngineConfigSegmentIDRequired
		}
		if _, exists := segmentSet[seg.ID]; exists {
			return fmt.Errorf("%w: %q", ErrEngineConfigDuplicateSegmentID, seg.ID)
		}
		segmentSet[seg.ID] = struct{}{}
	}

	couplingSet := map[pipelinetypes.CouplingID]pipelinetypes.CouplingDescriptor{}
	for _, coupling := range cfg.Couplings {
		if coupling.ID == "" {
			return ErrEngineConfigCouplingIDRequired
		}
		if _, exists := couplingSet[coupling.ID]; exists {
			return fmt.Errorf("%w: %q", ErrEngineConfigDuplicateCouplingID, coupling.ID)
		}
		couplingSet[coupling.ID] = coupling
	}

	outDegree := map[pipelinetypes.SegmentID]int{}
	inDegree := map[pipelinetypes.SegmentID]int{}
	adjacency := map[pipelinetypes.SegmentID][]pipelinetypes.SegmentID{}
	for segID := range segmentSet {
		outDegree[segID] = 0
		inDegree[segID] = 0
		adjacency[segID] = nil
	}

	for idx, conn := range cfg.Topology.Connections {
		if conn.From == "" {
			return fmt.Errorf("%w: connection_index=%d", ErrTopologyConnectionFromRequired, idx)
		}
		if conn.To == "" {
			return fmt.Errorf("%w: connection_index=%d", ErrTopologyConnectionToRequired, idx)
		}
		if conn.CouplingID == "" {
			return fmt.Errorf("%w: connection_index=%d", ErrTopologyConnectionCouplingRequired, idx)
		}
		if _, ok := segmentSet[conn.From]; !ok {
			return fmt.Errorf("%w: %q", ErrTopologyUnknownSegment, conn.From)
		}
		if _, ok := segmentSet[conn.To]; !ok {
			return fmt.Errorf("%w: %q", ErrTopologyUnknownSegment, conn.To)
		}

		coupling, ok := couplingSet[conn.CouplingID]
		if !ok {
			return fmt.Errorf("%w: %q", ErrTopologyUnknownCoupling, conn.CouplingID)
		}
		if coupling.FromSegment != "" && coupling.FromSegment != conn.From {
			return fmt.Errorf("%w: coupling=%q from=%q", ErrTopologyCouplingSegmentMismatch, coupling.ID, conn.From)
		}
		if coupling.ToSegment != "" && coupling.ToSegment != conn.To {
			return fmt.Errorf("%w: coupling=%q to=%q", ErrTopologyCouplingSegmentMismatch, coupling.ID, conn.To)
		}

		outDegree[conn.From]++
		if !cfg.Topology.AllowAmbiguousOrder && outDegree[conn.From] > 1 {
			return fmt.Errorf("%w: from=%q", ErrTopologyAmbiguousOrder, conn.From)
		}
		inDegree[conn.To]++
		adjacency[conn.From] = append(adjacency[conn.From], conn.To)
	}

	// Detect cycles so engine-owned ordering remains well-defined.
	queue := make([]pipelinetypes.SegmentID, 0, len(inDegree))
	for segID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, segID)
		}
	}
	visited := 0
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		visited++
		for _, downstream := range adjacency[current] {
			inDegree[downstream]--
			if inDegree[downstream] == 0 {
				queue = append(queue, downstream)
			}
		}
	}
	if visited != len(segmentSet) {
		return ErrTopologyCycle
	}

	return nil
}
