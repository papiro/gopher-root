package pipeline

import (
	"encoding/json"

	contracts "github.com/pierre/gopher-root/pipeline/interfaces"
	pipelinetypes "github.com/pierre/gopher-root/pipeline/types"
)

type (
	SegmentID       = pipelinetypes.SegmentID
	CouplingID      = pipelinetypes.CouplingID
	RecordID        = pipelinetypes.RecordID
	AttemptID       = pipelinetypes.AttemptID
	IdempotencyKind = pipelinetypes.IdempotencyKind
	AckStatus       = pipelinetypes.AckStatus

	Envelope[T any] = pipelinetypes.Envelope[T]
	SourceRecord[T any] = pipelinetypes.SourceRecord[T]

	SegmentAck         = pipelinetypes.SegmentAck
	SegmentDescriptor  = pipelinetypes.SegmentDescriptor
	CouplingDescriptor = pipelinetypes.CouplingDescriptor
	Connection         = pipelinetypes.Connection
	Topology           = pipelinetypes.Topology
	EngineConfig       = pipelinetypes.EngineConfig

	Source[T any]       = contracts.Source[T]
	StreamSource[T any] = contracts.StreamSource[T]
	Sink[T any]         = contracts.Sink[T]
	SinkWithDone[T any] = contracts.SinkWithDone[T]

	SegmentStateStore      = contracts.SegmentStateStore
	AckGraphStore          = contracts.AckGraphStore
	Acker                  = contracts.Acker
	Compensator            = contracts.Compensator
	Coupling               = contracts.Coupling
	Segment[TIn, TOut any] = contracts.Segment[TIn, TOut]
	Engine[TIn, TOut any]  = contracts.Engine[TIn, TOut]
)

const (
	Idempotent    IdempotencyKind = pipelinetypes.Idempotent
	NonIdempotent IdempotencyKind = pipelinetypes.NonIdempotent

	AckCommitted     AckStatus = pipelinetypes.AckCommitted
	AckRetryableFail AckStatus = pipelinetypes.AckRetryableFail
	AckTerminalFail  AckStatus = pipelinetypes.AckTerminalFail
)

var (
	ErrCompensatorRequired       = contracts.ErrCompensatorRequired
	ErrSegmentIDRequired         = contracts.ErrSegmentIDRequired
	ErrCouplingNil               = contracts.ErrCouplingNil
	ErrCouplingInputInvalidJSON  = contracts.ErrCouplingInputInvalidJSON
	ErrCouplingOutputInvalidJSON = contracts.ErrCouplingOutputInvalidJSON

	ErrEngineConfigSegmentIDRequired      = contracts.ErrEngineConfigSegmentIDRequired
	ErrEngineConfigDuplicateSegmentID     = contracts.ErrEngineConfigDuplicateSegmentID
	ErrEngineConfigCouplingIDRequired     = contracts.ErrEngineConfigCouplingIDRequired
	ErrEngineConfigDuplicateCouplingID    = contracts.ErrEngineConfigDuplicateCouplingID
	ErrTopologyConnectionFromRequired     = contracts.ErrTopologyConnectionFromRequired
	ErrTopologyConnectionToRequired       = contracts.ErrTopologyConnectionToRequired
	ErrTopologyConnectionCouplingRequired = contracts.ErrTopologyConnectionCouplingRequired
	ErrTopologyUnknownSegment             = contracts.ErrTopologyUnknownSegment
	ErrTopologyUnknownCoupling            = contracts.ErrTopologyUnknownCoupling
	ErrTopologyCouplingSegmentMismatch    = contracts.ErrTopologyCouplingSegmentMismatch
	ErrTopologyAmbiguousOrder             = contracts.ErrTopologyAmbiguousOrder
	ErrTopologyCycle                      = contracts.ErrTopologyCycle
)

// ValidateSegment performs baseline contract checks independent of engine implementation.
func ValidateSegment[TIn, TOut any](s Segment[TIn, TOut]) error {
	return contracts.ValidateSegment[TIn, TOut](s)
}

// ApplyCoupling runs one coupling with baseline JSON-contract checks.
func ApplyCoupling(c Coupling, segmentOutput json.RawMessage) (json.RawMessage, error) {
	return contracts.ApplyCoupling(c, segmentOutput)
}

// ValidateTopology enforces engine-owned ordering and coupling assignment constraints.
func ValidateTopology(cfg EngineConfig) error {
	return contracts.ValidateTopology(cfg)
}

// DeterministicValue returns a pointer used to explicitly set deterministic behavior.
func DeterministicValue(v bool) *bool {
	return pipelinetypes.DeterministicValue(v)
}
