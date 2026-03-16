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

	Envelope[T any]      = pipelinetypes.Envelope[T]
	SegmentRecord[T any] = pipelinetypes.SegmentRecord[T]
	SourceRecord[T any]  = pipelinetypes.SourceRecord[T]

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
	Runtime             = contracts.Runtime

	Checkpoint                         = contracts.Checkpoint
	CheckpointFrame                    = contracts.CheckpointFrame
	SegmentState                       = contracts.SegmentState
	SegmentCommit                      = contracts.SegmentCommit
	SegmentOutputRecord                = contracts.SegmentOutputRecord
	TerminalRecord                     = contracts.TerminalRecord
	Compensator                        = contracts.Compensator
	ProcessContext                     = contracts.ProcessContext
	ProcessStatus                      = contracts.ProcessStatus
	ProcessResult                      = contracts.ProcessResult
	Coupling                           = contracts.Coupling
	Segment[TIn, TOut any]             = contracts.Segment[TIn, TOut]
	CompensatingSegment[TIn, TOut any] = contracts.CompensatingSegment[TIn, TOut]
	Engine[TIn, TOut any]              = contracts.Engine[TIn, TOut]
)

const (
	Idempotent    IdempotencyKind = pipelinetypes.Idempotent
	NonIdempotent IdempotencyKind = pipelinetypes.NonIdempotent

	AckCommitted     AckStatus = pipelinetypes.AckCommitted
	AckRetryableFail AckStatus = pipelinetypes.AckRetryableFail
	AckTerminalFail  AckStatus = pipelinetypes.AckTerminalFail

	ProcessCompleted ProcessStatus = contracts.ProcessCompleted
	ProcessPaused    ProcessStatus = contracts.ProcessPaused
)

var (
	ErrCompensatorRequired       = contracts.ErrCompensatorRequired
	ErrSourceRequired            = contracts.ErrSourceRequired
	ErrStreamSourceRequired      = contracts.ErrStreamSourceRequired
	ErrSegmentIDRequired         = contracts.ErrSegmentIDRequired
	ErrSinkRequired              = contracts.ErrSinkRequired
	ErrRuntimeRequired           = contracts.ErrRuntimeRequired
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

// ValidateSource performs baseline contract checks independent of engine implementation.
func ValidateSource[T any](s Source[T]) error {
	return contracts.ValidateSource(s)
}

// ValidateStreamSource performs baseline contract checks independent of engine implementation.
func ValidateStreamSource[T any](s StreamSource[T]) error {
	return contracts.ValidateStreamSource(s)
}

// ValidateSink performs baseline contract checks independent of engine implementation.
func ValidateSink[T any](s Sink[T]) error {
	return contracts.ValidateSink(s)
}

// ValidateRuntime performs baseline contract checks independent of engine implementation.
func ValidateRuntime(r Runtime) error {
	return contracts.ValidateRuntime(r)
}

// ValidateSegment performs baseline contract checks independent of engine implementation.
func ValidateSegment[TIn, TOut any](s Segment[TIn, TOut]) error {
	return contracts.ValidateSegment(s)
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
