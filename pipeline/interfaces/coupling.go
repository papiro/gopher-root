package interfaces

import (
	"encoding/json"
	"errors"
)

// ErrCouplingNil indicates a coupling instance was not provided.
var ErrCouplingNil = errors.New("coupling is required")

// ErrCouplingInputInvalidJSON indicates coupling input is not valid serialized JSON.
var ErrCouplingInputInvalidJSON = errors.New("coupling input must be valid JSON")

// ErrCouplingOutputInvalidJSON indicates coupling output is not valid serialized JSON.
var ErrCouplingOutputInvalidJSON = errors.New("coupling output must be valid JSON")

// Coupling converts one segment's serialized JSON output into another segment's required JSON input.
// A coupling is a stateless transform artifact.
type Coupling interface {
	// Couple transforms one valid JSON payload into another valid JSON payload.
	Couple(segmentOutput json.RawMessage) (nextSegmentInput json.RawMessage, err error)
}

// ApplyCoupling runs one coupling with baseline JSON-contract checks.
func ApplyCoupling(c Coupling, segmentOutput json.RawMessage) (json.RawMessage, error) {
	if c == nil {
		return nil, ErrCouplingNil
	}
	if !json.Valid(segmentOutput) {
		return nil, ErrCouplingInputInvalidJSON
	}

	nextInput, err := c.Couple(segmentOutput)
	if err != nil {
		return nil, err
	}
	if !json.Valid(nextInput) {
		return nil, ErrCouplingOutputInvalidJSON
	}
	return nextInput, nil
}
