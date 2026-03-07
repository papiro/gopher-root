package interfaces

import (
	"context"

	pipelinetypes "github.com/pierre/gopher-root/pipeline/types"
)

// Engine orchestrates source, segments, sinks, and durable progress APIs.
type Engine[TIn, TOut any] interface {
	// Run starts or continues processing until completion, cancellation, or fatal error.
	Run(ctx context.Context) error
	// Pause requests safe boundaries, flushes, and snapshots before returning.
	Pause(ctx context.Context) error
	// Resume continues processing from the next durable recovery boundary.
	Resume(ctx context.Context) error
	// Retry replays a specific lineage record according to retry and compensation rules.
	Retry(ctx context.Context, record pipelinetypes.RecordID) error
	// Trace returns terminal outputs derived from one source record identity.
	Trace(ctx context.Context, source pipelinetypes.RecordID) ([]pipelinetypes.Envelope[TOut], error)
}
