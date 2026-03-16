package interfaces

import (
	"context"

	pipelinetypes "github.com/pierre/gopher-root/pipeline/types"
)

// Engine orchestrates source, segments, sinks, and durable progress APIs.
type Engine[TIn, TOut any] interface {
	// Run starts or continues processing until completion, cancellation, or fatal error.
	Run(ctx context.Context) error
	// Pause requests the next resumable boundary, snapshots in-flight frontier state,
	// and persists progress before returning.
	Pause(ctx context.Context) error
	// Resume restores persisted segment/frontier state and continues processing from
	// the next durable recovery boundary.
	Resume(ctx context.Context) error
	// Retry replays a specific lineage record according to retry and compensation rules.
	Retry(ctx context.Context, record pipelinetypes.RecordID) error
	// Trace returns terminal outputs derived from one source record identity.
	Trace(ctx context.Context, source pipelinetypes.RecordID) ([]pipelinetypes.Envelope[TOut], error)
}
