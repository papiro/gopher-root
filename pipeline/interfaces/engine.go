package interfaces

import (
	"context"

	pipelinetypes "github.com/pierre/manifold/pipeline/types"
)

// Engine orchestrates source, segments, sinks, and durable progress APIs.
type Engine[TIn, TOut any] interface {
	// Run starts processing from the latest durable recovery boundary if one exists,
	// otherwise it starts from the source's initial position.
	Run(ctx context.Context) error
	// Pause requests the next resumable boundary and persists enough progress to
	// infer the next resumable lineage step before returning.
	Pause(ctx context.Context) error
	// Resume explicitly requires durable recovery state and continues processing from
	// the next persisted recovery boundary.
	Resume(ctx context.Context) error
	// Restart discards durable recovery state for this pipeline, runs any configured
	// restart hook, and then starts from the source's initial position.
	Restart(ctx context.Context) error
	// Retry replays a specific lineage record according to retry and compensation rules.
	Retry(ctx context.Context, record pipelinetypes.RecordID) error
	// Trace returns terminal outputs derived from one source record identity.
	Trace(ctx context.Context, source pipelinetypes.RecordID) ([]pipelinetypes.Envelope[TOut], error)
}
