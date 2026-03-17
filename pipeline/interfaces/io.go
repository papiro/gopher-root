package interfaces

import (
	"context"

	pipelinetypes "github.com/pierre/manifold/pipeline/types"
)

// Source yields source records in pull mode where downstream controls pace.
type Source[T any] interface {
	// Next returns (record, true, nil) for data, (zero, false, nil) for end-of-stream, or error.
	Next(ctx context.Context) (pipelinetypes.SourceRecord[T], bool, error)
	// SnapshotCursor returns the source-owned resume token at the current durable boundary.
	SnapshotCursor(ctx context.Context) ([]byte, error)
	// RestoreCursor restores the source-owned resume token before processing resumes.
	RestoreCursor(ctx context.Context, cursor []byte) error
}

// StreamSource yields source records in push mode where upstream controls emission cadence.
type StreamSource[T any] interface {
	// Stream returns a channel that the source closes on completion or fatal error.
	Stream(ctx context.Context) <-chan pipelinetypes.SourceRecord[T]
}

// Sink is a terminal destination where Consume behavior defines back-pressure semantics.
type Sink[T any] interface {
	// Consume handles one envelope and should block when the sink wants to apply back-pressure.
	Consume(ctx context.Context, item pipelinetypes.Envelope[T]) error
}

// SinkWithDone extends Sink with an explicit completion hook.
type SinkWithDone[T any] interface {
	// Sink is embedded so all Consume semantics still apply.
	Sink[T]
	// Done allows sink-specific flush/finalize work once upstream has finished.
	Done(ctx context.Context) error
}
