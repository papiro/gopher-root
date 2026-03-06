package interfaces

import (
	"context"

	pipelinetypes "github.com/pierre/gopher-root/pipeline/types"
)

// SegmentStateStore persists segment-owned snapshots used by pause/resume and restart recovery.
type SegmentStateStore interface {
	// Load retrieves the latest persisted snapshot for one segment.
	Load(ctx context.Context, segment pipelinetypes.SegmentID) ([]byte, error)
	// Save persists the latest snapshot bytes for one segment.
	Save(ctx context.Context, segment pipelinetypes.SegmentID, snapshot []byte) error
	// Migrate transforms previously persisted snapshot bytes across segment versions.
	Migrate(ctx context.Context, segment pipelinetypes.SegmentID, fromVersion, toVersion string, old []byte) ([]byte, error)
}

// AckGraphStore is the durable source of truth for per-record progress and lineage.
type AckGraphStore interface {
	// CommitAck stores one segment acknowledgment as an atomic durable update.
	CommitAck(ctx context.Context, ack pipelinetypes.SegmentAck) error
	// LinkParentChild records lineage edge(s) so source-to-output tracing is possible.
	LinkParentChild(ctx context.Context, parent pipelinetypes.RecordID, child pipelinetypes.RecordID) error
	// GetAck returns one segment acknowledgment if present.
	GetAck(ctx context.Context, segment pipelinetypes.SegmentID, record pipelinetypes.RecordID) (pipelinetypes.SegmentAck, bool, error)
	// Children returns direct descendants for fan-out tracing.
	Children(ctx context.Context, record pipelinetypes.RecordID) ([]pipelinetypes.RecordID, error)
	// Parents returns direct ancestors for explainability and replay strategies.
	Parents(ctx context.Context, record pipelinetypes.RecordID) ([]pipelinetypes.RecordID, error)
	// PendingBySegment returns records observed but not yet committed for one segment.
	PendingBySegment(ctx context.Context, segment pipelinetypes.SegmentID) ([]pipelinetypes.RecordID, error)
}

// Acker is a minimal acknowledgment writer used when full graph access is unnecessary.
type Acker interface {
	// Commit persists one segment acknowledgment.
	Commit(ctx context.Context, ack pipelinetypes.SegmentAck) error
}
