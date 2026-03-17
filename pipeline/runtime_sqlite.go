package pipeline

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

//go:embed sqlite/migrations
var sqliteRuntimeMigrations embed.FS

// SQLiteRuntime persists the runtime contract from `spec/manifold/v1/runtime.proto`
// in SQLite using manifold-owned embedded migrations.
type SQLiteRuntime struct {
	db *sql.DB
}

// NewSQLiteRuntime opens a manifold-owned SQLite runtime database, applies any
// pending Atlas migrations shipped with this library, and returns a ready runtime.
func NewSQLiteRuntime(ctx context.Context, databasePath string) (*SQLiteRuntime, error) {
	if err := migrateSQLiteRuntime(ctx, databasePath); err != nil {
		return nil, err
	}

	db, err := openSQLiteRuntimeDB(ctx, databasePath)
	if err != nil {
		return nil, err
	}

	return &SQLiteRuntime{db: db}, nil
}

func (r *SQLiteRuntime) LoadCheckpoint(ctx context.Context, pipelineID string) (Checkpoint, bool, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT source_cursor, frontier_json, paused
		FROM mf_checkpoints
		WHERE pipeline_id = ?
	`, pipelineID)

	var (
		sourceCursor []byte
		frontierJSON []byte
		paused       bool
	)
	if err := row.Scan(&sourceCursor, &frontierJSON, &paused); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Checkpoint{}, false, nil
		}
		return Checkpoint{}, false, fmt.Errorf("load checkpoint %q: %w", pipelineID, err)
	}

	var frontier []CheckpointFrame
	if err := decodeJSON(frontierJSON, &frontier); err != nil {
		return Checkpoint{}, false, fmt.Errorf("decode checkpoint frontier %q: %w", pipelineID, err)
	}

	return Checkpoint{
		PipelineID:   pipelineID,
		SourceCursor: append([]byte(nil), sourceCursor...),
		Frontier:     frontier,
		Paused:       paused,
	}, true, nil
}

func (r *SQLiteRuntime) SaveCheckpoint(ctx context.Context, checkpoint Checkpoint) error {
	frontierJSON, err := encodeJSON(checkpoint.Frontier)
	if err != nil {
		return fmt.Errorf("encode checkpoint frontier %q: %w", checkpoint.PipelineID, err)
	}

	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO mf_checkpoints (
			pipeline_id,
			source_cursor,
			frontier_json,
			paused
		) VALUES (?, ?, ?, ?)
		ON CONFLICT(pipeline_id) DO UPDATE SET
			source_cursor = excluded.source_cursor,
			frontier_json = excluded.frontier_json,
			paused = excluded.paused
	`, checkpoint.PipelineID, checkpoint.SourceCursor, frontierJSON, checkpoint.Paused); err != nil {
		return fmt.Errorf("save checkpoint %q: %w", checkpoint.PipelineID, err)
	}

	return nil
}

func (r *SQLiteRuntime) SaveSegmentState(ctx context.Context, state SegmentState) error {
	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO mf_segment_states (
			pipeline_id,
			segment_id,
			record_id,
			attempt_id,
			snapshot
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, segment_id, record_id, attempt_id) DO UPDATE SET
			snapshot = excluded.snapshot
	`, state.PipelineID, state.SegmentID, state.RecordID, state.AttemptID, state.Snapshot); err != nil {
		return fmt.Errorf(
			"save segment state pipeline=%q segment=%q record=%q attempt=%d: %w",
			state.PipelineID,
			state.SegmentID,
			state.RecordID,
			state.AttemptID,
			err,
		)
	}

	return nil
}

func (r *SQLiteRuntime) LoadSegmentState(ctx context.Context, pipelineID string, segment SegmentID, record RecordID, attempt AttemptID) (SegmentState, bool, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT snapshot
		FROM mf_segment_states
		WHERE pipeline_id = ? AND segment_id = ? AND record_id = ? AND attempt_id = ?
	`, pipelineID, segment, record, attempt)

	var snapshot []byte
	if err := row.Scan(&snapshot); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SegmentState{}, false, nil
		}
		return SegmentState{}, false, fmt.Errorf(
			"load segment state pipeline=%q segment=%q record=%q attempt=%d: %w",
			pipelineID,
			segment,
			record,
			attempt,
			err,
		)
	}

	return SegmentState{
		PipelineID: pipelineID,
		SegmentID:  segment,
		RecordID:   record,
		AttemptID:  attempt,
		Snapshot:   append([]byte(nil), snapshot...),
	}, true, nil
}

func (r *SQLiteRuntime) DeleteSegmentState(ctx context.Context, pipelineID string, segment SegmentID, record RecordID, attempt AttemptID) error {
	if _, err := r.db.ExecContext(ctx, `
		DELETE FROM mf_segment_states
		WHERE pipeline_id = ? AND segment_id = ? AND record_id = ? AND attempt_id = ?
	`, pipelineID, segment, record, attempt); err != nil {
		return fmt.Errorf(
			"delete segment state pipeline=%q segment=%q record=%q attempt=%d: %w",
			pipelineID,
			segment,
			record,
			attempt,
			err,
		)
	}

	return nil
}

func (r *SQLiteRuntime) CommitSegment(ctx context.Context, commit SegmentCommit) error {
	parentIDsJSON, err := encodeJSON(commit.ParentIDs)
	if err != nil {
		return fmt.Errorf(
			"encode segment commit parents pipeline=%q segment=%q record=%q: %w",
			commit.PipelineID,
			commit.SegmentID,
			commit.RecordID,
			err,
		)
	}

	var errorMessage any
	if commit.Err != nil {
		errorMessage = commit.Err.Error()
	}

	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO mf_segment_acks (
			pipeline_id,
			segment_id,
			record_id,
			attempt_id,
			origin_record_id,
			parent_ids_json,
			status,
			error_message
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, segment_id, record_id) DO UPDATE SET
			attempt_id = excluded.attempt_id,
			origin_record_id = excluded.origin_record_id,
			parent_ids_json = excluded.parent_ids_json,
			status = excluded.status,
			error_message = excluded.error_message
	`,
		commit.PipelineID,
		commit.SegmentID,
		commit.RecordID,
		commit.AttemptID,
		commit.OriginRecordID,
		parentIDsJSON,
		commit.Status,
		errorMessage,
	); err != nil {
		return fmt.Errorf(
			"commit segment pipeline=%q segment=%q record=%q: %w",
			commit.PipelineID,
			commit.SegmentID,
			commit.RecordID,
			err,
		)
	}

	return nil
}

func (r *SQLiteRuntime) CommitSegmentOutput(ctx context.Context, output SegmentOutputRecord) error {
	encoded, err := encodeRawEnvelope(output.Item)
	if err != nil {
		return fmt.Errorf(
			"encode segment output pipeline=%q segment=%q record=%q: %w",
			output.PipelineID,
			output.SegmentID,
			output.Item.RecordID,
			err,
		)
	}

	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO mf_segment_outputs (
			pipeline_id,
			segment_id,
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, segment_id, record_id) DO UPDATE SET
			origin_record_id = excluded.origin_record_id,
			attempt_id = excluded.attempt_id,
			parent_ids_json = excluded.parent_ids_json,
			segment_path_json = excluded.segment_path_json,
			payload_json = excluded.payload_json,
			metadata_json = excluded.metadata_json
	`,
		output.PipelineID,
		output.SegmentID,
		output.Item.OriginRecordID,
		output.Item.RecordID,
		output.Item.AttemptID,
		encoded.ParentIDsJSON,
		encoded.SegmentPathJSON,
		encoded.PayloadJSON,
		encoded.MetadataJSON,
	); err != nil {
		return fmt.Errorf(
			"commit segment output pipeline=%q segment=%q record=%q: %w",
			output.PipelineID,
			output.SegmentID,
			output.Item.RecordID,
			err,
		)
	}

	return nil
}

func (r *SQLiteRuntime) CommitTerminal(ctx context.Context, terminal TerminalRecord) error {
	encoded, err := encodeRawEnvelope(terminal.Item)
	if err != nil {
		return fmt.Errorf(
			"encode terminal record pipeline=%q record=%q: %w",
			terminal.PipelineID,
			terminal.Item.RecordID,
			err,
		)
	}

	if _, err := r.db.ExecContext(ctx, `
		INSERT INTO mf_terminal_records (
			pipeline_id,
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, record_id) DO UPDATE SET
			origin_record_id = excluded.origin_record_id,
			attempt_id = excluded.attempt_id,
			parent_ids_json = excluded.parent_ids_json,
			segment_path_json = excluded.segment_path_json,
			payload_json = excluded.payload_json,
			metadata_json = excluded.metadata_json
	`,
		terminal.PipelineID,
		terminal.Item.OriginRecordID,
		terminal.Item.RecordID,
		terminal.Item.AttemptID,
		encoded.ParentIDsJSON,
		encoded.SegmentPathJSON,
		encoded.PayloadJSON,
		encoded.MetadataJSON,
	); err != nil {
		return fmt.Errorf(
			"commit terminal record pipeline=%q record=%q: %w",
			terminal.PipelineID,
			terminal.Item.RecordID,
			err,
		)
	}

	return nil
}

func (r *SQLiteRuntime) SegmentOutputs(ctx context.Context, pipelineID string, segment SegmentID, origin RecordID) ([]Envelope[json.RawMessage], error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		FROM mf_segment_outputs
		WHERE pipeline_id = ? AND segment_id = ? AND origin_record_id = ?
		ORDER BY id ASC
	`, pipelineID, segment, origin)
	if err != nil {
		return nil, fmt.Errorf(
			"query segment outputs pipeline=%q segment=%q origin=%q: %w",
			pipelineID,
			segment,
			origin,
			err,
		)
	}
	defer rows.Close()

	var outputs []Envelope[json.RawMessage]
	for rows.Next() {
		item, err := scanRawEnvelope(rows)
		if err != nil {
			return nil, fmt.Errorf(
				"scan segment output pipeline=%q segment=%q origin=%q: %w",
				pipelineID,
				segment,
				origin,
				err,
			)
		}
		outputs = append(outputs, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate segment outputs pipeline=%q segment=%q origin=%q: %w",
			pipelineID,
			segment,
			origin,
			err,
		)
	}

	return outputs, nil
}

func (r *SQLiteRuntime) Trace(ctx context.Context, pipelineID string, origin RecordID) ([]Envelope[json.RawMessage], error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		FROM mf_terminal_records
		WHERE pipeline_id = ? AND origin_record_id = ?
		ORDER BY id ASC
	`, pipelineID, origin)
	if err != nil {
		return nil, fmt.Errorf("query trace pipeline=%q origin=%q: %w", pipelineID, origin, err)
	}
	defer rows.Close()

	var outputs []Envelope[json.RawMessage]
	for rows.Next() {
		item, err := scanRawEnvelope(rows)
		if err != nil {
			return nil, fmt.Errorf("scan trace pipeline=%q origin=%q: %w", pipelineID, origin, err)
		}
		outputs = append(outputs, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate trace pipeline=%q origin=%q: %w", pipelineID, origin, err)
	}

	return outputs, nil
}

func (r *SQLiteRuntime) Ack(ctx context.Context, pipelineID string, segment SegmentID, record RecordID) (SegmentAck, bool, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT attempt_id, status, error_message
		FROM mf_segment_acks
		WHERE pipeline_id = ? AND segment_id = ? AND record_id = ?
	`, pipelineID, segment, record)

	var (
		attempt      AttemptID
		status       AckStatus
		errorMessage sql.NullString
	)
	if err := row.Scan(&attempt, &status, &errorMessage); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SegmentAck{}, false, nil
		}
		return SegmentAck{}, false, fmt.Errorf(
			"load ack pipeline=%q segment=%q record=%q: %w",
			pipelineID,
			segment,
			record,
			err,
		)
	}

	var ackErr error
	if errorMessage.Valid {
		ackErr = errors.New(errorMessage.String)
	}

	return SegmentAck{
		Segment:  segment,
		RecordID: record,
		Attempt:  attempt,
		Status:   status,
		Err:      ackErr,
	}, true, nil
}

type encodedRawEnvelope struct {
	ParentIDsJSON   []byte
	SegmentPathJSON []byte
	PayloadJSON     []byte
	MetadataJSON    []byte
}

func encodeRawEnvelope(item Envelope[json.RawMessage]) (encodedRawEnvelope, error) {
	parentIDsJSON, err := encodeJSON(item.ParentIDs)
	if err != nil {
		return encodedRawEnvelope{}, err
	}
	segmentPathJSON, err := encodeJSON(item.SegmentPath)
	if err != nil {
		return encodedRawEnvelope{}, err
	}
	metadataJSON, err := encodeJSON(item.Metadata)
	if err != nil {
		return encodedRawEnvelope{}, err
	}

	return encodedRawEnvelope{
		ParentIDsJSON:   parentIDsJSON,
		SegmentPathJSON: segmentPathJSON,
		PayloadJSON:     append([]byte(nil), item.Payload...),
		MetadataJSON:    metadataJSON,
	}, nil
}

func scanRawEnvelope(scanner interface {
	Scan(dest ...any) error
}) (Envelope[json.RawMessage], error) {
	var (
		originRecordID  RecordID
		recordID        RecordID
		attemptID       AttemptID
		parentIDsJSON   []byte
		segmentPathJSON []byte
		payloadJSON     []byte
		metadataJSON    []byte
	)
	if err := scanner.Scan(
		&originRecordID,
		&recordID,
		&attemptID,
		&parentIDsJSON,
		&segmentPathJSON,
		&payloadJSON,
		&metadataJSON,
	); err != nil {
		return Envelope[json.RawMessage]{}, err
	}

	var parentIDs []RecordID
	if err := decodeJSON(parentIDsJSON, &parentIDs); err != nil {
		return Envelope[json.RawMessage]{}, err
	}

	var segmentPath []SegmentID
	if err := decodeJSON(segmentPathJSON, &segmentPath); err != nil {
		return Envelope[json.RawMessage]{}, err
	}

	var metadata map[string]string
	if err := decodeJSON(metadataJSON, &metadata); err != nil {
		return Envelope[json.RawMessage]{}, err
	}

	return Envelope[json.RawMessage]{
		OriginRecordID: originRecordID,
		RecordID:       recordID,
		AttemptID:      attemptID,
		ParentIDs:      parentIDs,
		SegmentPath:    segmentPath,
		Payload:        append(json.RawMessage(nil), payloadJSON...),
		Metadata:       metadata,
	}, nil
}

func encodeJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return raw, nil
}

func decodeJSON(raw []byte, dst any) error {
	if len(raw) == 0 {
		return nil
	}
	return json.Unmarshal(raw, dst)
}

func migrateSQLiteRuntime(ctx context.Context, databasePath string) error {
	migrations, err := fs.Sub(sqliteRuntimeMigrations, "sqlite/migrations")
	if err != nil {
		return fmt.Errorf("load embedded sqlite runtime migrations: %w", err)
	}

	workdir, err := os.MkdirTemp("", "manifold-atlas-*")
	if err != nil {
		return fmt.Errorf("prepare Atlas working directory for sqlite runtime migrations: %w", err)
	}
	defer os.RemoveAll(workdir)

	if err := writeFS(filepath.Join(workdir, "migrations"), migrations); err != nil {
		return fmt.Errorf("materialize sqlite runtime migrations for Atlas: %w", err)
	}

	dbURL, err := sqliteRuntimeAtlasURL(databasePath)
	if err != nil {
		return err
	}

	cmd := exec.CommandContext(
		ctx,
		"atlas",
		"migrate",
		"apply",
		"--dir",
		"file://migrations",
		"--url",
		dbURL,
	)
	cmd.Dir = workdir

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		if stderr.Len() > 0 {
			return fmt.Errorf("apply sqlite runtime migrations with Atlas: %w: %s", err, strings.TrimSpace(stderr.String()))
		}
		return fmt.Errorf("apply sqlite runtime migrations with Atlas: %w", err)
	}

	return nil
}

func openSQLiteRuntimeDB(ctx context.Context, databasePath string) (*sql.DB, error) {
	if strings.TrimSpace(databasePath) == "" {
		return nil, fmt.Errorf("sqlite runtime database path is required")
	}

	absPath, err := filepath.Abs(databasePath)
	if err != nil {
		return nil, fmt.Errorf("resolve sqlite runtime database path %q: %w", databasePath, err)
	}

	db, err := sql.Open("sqlite", absPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite runtime database %q: %w", absPath, err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping sqlite runtime database %q: %w", absPath, err)
	}

	return db, nil
}

func sqliteRuntimeAtlasURL(databasePath string) (string, error) {
	if strings.TrimSpace(databasePath) == "" {
		return "", fmt.Errorf("sqlite runtime database path is required")
	}

	absPath, err := filepath.Abs(databasePath)
	if err != nil {
		return "", fmt.Errorf("resolve sqlite runtime database path %q: %w", databasePath, err)
	}

	return (&url.URL{
		Scheme: "sqlite",
		Path:   filepath.ToSlash(absPath),
	}).String(), nil
}

func writeFS(dstDir string, src fs.FS) error {
	if err := os.MkdirAll(dstDir, 0o700); err != nil {
		return err
	}

	return fs.WalkDir(src, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || path == "." {
			return err
		}

		dstPath := filepath.Join(dstDir, path)
		if d.IsDir() {
			return os.MkdirAll(dstPath, 0o700)
		}

		data, err := fs.ReadFile(src, path)
		if err != nil {
			return err
		}

		return os.WriteFile(dstPath, data, 0o600)
	})
}
