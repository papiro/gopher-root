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

func (r *SQLiteRuntime) LoadSourceResumeState(ctx context.Context, pipelineID string) (SourceResumeState, bool, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT source_cursor, paused
		FROM mf_source_resume_states
		WHERE pipeline_id = ?
	`, pipelineID)

	var (
		sourceCursor []byte
		paused       bool
	)
	if err := row.Scan(&sourceCursor, &paused); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SourceResumeState{}, false, nil
		}
		return SourceResumeState{}, false, fmt.Errorf("load source resume state %q: %w", pipelineID, err)
	}

	return SourceResumeState{
		PipelineID:   pipelineID,
		SourceCursor: append([]byte(nil), sourceCursor...),
		Paused:       paused,
	}, true, nil
}

func (r *SQLiteRuntime) SaveSourceResumeState(ctx context.Context, resumeState SourceResumeState) error {
	return saveSourceResumeStateExec(ctx, r.db, resumeState)
}

func (r *SQLiteRuntime) CommitSourceProgress(ctx context.Context, progress SourceProgress) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin source progress %q: %w", progress.ResumeState.PipelineID, err)
	}
	defer tx.Rollback()

	if err := saveSourceResumeStateExec(ctx, tx, progress.ResumeState); err != nil {
		return err
	}
	if err := commitSourceRecordExec(ctx, tx, progress.Started); err != nil {
		return err
	}
	if progress.PendingWork.PipelineID != "" {
		if err := commitPendingWorkExec(ctx, tx, progress.PendingWork); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit source progress %q: %w", progress.ResumeState.PipelineID, err)
	}
	return nil
}

func (r *SQLiteRuntime) CommitProgressUpdate(ctx context.Context, update RuntimeDelta) error {
	pipelineID := ""
	if update.Progress != nil {
		pipelineID = update.Progress.PipelineID
	} else if update.DeletePendingWork != nil {
		pipelineID = update.DeletePendingWork.PipelineID
	} else if len(update.EnqueuePendingWork) > 0 {
		pipelineID = update.EnqueuePendingWork[0].PipelineID
	} else if update.OutputRecord != nil {
		pipelineID = update.OutputRecord.PipelineID
	} else if update.TerminalRecord != nil {
		pipelineID = update.TerminalRecord.PipelineID
	} else if update.DeterministicResult != nil {
		pipelineID = update.DeterministicResult.PipelineID
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin progress update %q: %w", pipelineID, err)
	}
	defer tx.Rollback()

	if update.Progress != nil {
		if err := saveSegmentProgressExec(ctx, tx, *update.Progress); err != nil {
			return err
		}
	}
	if update.DeletePendingWork != nil {
		if err := deletePendingWorkExec(ctx, tx, *update.DeletePendingWork); err != nil {
			return err
		}
	}
	for _, item := range update.EnqueuePendingWork {
		if err := commitPendingWorkExec(ctx, tx, item); err != nil {
			return err
		}
	}
	if update.OutputRecord != nil {
		if err := commitSegmentOutputExec(ctx, tx, *update.OutputRecord); err != nil {
			return err
		}
	}
	if update.TerminalRecord != nil {
		if err := commitTerminalExec(ctx, tx, *update.TerminalRecord); err != nil {
			return err
		}
	}
	if update.DeterministicResult != nil {
		if err := saveDeterministicResultExec(ctx, tx, *update.DeterministicResult); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit progress update %q: %w", pipelineID, err)
	}
	return nil
}

func (r *SQLiteRuntime) DeterministicResult(ctx context.Context, pipelineID string, segment SegmentID, compatibilityVersion string, inputChecksum string) (DeterministicSegmentResult, bool, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT outputs_json
		FROM mf_deterministic_segment_results
		WHERE pipeline_id = ? AND segment_id = ? AND compatibility_version = ? AND input_checksum = ?
	`, pipelineID, segment, compatibilityVersion, inputChecksum)

	var outputsJSON []byte
	if err := row.Scan(&outputsJSON); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return DeterministicSegmentResult{}, false, nil
		}
		return DeterministicSegmentResult{}, false, fmt.Errorf("load deterministic result pipeline=%q segment=%q version=%q checksum=%q: %w", pipelineID, segment, compatibilityVersion, inputChecksum, err)
	}

	var outputs []DeterministicSegmentOutput
	if err := decodeJSON(outputsJSON, &outputs); err != nil {
		return DeterministicSegmentResult{}, false, fmt.Errorf("decode deterministic result pipeline=%q segment=%q version=%q checksum=%q: %w", pipelineID, segment, compatibilityVersion, inputChecksum, err)
	}

	return DeterministicSegmentResult{
		PipelineID:           pipelineID,
		SegmentID:            segment,
		CompatibilityVersion: compatibilityVersion,
		InputChecksum:        inputChecksum,
		Outputs:              cloneDeterministicOutputs(outputs),
	}, true, nil
}

func (r *SQLiteRuntime) ResetPipeline(ctx context.Context, pipelineID string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reset pipeline %q: %w", pipelineID, err)
	}
	defer tx.Rollback()

	for _, query := range []string{
		`DELETE FROM mf_terminal_records WHERE pipeline_id = ?`,
		`DELETE FROM mf_segment_outputs WHERE pipeline_id = ?`,
		`DELETE FROM mf_pending_segment_work WHERE pipeline_id = ?`,
		`DELETE FROM mf_segment_progress WHERE pipeline_id = ?`,
		`DELETE FROM mf_deterministic_segment_results WHERE pipeline_id = ?`,
		`DELETE FROM mf_source_records WHERE pipeline_id = ?`,
		`DELETE FROM mf_source_resume_states WHERE pipeline_id = ?`,
	} {
		if _, err := tx.ExecContext(ctx, query, pipelineID); err != nil {
			return fmt.Errorf("reset pipeline %q: %w", pipelineID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reset pipeline %q: %w", pipelineID, err)
	}
	return nil
}

func (r *SQLiteRuntime) SourceRecords(ctx context.Context, pipelineID string) ([]Envelope[json.RawMessage], error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		FROM mf_source_records
		WHERE pipeline_id = ?
		ORDER BY id ASC
	`, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("query source records pipeline=%q: %w", pipelineID, err)
	}
	defer rows.Close()

	var items []Envelope[json.RawMessage]
	for rows.Next() {
		item, err := scanRawEnvelope(rows)
		if err != nil {
			return nil, fmt.Errorf("scan source record pipeline=%q: %w", pipelineID, err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate source records pipeline=%q: %w", pipelineID, err)
	}
	return items, nil
}

func (r *SQLiteRuntime) PendingWork(ctx context.Context, pipelineID string) ([]PendingSegmentWork, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT
			next_segment_id,
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		FROM mf_pending_segment_work
		WHERE pipeline_id = ?
		ORDER BY id ASC
	`, pipelineID)
	if err != nil {
		return nil, fmt.Errorf("query pending work pipeline=%q: %w", pipelineID, err)
	}
	defer rows.Close()

	var items []PendingSegmentWork
	for rows.Next() {
		var nextSegmentID SegmentID
		item, err := scanRawEnvelopeWithPrefix(rows, &nextSegmentID)
		if err != nil {
			return nil, fmt.Errorf("scan pending work pipeline=%q: %w", pipelineID, err)
		}
		items = append(items, PendingSegmentWork{
			PipelineID:    pipelineID,
			NextSegmentID: nextSegmentID,
			Item:          item,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending work pipeline=%q: %w", pipelineID, err)
	}
	return items, nil
}

func (r *SQLiteRuntime) LoadSegmentProgress(ctx context.Context, pipelineID string, segment SegmentID, record RecordID, attempt AttemptID) (SegmentProgress, bool, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT compatibility_version, input_checksum, status, emitted_count
		FROM mf_segment_progress
		WHERE pipeline_id = ? AND segment_id = ? AND record_id = ? AND attempt_id = ?
	`, pipelineID, segment, record, attempt)

	var (
		compatibilityVersion string
		inputChecksum        string
		status               SegmentProgressStatus
		emittedCount         int
	)
	if err := row.Scan(&compatibilityVersion, &inputChecksum, &status, &emittedCount); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SegmentProgress{}, false, nil
		}
		return SegmentProgress{}, false, fmt.Errorf("load segment progress pipeline=%q segment=%q record=%q attempt=%d: %w", pipelineID, segment, record, attempt, err)
	}
	return SegmentProgress{
		PipelineID:           pipelineID,
		SegmentID:            segment,
		CompatibilityVersion: compatibilityVersion,
		RecordID:             record,
		AttemptID:            attempt,
		InputChecksum:        inputChecksum,
		Status:               status,
		EmittedCount:         emittedCount,
	}, true, nil
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

type sqliteExec interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func saveSourceResumeStateExec(ctx context.Context, exec sqliteExec, resumeState SourceResumeState) error {
	if _, err := exec.ExecContext(ctx, `
		INSERT INTO mf_source_resume_states (
			pipeline_id,
			source_cursor,
			paused
		) VALUES (?, ?, ?)
		ON CONFLICT(pipeline_id) DO UPDATE SET
			source_cursor = excluded.source_cursor,
			paused = excluded.paused
	`, resumeState.PipelineID, resumeState.SourceCursor, resumeState.Paused); err != nil {
		return fmt.Errorf("save source resume state %q: %w", resumeState.PipelineID, err)
	}

	return nil
}

func commitSourceRecordExec(ctx context.Context, exec sqliteExec, started StartedRecord) error {
	encoded, err := encodeRawEnvelope(started.Item)
	if err != nil {
		return fmt.Errorf("encode source record pipeline=%q record=%q: %w", started.PipelineID, started.Item.RecordID, err)
	}

	if _, err := exec.ExecContext(ctx, `
		INSERT INTO mf_source_records (
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
	`, started.PipelineID, started.Item.OriginRecordID, started.Item.RecordID, started.Item.AttemptID, encoded.ParentIDsJSON, encoded.SegmentPathJSON, encoded.PayloadJSON, encoded.MetadataJSON); err != nil {
		return fmt.Errorf("commit source record pipeline=%q record=%q: %w", started.PipelineID, started.Item.RecordID, err)
	}
	return nil
}

func commitPendingWorkExec(ctx context.Context, exec sqliteExec, item PendingSegmentWork) error {
	encoded, err := encodeRawEnvelope(item.Item)
	if err != nil {
		return fmt.Errorf("encode pending work pipeline=%q next_segment=%q record=%q: %w", item.PipelineID, item.NextSegmentID, item.Item.RecordID, err)
	}
	if _, err := exec.ExecContext(ctx, `
		INSERT INTO mf_pending_segment_work (
			pipeline_id,
			next_segment_id,
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, next_segment_id, record_id) DO NOTHING
	`, item.PipelineID, item.NextSegmentID, item.Item.OriginRecordID, item.Item.RecordID, item.Item.AttemptID, encoded.ParentIDsJSON, encoded.SegmentPathJSON, encoded.PayloadJSON, encoded.MetadataJSON); err != nil {
		return fmt.Errorf("commit pending work pipeline=%q next_segment=%q record=%q: %w", item.PipelineID, item.NextSegmentID, item.Item.RecordID, err)
	}
	return nil
}

func deletePendingWorkExec(ctx context.Context, exec sqliteExec, key PendingSegmentWorkKey) error {
	if _, err := exec.ExecContext(ctx, `
		DELETE FROM mf_pending_segment_work
		WHERE pipeline_id = ? AND next_segment_id = ? AND record_id = ?
	`, key.PipelineID, key.NextSegmentID, key.RecordID); err != nil {
		return fmt.Errorf("delete pending work pipeline=%q next_segment=%q record=%q: %w", key.PipelineID, key.NextSegmentID, key.RecordID, err)
	}
	return nil
}

func saveSegmentProgressExec(ctx context.Context, exec sqliteExec, progress SegmentProgress) error {
	if _, err := exec.ExecContext(ctx, `
		INSERT INTO mf_segment_progress (
			pipeline_id,
			segment_id,
			record_id,
			attempt_id,
			compatibility_version,
			input_checksum,
			status,
			emitted_count
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, segment_id, record_id, attempt_id) DO UPDATE SET
			compatibility_version = excluded.compatibility_version,
			input_checksum = excluded.input_checksum,
			status = excluded.status,
			emitted_count = excluded.emitted_count
	`, progress.PipelineID, progress.SegmentID, progress.RecordID, progress.AttemptID, progress.CompatibilityVersion, progress.InputChecksum, progress.Status, progress.EmittedCount); err != nil {
		return fmt.Errorf("save segment progress pipeline=%q segment=%q record=%q attempt=%d: %w", progress.PipelineID, progress.SegmentID, progress.RecordID, progress.AttemptID, err)
	}
	return nil
}

func commitSegmentOutputExec(ctx context.Context, exec sqliteExec, output SegmentOutputRecord) error {
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

	if _, err := exec.ExecContext(ctx, `
		INSERT INTO mf_segment_outputs (
			pipeline_id,
			segment_id,
			compatibility_version,
			input_checksum,
			origin_record_id,
			record_id,
			attempt_id,
			parent_ids_json,
			segment_path_json,
			payload_json,
			metadata_json
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, segment_id, record_id) DO UPDATE SET
			compatibility_version = excluded.compatibility_version,
			input_checksum = excluded.input_checksum,
			origin_record_id = excluded.origin_record_id,
			attempt_id = excluded.attempt_id,
			parent_ids_json = excluded.parent_ids_json,
			segment_path_json = excluded.segment_path_json,
			payload_json = excluded.payload_json,
			metadata_json = excluded.metadata_json
	`,
		output.PipelineID,
		output.SegmentID,
		output.CompatibilityVersion,
		output.InputChecksum,
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

func saveDeterministicResultExec(ctx context.Context, exec sqliteExec, result DeterministicSegmentResult) error {
	outputsJSON, err := encodeJSON(result.Outputs)
	if err != nil {
		return fmt.Errorf("encode deterministic result pipeline=%q segment=%q version=%q checksum=%q: %w", result.PipelineID, result.SegmentID, result.CompatibilityVersion, result.InputChecksum, err)
	}

	if _, err := exec.ExecContext(ctx, `
		INSERT INTO mf_deterministic_segment_results (
			pipeline_id,
			segment_id,
			compatibility_version,
			input_checksum,
			outputs_json
		) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(pipeline_id, segment_id, compatibility_version, input_checksum) DO UPDATE SET
			outputs_json = excluded.outputs_json
	`, result.PipelineID, result.SegmentID, result.CompatibilityVersion, result.InputChecksum, outputsJSON); err != nil {
		return fmt.Errorf("save deterministic result pipeline=%q segment=%q version=%q checksum=%q: %w", result.PipelineID, result.SegmentID, result.CompatibilityVersion, result.InputChecksum, err)
	}
	return nil
}

func commitTerminalExec(ctx context.Context, exec sqliteExec, terminal TerminalRecord) error {
	encoded, err := encodeRawEnvelope(terminal.Item)
	if err != nil {
		return fmt.Errorf(
			"encode terminal record pipeline=%q record=%q: %w",
			terminal.PipelineID,
			terminal.Item.RecordID,
			err,
		)
	}

	if _, err := exec.ExecContext(ctx, `
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

func scanRawEnvelopeWithPrefix(scanner interface {
	Scan(dest ...any) error
}, prefixDest ...any) (Envelope[json.RawMessage], error) {
	var (
		originRecordID  RecordID
		recordID        RecordID
		attemptID       AttemptID
		parentIDsJSON   []byte
		segmentPathJSON []byte
		payloadJSON     []byte
		metadataJSON    []byte
	)
	dest := append(prefixDest,
		&originRecordID,
		&recordID,
		&attemptID,
		&parentIDsJSON,
		&segmentPathJSON,
		&payloadJSON,
		&metadataJSON,
	)
	if err := scanner.Scan(dest...); err != nil {
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
