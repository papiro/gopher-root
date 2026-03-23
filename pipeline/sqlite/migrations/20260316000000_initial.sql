CREATE TABLE IF NOT EXISTS mf_source_resume_states (
  pipeline_id text PRIMARY KEY,
  source_cursor blob NOT NULL,
  paused integer NOT NULL DEFAULT 0 CHECK (paused IN (0, 1))
);

CREATE TABLE IF NOT EXISTS mf_segment_outputs (
  id integer PRIMARY KEY AUTOINCREMENT,
  pipeline_id text NOT NULL,
  segment_id text NOT NULL,
  origin_record_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  compatibility_version text NOT NULL DEFAULT '',
  input_checksum text NOT NULL DEFAULT '',
  parent_ids_json blob NOT NULL,
  segment_path_json blob NOT NULL,
  payload_json blob NOT NULL,
  metadata_json blob NOT NULL,
  CONSTRAINT mf_segment_outputs_pipeline_segment_record_key UNIQUE (pipeline_id, segment_id, record_id)
);

CREATE INDEX IF NOT EXISTS mf_segment_outputs_origin_idx
  ON mf_segment_outputs (pipeline_id, segment_id, origin_record_id, id);

CREATE TABLE IF NOT EXISTS mf_source_records (
  id integer PRIMARY KEY AUTOINCREMENT,
  pipeline_id text NOT NULL,
  origin_record_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  parent_ids_json blob NOT NULL,
  segment_path_json blob NOT NULL,
  payload_json blob NOT NULL,
  metadata_json blob NOT NULL,
  CONSTRAINT mf_source_records_pipeline_record_key UNIQUE (pipeline_id, record_id)
);

CREATE INDEX IF NOT EXISTS mf_source_records_pipeline_idx
  ON mf_source_records (pipeline_id, id);

CREATE TABLE IF NOT EXISTS mf_deterministic_segment_results (
  pipeline_id text NOT NULL,
  segment_id text NOT NULL,
  compatibility_version text NOT NULL,
  input_checksum text NOT NULL,
  outputs_json blob NOT NULL,
  PRIMARY KEY (pipeline_id, segment_id, compatibility_version, input_checksum)
);

CREATE TABLE IF NOT EXISTS mf_pending_segment_work (
  id integer PRIMARY KEY AUTOINCREMENT,
  pipeline_id text NOT NULL,
  next_segment_id text NOT NULL,
  origin_record_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  parent_ids_json blob NOT NULL,
  segment_path_json blob NOT NULL,
  payload_json blob NOT NULL,
  metadata_json blob NOT NULL,
  CONSTRAINT mf_pending_segment_work_pipeline_segment_record_key UNIQUE (pipeline_id, next_segment_id, record_id)
);

CREATE INDEX IF NOT EXISTS mf_pending_segment_work_pipeline_idx
  ON mf_pending_segment_work (pipeline_id, id);

CREATE TABLE IF NOT EXISTS mf_segment_progress (
  pipeline_id text NOT NULL,
  segment_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  compatibility_version text NOT NULL DEFAULT '',
  input_checksum text NOT NULL DEFAULT '',
  status integer NOT NULL CHECK (status IN (0, 1, 2, 3, 4)),
  emitted_count integer NOT NULL DEFAULT 0,
  PRIMARY KEY (pipeline_id, segment_id, record_id, attempt_id)
);

CREATE TABLE IF NOT EXISTS mf_terminal_records (
  id integer PRIMARY KEY AUTOINCREMENT,
  pipeline_id text NOT NULL,
  origin_record_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  parent_ids_json blob NOT NULL,
  segment_path_json blob NOT NULL,
  payload_json blob NOT NULL,
  metadata_json blob NOT NULL,
  CONSTRAINT mf_terminal_records_pipeline_record_key UNIQUE (pipeline_id, record_id)
);

CREATE INDEX IF NOT EXISTS mf_terminal_records_origin_idx
  ON mf_terminal_records (pipeline_id, origin_record_id, id);
