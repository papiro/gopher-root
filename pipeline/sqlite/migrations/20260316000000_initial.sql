CREATE TABLE IF NOT EXISTS mf_source_resume_states (
  pipeline_id text PRIMARY KEY,
  source_cursor blob NOT NULL,
  paused integer NOT NULL DEFAULT 0 CHECK (paused IN (0, 1))
);

CREATE TABLE IF NOT EXISTS mf_segment_states (
  pipeline_id text NOT NULL,
  segment_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  snapshot blob NOT NULL,
  PRIMARY KEY (pipeline_id, segment_id, record_id, attempt_id)
);

CREATE TABLE IF NOT EXISTS mf_segment_acks (
  pipeline_id text NOT NULL,
  segment_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  origin_record_id text NOT NULL,
  parent_ids_json blob NOT NULL,
  status integer NOT NULL CHECK (status IN (0, 1, 2)),
  error_message text,
  PRIMARY KEY (pipeline_id, segment_id, record_id)
);

CREATE TABLE IF NOT EXISTS mf_segment_outputs (
  id integer PRIMARY KEY AUTOINCREMENT,
  pipeline_id text NOT NULL,
  segment_id text NOT NULL,
  origin_record_id text NOT NULL,
  record_id text NOT NULL,
  attempt_id integer NOT NULL,
  parent_ids_json blob NOT NULL,
  segment_path_json blob NOT NULL,
  payload_json blob NOT NULL,
  metadata_json blob NOT NULL,
  CONSTRAINT mf_segment_outputs_pipeline_segment_record_key UNIQUE (pipeline_id, segment_id, record_id)
);

CREATE INDEX IF NOT EXISTS mf_segment_outputs_origin_idx
  ON mf_segment_outputs (pipeline_id, segment_id, origin_record_id, id);

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
