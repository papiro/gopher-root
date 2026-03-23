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
