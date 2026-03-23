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
