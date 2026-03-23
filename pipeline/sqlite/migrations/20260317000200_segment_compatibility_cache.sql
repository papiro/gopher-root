ALTER TABLE mf_segment_states ADD COLUMN compatibility_version text NOT NULL DEFAULT '';
ALTER TABLE mf_segment_states ADD COLUMN input_checksum text NOT NULL DEFAULT '';

ALTER TABLE mf_segment_acks ADD COLUMN compatibility_version text NOT NULL DEFAULT '';
ALTER TABLE mf_segment_acks ADD COLUMN input_checksum text NOT NULL DEFAULT '';

ALTER TABLE mf_segment_outputs ADD COLUMN compatibility_version text NOT NULL DEFAULT '';
ALTER TABLE mf_segment_outputs ADD COLUMN input_checksum text NOT NULL DEFAULT '';

CREATE TABLE IF NOT EXISTS mf_deterministic_segment_results (
  pipeline_id text NOT NULL,
  segment_id text NOT NULL,
  compatibility_version text NOT NULL,
  input_checksum text NOT NULL,
  outputs_json blob NOT NULL,
  PRIMARY KEY (pipeline_id, segment_id, compatibility_version, input_checksum)
);
