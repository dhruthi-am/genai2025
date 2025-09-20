CREATE TABLE `gen-ai-hackathon-472409.legal_analytics.entities` (
  entity_id STRING,
  doc_id STRING,
  entity_type STRING,
  mention_text STRING,
  normalized_value STRING,
  start_pos INT64,
  end_pos INT64,
  confidence FLOAT64,
  upload_ts TIMESTAMP,
  source_processor STRING
)
PARTITION BY DATE(upload_ts)
CLUSTER BY doc_id, entity_type;

