CREATE TABLE `gen-ai-hackathon-472409.legal_analytics.clauses` (
  doc_id STRING,
  clause_id STRING,
  clause_type STRING,
  clause_text STRING,
  start_pos INT64,
  end_pos INT64,
  risk_flag BOOL,
  risk_score FLOAT64,
  upload_ts TIMESTAMP,
  source_processor STRING,
  ingest_id STRING
)
PARTITION BY DATE(upload_ts)
CLUSTER BY doc_id, clause_type;
