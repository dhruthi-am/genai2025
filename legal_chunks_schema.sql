CREATE TABLE `gen-ai-hackathon-472409.legal_analytics.chunks` (
  doc_id STRING,
  chunk_id STRING,
  chunk_text STRING,
  chunk_order INT64,
  start_pos INT64,
  end_pos INT64,
  chunk_tokens INT64,
  upload_ts TIMESTAMP,
  source STRING
) PARTITION BY DATE(upload_ts)
CLUSTER BY (doc_id);

