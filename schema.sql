CREATE TABLE `gen-ai-hackathon-472409.legal_analytics.documents` (
  doc_id STRING,
  filename STRING,
  uploader STRING,
  upload_ts TIMESTAMP,
  processing_ts TIMESTAMP,
  processor_name STRING,
  language STRING,
  full_text STRING,
  raw_gcs_uri STRING
)
PARTITION BY DATE(upload_ts)
OPTIONS (description="One row per input document (normalized JSON).");
