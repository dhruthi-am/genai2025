CREATE TABLE `gen-ai-hackathon-472409.legal_analytics.metadata` (
  doc_id STRING,
  key STRING,
  value STRING,
  upload_ts TIMESTAMP
) PARTITION BY DATE(upload_ts);
