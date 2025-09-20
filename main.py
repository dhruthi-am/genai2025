import json
from datetime import datetime
from google.cloud import storage, documentai, bigquery

# === CONFIGURE YOUR PROJECT & PROCESSOR ===
PROJECT_ID = "gen-ai-hackathon-472409"
DATASET = "legal_analytics"
PROCESSOR_ID = "YOUR_PROCESSOR_ID"  # Replace with your Document AI processor ID

# === CLIENTS ===
storage_client = storage.Client()
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client()

def process_file(event, context):
    bucket_name = event['bucket']
    file_name = event['name']
    gcs_input_uri = f"gs://{bucket_name}/{file_name}"
    doc_id = file_name.split('.')[0]

    # --- Call Document AI ---
    name = docai_client.processor_path(PROJECT_ID, "us", PROCESSOR_ID)
    gcs_document = documentai.GcsDocument(gcs_uri=gcs_input_uri, mime_type="application/pdf")
    gcs_documents = documentai.GcsDocuments(documents=[gcs_document])
    input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)
    request = documentai.ProcessRequest(name=name, input_documents=input_config)
    result = docai_client.process_document(request=request)

    document = result.document

    # === 1️⃣ Insert into documents table ===
    documents_table = f"{PROJECT_ID}.{DATASET}.documents"
    doc_row = {
        "doc_id": doc_id,
        "filename": file_name,
        "uploader": "user1",
        "upload_ts": datetime.utcnow().isoformat(),
        "processing_ts": datetime.utcnow().isoformat(),
        "processor_name": PROCESSOR_ID,
        "language": "en",
        "full_text": document.text,
        "raw_gcs_uri": gcs_input_uri,
    }
    errors = bq_client.insert_rows_json(documents_table, [doc_row])
    if errors:
        print(f"BigQuery documents insert errors: {errors}")
    else:
        print(f"Inserted document {doc_id} into documents table")

    # === 2️⃣ Insert clauses ===
    clauses_table = f"{PROJECT_ID}.{DATASET}.clauses"
    clauses_rows = []
    for i, entity in enumerate(document.entities):
        # Example: using type "CLAUSE" for demonstration; adjust based on your extractor
        if entity.type_ == "CLAUSE":
            clause_row = {
                "doc_id": doc_id,
                "clause_id": f"{doc_id}_clause_{i}",
                "clause_type": entity.type_,
                "clause_text": entity.mention_text,
                "start_pos": entity.text_anchor.start_index or 0,
                "end_pos": entity.text_anchor.end_index or 0,
                "risk_flag": False,   # You can add logic to detect risks
                "risk_score": 0.0,
                "upload_ts": datetime.utcnow().isoformat(),
                "source_processor": PROCESSOR_ID,
                "ingest_id": f"{doc_id}_ingest"
            }
            clauses_rows.append(clause_row)

    if clauses_rows:
        errors = bq_client.insert_rows_json(clauses_table, clauses_rows)
        if errors:
            print(f"BigQuery clauses insert errors: {errors}")
        else:
            print(f"Inserted {len(clauses_rows)} clauses for document {doc_id}")

    # === 3️⃣ Insert entities ===
    entities_table = f"{PROJECT_ID}.{DATASET}.entities"
    entities_rows = []
    for i, entity in enumerate(document.entities):
        entity_row = {
            "entity_id": f"{doc_id}_entity_{i}",
            "doc_id": doc_id,
            "entity_type": entity.type_,
            "mention_text": entity.mention_text,
            "normalized_value": entity.normalized_value.text if entity.normalized_value else None,
            "start_pos": entity.text_anchor.start_index or 0,
            "end_pos": entity.text_anchor.end_index or 0,
            "confidence": entity.confidence,
            "upload_ts": datetime.utcnow().isoformat(),
            "source_processor": PROCESSOR_ID
        }
        entities_rows.append(entity_row)

    if entities_rows:
        errors = bq_client.insert_rows_json(entities_table, entities_rows)
        if errors:
            print(f"BigQuery entities insert errors: {errors}")
        else:
            print(f"Inserted {len(entities_rows)} entities for document {doc_id}")
