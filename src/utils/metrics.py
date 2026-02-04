import json
from datetime import datetime

from google.cloud import bigquery


def emit_metrics(logger, project_id, dataset_id, table_name, rows_extracted, extraction_time_sec, load_time_sec):
    payload = {
        "table": table_name,
        "rows_extracted": rows_extracted,
        "extraction_time_sec": extraction_time_sec,
        "load_time_sec": load_time_sec,
        "timestamp": datetime.utcnow().isoformat(),
    }
    logger.info("metrics=%s", json.dumps(payload))
    return payload


def ensure_metrics_table(client, dataset_id, table_id):
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        client.create_dataset(bigquery.Dataset(dataset_ref))

    table_ref = dataset_ref.table(table_id)
    try:
        client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField("table", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("rows_extracted", "INTEGER"),
            bigquery.SchemaField("extraction_time_sec", "FLOAT"),
            bigquery.SchemaField("load_time_sec", "FLOAT"),
            bigquery.SchemaField("timestamp", "TIMESTAMP"),
        ]
        client.create_table(bigquery.Table(table_ref, schema=schema))


def write_metrics(client, dataset_id, table_id, metrics):
    ensure_metrics_table(client, dataset_id, table_id)
    errors = client.insert_rows_json(
        bigquery.DatasetReference(client.project, dataset_id).table(table_id),
        [metrics],
    )
    if errors:
        raise RuntimeError(f"Failed to write metrics: {errors}")
