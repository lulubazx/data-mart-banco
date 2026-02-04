from google.cloud import bigquery


def validate_schema(df, schema_cfg):
    required_columns = schema_cfg.get("required_columns", [])
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def get_last_rowcount(client, dataset_id, table_id, table_name):
    query = (
        f"SELECT rows_extracted FROM `{client.project}.{dataset_id}.{table_id}` "
        f"WHERE table = @table_name ORDER BY timestamp DESC LIMIT 1"
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("table_name", "STRING", table_name)
        ]
    )
    try:
        result = client.query(query, job_config=job_config).result()
        for row in result:
            return row["rows_extracted"]
    except Exception:
        return None
    return None


def detect_rowcount_anomaly(logger, client, dataset_id, table_id, table_name, current_count, threshold_pct):
    last_count = get_last_rowcount(client, dataset_id, table_id, table_name)
    if last_count is None:
        return
    if last_count == 0:
        return
    change_pct = abs(current_count - last_count) / last_count * 100.0
    if change_pct >= threshold_pct:
        logger.warning(
            "Rowcount anomaly detected: table=%s last=%s current=%s change_pct=%.2f",
            table_name,
            last_count,
            current_count,
            change_pct,
        )
