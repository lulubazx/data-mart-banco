import os
from datetime import datetime

import pandas as pd
from pandas.errors import EmptyDataError
from sqlalchemy.exc import SQLAlchemyError

try:
    from airflow.exceptions import AirflowException
except Exception:
    AirflowException = RuntimeError

from src.ingestion.config import load_tables_config, load_schemas_config
from src.ingestion.extractors import build_engine
from src.ingestion.transformers import convert_date_columns
from src.ingestion.loaders import BigQueryLoader
from src.utils.logging import build_logger
from src.utils.metrics import emit_metrics, write_metrics
from src.utils.validators import validate_schema, detect_rowcount_anomaly
from google.api_core.exceptions import Forbidden


def get_bigquery_config():
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    raw_dataset = os.getenv("BIGQUERY_DATASET_ID", "bank_raw")
    staging_dataset = os.getenv("BIGQUERY_STAGING_DATASET_ID", "bank_raw_staging")
    meta_dataset = os.getenv("BIGQUERY_META_DATASET_ID", "bank_meta")
    if not project_id:
        raise RuntimeError("Missing env var: BIGQUERY_PROJECT_ID")
    return project_id, raw_dataset, staging_dataset, meta_dataset


def run_pipeline():
    logger = build_logger("ingestao")
    logger.info("Iniciando Pipeline de Ingestao (Airflow/Docker)...")

    try:
        db_engine = build_engine(logger)
    except Exception:
        logger.error("Erro fatal ao configurar conexao", exc_info=True)
        return

    project_id, raw_dataset, staging_dataset, meta_dataset = get_bigquery_config()
    tables_cfg = load_tables_config()
    schemas_cfg = load_schemas_config()
    table_configs = tables_cfg.get("tables", [])

    if not table_configs:
        raise RuntimeError("No tables configured. Check config/tables.yaml")

    loader = BigQueryLoader(project_id, raw_dataset, staging_dataset, meta_dataset, logger)
    loader.ensure_datasets()

    for table_cfg in table_configs:
        table_name = table_cfg["name"]
        logger.info("Processando tabela: %s", table_name)

        try:
            extraction_start = datetime.utcnow()
            df = pd.read_sql(f"SELECT * FROM {table_name}", db_engine)
            extraction_time = (datetime.utcnow() - extraction_start).total_seconds()
            logger.info("Linhas extraidas: %s", len(df))

            schema_cfg = schemas_cfg.get(table_name, {})
            validate_schema(df, schema_cfg)
            df = convert_date_columns(df, table_cfg, logger)

            if table_cfg.get("incremental") and table_cfg.get("timestamp_column"):
                ts_col = table_cfg["timestamp_column"]
                if loader.table_exists(raw_dataset, table_name) and ts_col in df.columns:
                    last_ts = loader.get_max_timestamp(table_name, ts_col)
                    if last_ts is not None:
                        last_ts = pd.to_datetime(last_ts, utc=True)
                        df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
                        df = df[df[ts_col] > last_ts]

            loader.load_to_staging(df, table_name)

            merge_strategy = table_cfg.get("merge_strategy", "merge")

            if not loader.table_exists(raw_dataset, table_name):
                loader.create_table_from_staging(table_name)
            else:
                if merge_strategy == "replace":
                    loader.replace_from_staging(table_name)
                else:
                    try:
                        loader.merge_from_staging(table_name, table_cfg["primary_key"])
                    except Forbidden as e:
                        logger.warning("MERGE blocked (billing). Falling back to replace. table=%s", table_name)
                        loader.replace_from_staging(table_name)

            load_time = (datetime.utcnow() - extraction_start).total_seconds()
            metrics = emit_metrics(
                logger,
                project_id,
                meta_dataset,
                table_name,
                len(df),
                extraction_time,
                load_time,
            )
            try:
                write_metrics(loader.client, meta_dataset, "ingestion_metrics", metrics)
            except Forbidden:
                logger.warning("Metrics streaming blocked (billing). Skipping metrics write.")

            threshold = table_cfg.get("rowcount_warning_pct", 30)
            detect_rowcount_anomaly(
                logger,
                loader.client,
                meta_dataset,
                "ingestion_metrics",
                table_name,
                len(df),
                threshold,
            )

        except EmptyDataError:
            logger.warning("Tabela vazia, pulando: %s", table_name)
            return
        except SQLAlchemyError as e:
            logger.error("Database error on table %s", table_name, exc_info=True)
            raise AirflowException(f"Failed to process {table_name}") from e
        except Exception as e:
            logger.error("Erro ao processar %s", table_name, exc_info=True)
            raise AirflowException(f"Failed to process {table_name}") from e

    logger.info("Pipeline finalizado com sucesso!")


if __name__ == "__main__":
    run_pipeline()
