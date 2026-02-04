import os

from sqlalchemy import create_engine

try:
    from airflow.hooks.postgres_hook import PostgresHook
except Exception:
    PostgresHook = None


def build_db_connection():
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_host = os.getenv("POSTGRES_HOST", "host.docker.internal")
    pg_port = os.getenv("POSTGRES_PORT", "5433")
    pg_db = os.getenv("POSTGRES_DB")
    pg_sslmode = os.getenv("POSTGRES_SSLMODE")
    pg_driver = os.getenv("POSTGRES_DRIVER", "pg8000")

    if not all([pg_user, pg_password, pg_db]):
        raise RuntimeError("Missing Postgres env vars: POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB")

    conn = f"postgresql+{pg_driver}://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    if pg_sslmode and pg_driver == "psycopg2":
        conn = f"{conn}?sslmode={pg_sslmode}"
    return conn


def build_engine(logger=None):
    conn_id = os.getenv("POSTGRES_CONN_ID")
    if conn_id and PostgresHook:
        if logger:
            logger.info("Using Airflow connection: %s", conn_id)
        hook = PostgresHook(postgres_conn_id=conn_id)
        return hook.get_sqlalchemy_engine()
    return create_engine(build_db_connection())
