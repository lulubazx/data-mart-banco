import os
import pandas as pd
from sqlalchemy import create_engine
import time

# --- CONFIGURAÇÕES ---
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

def get_bigquery_config():
    project_id = os.getenv("BIGQUERY_PROJECT_ID")
    dataset_id = os.getenv("BIGQUERY_DATASET_ID", "bank_raw")
    if not project_id:
        raise RuntimeError("Missing env var: BIGQUERY_PROJECT_ID")
    return project_id, dataset_id

TABLES = ['users', 'loans', 'investments', 'card_transactions']

def run_pipeline():
    print("🚀 Iniciando Pipeline de Ingestão (Via Airflow/Docker)...")
    
    try:
        db_engine = create_engine(build_db_connection())
    except Exception as e:
        print(f"❌ Erro fatal ao configurar conexão: {e}")
        return

    project_id, dataset_id = get_bigquery_config()

    for table_name in TABLES:
        print(f"\n📦 Processando tabela: {table_name}")
        
        try:
            # 1. EXTRAÇÃO
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, db_engine)
            print(f"   -> {len(df)} linhas extraídas.")

            # --- CORREÇÃO DO ERRO DE DATA ---
            # Converte colunas de objeto/data para datetime64 (que o BigQuery aceita melhor)
            for col in df.columns:
                if df[col].dtype == 'object':
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        pass # Se não for data, deixa como está
            # --------------------------------

            # 2. CARGA
            destination_table = f"{dataset_id}.{table_name}"
            
            df.to_gbq(
                destination_table=destination_table,
                project_id=project_id,
                if_exists='replace',
                progress_bar=False
            )
            print(f"   -> Carga no BigQuery concluída: {destination_table}")
            
        except Exception as e:
            print(f"   ❌ Erro ao processar {table_name}: {e}")
            # Importante: Lançar o erro para o Airflow saber que falhou e ficar vermelho
            raise e 

    print("\n✅ Pipeline finalizado com sucesso!")

if __name__ == "__main__":
    run_pipeline()
