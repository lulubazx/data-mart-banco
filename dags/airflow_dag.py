from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# --- CONFIGURAÇÕES PADRÃO (ARGS) ---
default_args = {
    'owner': 'engenheiro_dados',
    'depends_on_past': False,
    'email': ['voce@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,              
    'retry_delay': timedelta(minutes=5), 
}

# --- DEFINIÇÃO DA DAG ---
# Nome: bank_data_pipeline
# Agendamento: '0 5 * * *' (Todo dia às 05:00 da manhã)
with DAG(
    'bank_data_pipeline',
    default_args=default_args,
    description='Pipeline de Ingestão e Transformação do Banco',
    schedule_interval='0 5 * * *', 
    start_date=days_ago(1),
    catchup=False,
    tags=['financeiro', 'dbt', 'producao'],
) as dag:

    # --- TAREFA 1: INGESTÃO (Extract & Load) ---
    task_ingestao = BashOperator(
        task_id='ingestao_postgres_to_bigquery',
        bash_command='python /opt/airflow/dags/dags/ingestao.py',
    )

    # --- TAREFA 2: TRANSFORMAÇÃO (DBT Run) ---
    # Roda o "dbt run" para criar as tabelas
    task_dbt_run = BashOperator(
        task_id='dbt_run_marts',
        bash_command='cd /opt/airflow/dags/bank_dbt && dbt run',
    )

    # --- TAREFA 3: TESTES (DBT Test) ---
    # Roda testes de qualidade 
    task_dbt_test = BashOperator(
        task_id='dbt_test_quality',
        bash_command='cd /opt/airflow/dags/bank_dbt && dbt test',
    )

    # --- ORQUESTRAÇÃO (A ORDEM DAS COISAS) ---
    # Aqui dizemos: Primeiro Ingestão, DEPOIS DBT Run, DEPOIS DBT Test.
    task_ingestao >> task_dbt_run >> task_dbt_test