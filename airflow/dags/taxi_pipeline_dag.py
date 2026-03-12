from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# ── Default settings ──────────────────────────────────────────
default_args = {
    'owner': 'john',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
}

# ── DAG definition ────────────────────────────────────────────
with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='End-to-end NYC Taxi data pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 6 * * *',   # runs every day at 6am
    catchup=False,
    tags=['nyc_taxi', 'dbt', 'portfolio']
) as dag:

    # ── Task 1: Download fresh taxi data ──────────────────────
    download_data = BashOperator(
        task_id='download_taxi_data',
        bash_command='python /opt/airflow/dbt_project/../ingestion/download_taxi_data.py'
    )

    # ── Task 2: Run dbt models ────────────────────────────────
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt_project && dbt run'
    )

    # ── Task 3: Run dbt tests ─────────────────────────────────
    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/dbt_project && dbt test'
    )

    # ── Task 4: Log completion ────────────────────────────────
    log_completion = BashOperator(
        task_id='log_completion',
        bash_command='echo "Pipeline completed successfully at $(date)"'
    )

    # ── Dependencies: run in this exact order ─────────────────
    download_data >> run_dbt_models >> run_dbt_tests >> log_completion
