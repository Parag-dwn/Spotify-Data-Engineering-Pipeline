from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from scripts.spotify_etl import run_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'spotify_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for Spotify streaming history',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl,
    )