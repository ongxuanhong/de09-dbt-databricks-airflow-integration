"""Airflow DAG: schedule bronze → silver dlt pipeline."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

from data_ingestion.pipeline import run_bronze_to_silver_pipeline


with DAG(
    dag_id="bronze_to_silver_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(minutes=1),
    catchup=False,
    max_active_runs=1,
    tags=["data_ingestion", "dlt", "bronze", "silver"],
) as dag:

    @task(task_id="run_bronze_to_silver")
    def run_bronze_to_silver():
        run_bronze_to_silver_pipeline()

    run_bronze_to_silver()
