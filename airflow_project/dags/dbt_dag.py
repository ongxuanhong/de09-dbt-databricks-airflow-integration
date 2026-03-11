import sys
sys.path.insert(0, '/usr/local/airflow/')  # Để import profiles

from airflow import DAG
from cosmos import DbtDag
from include.profiles import execution_config, profile_config, project_config, render_config
from datetime import datetime

with DAG(
    dag_id='dbt_orchestration_dag',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    dbt_dag = DbtDag(
        project_config=project_config,
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
        operator_args={'install_deps': True},
    )