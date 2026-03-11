from cosmos import ProfileConfig, ExecutionConfig, ProjectConfig, RenderConfig
from cosmos.profiles import DatabricksTokenProfileMapping
from cosmos.constants import TestBehavior
from pathlib import Path

DBT_EXECUTABLE_PATH = Path('/opt/airflow/dbt_venv/bin/dbt')
PROJECT_PATH = Path('/usr/local/airflow/dags/my_dbt_project')
PROFILES_PATH = Path('/usr/local/airflow/.dbt/profiles.yml')

execution_config = ExecutionConfig(
    dbt_executable_path=str(DBT_EXECUTABLE_PATH),
)

profile_config = ProfileConfig(
    profile_mapping=DatabricksTokenProfileMapping(
        conn_id='databricks_conn',
        profile_args={
            'schema': 'your_schema',
            'catalog': 'your_unity_catalog'
        }
    )
)

project_config = ProjectConfig(project_path=str(PROJECT_PATH))

render_config = RenderConfig(  # Optional: Chạy tests sau run
    test_behavior=TestBehavior.AFTER_ALL,
)