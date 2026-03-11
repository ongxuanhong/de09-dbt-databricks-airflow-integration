FROM apache/airflow:latest

# Install dependencies (Airflow packages, cosmos, etc.)
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# venv for dbt
USER airflow
RUN python -m venv /opt/airflow/dbt_venv && \
    source /opt/airflow/dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-databricks==1.10.9 && \
    deactivate