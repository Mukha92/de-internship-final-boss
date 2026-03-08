"""05 DAG для запуска ETL-процесса витрины клиентских признаков."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Конфигурация DAG
DAG_ID = "05_run_customer_feature_etl"
DESCRIPTION = "Запуск ETL для витрины клиентских признаков (ClickHouse + S3)"
SCHEDULE_INTERVAL = None  # Только ручной запуск
START_DATE = None
CATCHUP = False
TAGS = ["etl", "clickhouse", "s3", "pikcha"]

# Параметры для запуска ETL
DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=DESCRIPTION,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=START_DATE,
    catchup=CATCHUP,
    tags=TAGS,
    max_active_runs=1,
) as dag:

    run_etl_task = BashOperator(
        task_id="run_etl",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_etl.py "
            "--output-format all "
            "--upload-s3 "
            "--s3-format all "
        ),
    )

    run_etl_task
