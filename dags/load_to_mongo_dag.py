"""02 DAG для загрузки данных из JSON-файлов в MongoDB."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Конфигурация DAG
DAG_ID = "02_load_data_to_mongodb"
DESCRIPTION = "Загрузка JSON-данных из файлов в MongoDB"
SCHEDULE_INTERVAL = None  # Только ручной запуск
START_DATE = None
CATCHUP = False
TAGS = ["data_loading", "mongodb", "pikcha"]

# Параметры для загрузки данных
DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 1,
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

    load_to_mongo_task = BashOperator(
        task_id="load_to_mongodb",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/load_to_mongo.py "
            "--mongo-uri mongodb://mongo_db_pikcha_airflow:27017 "
            "--data-dir /opt/airflow/data "
            "--no-clear"
        ),
    )

    load_to_mongo_task
