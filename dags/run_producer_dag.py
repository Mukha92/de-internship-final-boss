"""03 DAG для запуска продюсера MongoDB → Kafka."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Конфигурация DAG
DAG_ID = "03_run_mongodb_kafka_producer"
DESCRIPTION = "Чтение данных из MongoDB и отправка в Kafka"
SCHEDULE_INTERVAL = None  # Только ручной запуск
START_DATE = None
CATCHUP = False
TAGS = ["kafka", "producer", "mongodb", "pikcha"]

# Параметры для запуска продюсера
DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
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

    run_producer_task = BashOperator(
        task_id="run_producer",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_producer.py "
            "--mongo-uri mongodb://mongo_db_pikcha_airflow:27017 "
            "--mongo-db mongo_db "
            "--kafka-broker kafka:29092 "
            "--topics stores customers products purchases "
        ),
    )

    run_producer_task
