"""04 DAG для запуска консюмера Kafka → ClickHouse."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Конфигурация DAG
DAG_ID = "04_run_kafka_clickhouse_consumer"
DESCRIPTION = "Чтение данных из Kafka и загрузка в ClickHouse"
SCHEDULE_INTERVAL = None  # Только ручной запуск
START_DATE = None
CATCHUP = False
TAGS = ["kafka", "consumer", "clickhouse", "pikcha"]

# Параметры для запуска консюмера
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

    run_consumer_task = BashOperator(
        task_id="run_consumer",
        bash_command=(
            "cd /opt/airflow && "
            "python scripts/run_consumer.py "
            "--clickhouse-host clickhouse "
            "--clickhouse-port 9000 "
            "--clickhouse-user clickhouse "
            "--clickhouse-password clickhouse "
            "--clickhouse-raw-db raw "
            "--kafka-broker kafka:29092 "
            "--kafka-group pikcha-consumer-group "
            "--topics stores customers products purchases "
            "--once "
            "--timeout 120"
        ),
    )

    run_consumer_task
