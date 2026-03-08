"""DAG для запуска полного ETL-пайплайна (оркестрация всех DAG'ов)."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Конфигурация DAG
DAG_ID = "etl_pipeline"
DESCRIPTION = "Полный ETL-пайплайн: SQL → Data Generation → MongoDB → Kafka → ClickHouse → ETL"
SCHEDULE_INTERVAL = "0 10 * * *"  # Каждый день в 10:00
START_DATE = datetime(2026, 3, 4, 10, 0)
CATCHUP = False
TAGS = ["pipeline", "orchestration", "etl", "pikcha"]

# Параметры по умолчанию
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

    # Таска 1: Запуск SQL скриптов (создание таблиц в ClickHouse)
    trigger_sql_scripts = TriggerDagRunOperator(
        task_id="trigger_run_sql_scripts",
        trigger_dag_id="00_run_sql_scripts",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Таска 2: Генерация синтетических данных
    trigger_generate_data = TriggerDagRunOperator(
        task_id="trigger_generate_synthetic_data",
        trigger_dag_id="01_generate_synthetic_data",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Таска 3: Загрузка данных в MongoDB
    trigger_load_to_mongodb = TriggerDagRunOperator(
        task_id="trigger_load_data_to_mongodb",
        trigger_dag_id="02_load_data_to_mongodb",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Таска 4: Запуск продюсера (MongoDB → Kafka)
    trigger_producer = TriggerDagRunOperator(
        task_id="trigger_run_mongodb_kafka_producer",
        trigger_dag_id="03_run_mongodb_kafka_producer",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Таска 5: Запуск консюмера (Kafka → ClickHouse)
    trigger_consumer = TriggerDagRunOperator(
        task_id="trigger_run_kafka_clickhouse_consumer",
        trigger_dag_id="04_run_kafka_clickhouse_consumer",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Таска 6: Запуск ETL витрины клиентских признаков
    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_run_customer_feature_etl",
        trigger_dag_id="05_run_customer_feature_etl",
        wait_for_completion=True,
        poke_interval=30,
    )

    # Последовательное выполнение всех задач
    (
        trigger_sql_scripts
        >> trigger_generate_data
        >> trigger_load_to_mongodb
        >> trigger_producer
        >> trigger_consumer
        >> trigger_etl
    )
