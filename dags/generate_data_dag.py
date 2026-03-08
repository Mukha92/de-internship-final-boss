"""01 DAG для генерации синтетических данных продуктового ритейла."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# Конфигурация DAG
DAG_ID = "01_generate_synthetic_data"
DESCRIPTION = "Генерация синтетических данных для продуктового ритейла"
SCHEDULE_INTERVAL = None  # Только ручной запуск
START_DATE = None
CATCHUP = False
TAGS = ["data_generation", "synthetic_data", "pikcha"]

# Параметры для генерации данных
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

    generate_data_task = BashOperator(
        task_id="generate_data",
        bash_command=(
            "cd /opt/airflow && "
            "mkdir -p /opt/airflow/data && "
            "python scripts/generate_data.py "
            "--output-dir /opt/airflow/data "
            "--num-purchases 200"
        ),
    )

    generate_data_task
