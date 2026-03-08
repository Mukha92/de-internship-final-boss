"""00 DAG для последовательного запуска SQL скриптов в ClickHouse через PythonOperator."""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from clickhouse_driver import Client

# Конфигурация DAG
DAG_ID = "00_run_sql_scripts"
DESCRIPTION = "Последовательный запуск SQL скриптов (00 -> 01 -> 02 -> 03)"
SCHEDULE_INTERVAL = None  # Только ручной запуск
START_DATE = None
CATCHUP = False
TAGS = ["sql", "clickhouse", "ddl", "pikcha"]

# Параметры подключения к ClickHouse
DEFAULT_ARGS = {
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Путь к SQL скриптам внутри контейнера Airflow
SQL_DIR = "/opt/airflow/sql"

# ClickHouse connection params
CH_HOST = "clickhouse"
CH_USER = "clickhouse"
CH_PASSWORD = "clickhouse"
CH_DATABASE = "raw"


def run_sql_script(sql_file: str, **context):
    """Читает SQL файл и выполняет его в ClickHouse (по одному оператору)."""
    # Подключаемся БЕЗ указания базы данных — она может быть ещё не создана
    client = Client(
        host=CH_HOST,
        user=CH_USER,
        password=CH_PASSWORD,
    )

    with open(sql_file, 'r', encoding='utf-8') as f:
        sql_content = f.read()

    # Удаляем многострочные комментарии (если есть) и разбиваем по ';'
    # Собираем операторы, пропуская чистые комментарии
    statements = []
    current_statement = []

    for line in sql_content.split('\n'):
        stripped = line.strip()
        # Пропускаем строки-разделители и чистые комментарии
        if stripped.startswith('--') or stripped.startswith('='):
            continue
        current_statement.append(line)

        # Если есть точка с запятой — конец оператора
        if ';' in stripped:
            full_stmt = '\n'.join(current_statement)
            # Убираем всё после последней ';'
            full_stmt = full_stmt[:full_stmt.rfind(';')+1]
            # Удаляем inline-комментарии
            clean_stmt = '\n'.join(
                l for l in full_stmt.split('\n')
                if l.strip() and not l.strip().startswith('--')
            ).strip()
            if clean_stmt and clean_stmt != ';':
                statements.append(clean_stmt)
            current_statement = []

    # Выполняем каждый оператор
    for i, statement in enumerate(statements):
        client.execute(statement)
        print(f"Выполнен оператор {i+1}/{len(statements)}")

    print(f"Выполнен скрипт: {sql_file}")


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

    # Запуск скрипта 00: создание RAW таблиц
    run_sql_00 = PythonOperator(
        task_id="run_sql_00_create_raw_tables",
        python_callable=run_sql_script,
        op_kwargs={"sql_file": f"{SQL_DIR}/00_create_raw_tables.sql"},
    )

    # Запуск скрипта 01: создание MART базы данных
    run_sql_01 = PythonOperator(
        task_id="run_sql_01_create_mart_database",
        python_callable=run_sql_script,
        op_kwargs={"sql_file": f"{SQL_DIR}/01_create_mart_database.sql"},
    )

    # Запуск скрипта 02: создание материализованных представлений
    run_sql_02 = PythonOperator(
        task_id="run_sql_02_create_materialized_views",
        python_callable=run_sql_script,
        op_kwargs={"sql_file": f"{SQL_DIR}/02_create_materialized_views.sql"},
    )

    # Запуск скрипта 03: создание таблицы клиентских признаков
    run_sql_03 = PythonOperator(
        task_id="run_sql_03_create_customer_features_table",
        python_callable=run_sql_script,
        op_kwargs={"sql_file": f"{SQL_DIR}/03_create_customer_features_table.sql"},
    )

    # Последовательное выполнение: 00 -> 01 -> 02 -> 03
    run_sql_00 >> run_sql_01 >> run_sql_02 >> run_sql_03
