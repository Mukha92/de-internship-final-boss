#!/usr/bin/env python3
r"""
================================================================================
ПОЛНАЯ ОЧИСТКА ДАННЫХ PIKCHA ETL
================================================================================

Скрипт принудительного удаления всех данных проекта.

КОМПОНЕНТЫ ДЛЯ ОЧИСТКИ:
  - data/         : Все папки с JSON-файлами (customers, products, purchases...)
  - MongoDB       : База данных mongo_db со всеми коллекциями
  - Kafka         : Топики (stores, customers, products, purchases)
  - ClickHouse    : Базы raw и mart со всеми таблицами и MV

================================================================================
БЫСТРЫЙ СТАРТ
================================================================================

  # Полная очистка (с подтверждением)
  python scripts/cleanup_all.py

  # Полная очистка без подтверждения (для скриптов)
  python scripts/cleanup_all.py --yes

================================================================================
КОМАНДЫ И ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ
================================================================================

1. ПОЛНАЯ ОЧИСТКА ВСЕХ КОМПОНЕНТОВ
   -------------------------------
   Удаляет data/, MongoDB, Kafka, ClickHouse (raw + mart):

   python scripts/cleanup_all.py
   python scripts/cleanup_all.py --yes  # без подтверждения

   Вывод:
     📁 Очистка директории данных: data
       ✓ Удалено: customers
       ✓ Удалено: products
       ✓ Удалено: purchases
       ✓ Удалено: stores
     🍃 Очистка MongoDB базы данных: mongo_db
       ✓ База данных 'mongo_db' удалена
     📡 Очистка Kafka топиков: ['stores', 'customers', 'products', 'purchases']
       ✓ Топик 'stores' удалён
       ...

2. ВЫБОРОЧНАЯ ОЧИСТКА (ПРОПУСК КОМПОНЕНТОВ)
   ----------------------------------------
   Пропустить очистку MongoDB:

   python scripts/cleanup_all.py --skip-mongo

   Пропустить очистку Kafka:

   python scripts/cleanup_all.py --skip-kafka

   Пропустить очистку ClickHouse:

   python scripts/cleanup_all.py --skip-clickhouse

   Пропустить очистку data/:

   python scripts/cleanup_all.py --skip-data

   Комбинация (только ClickHouse):

   python scripts/cleanup_all.py --skip-mongo --skip-kafka --skip-data

3. ОЧИСТКА ТОЛЬКО DATA ДИРЕКТОРИИ
   ------------------------------
   Удалить только JSON-файлы генерации:

   python scripts/cleanup_all.py --skip-mongo --skip-kafka --skip-clickhouse --yes
   python scripts/cleanup_all.py --skip-all --yes  # альтернатива

4. ОЧИСТКА ТОЛЬКО MONGODB
   ----------------------
   Удалить только базу MongoDB:

   python scripts/cleanup_all.py --skip-kafka --skip-clickhouse --skip-data --yes

5. ОЧИСТКА ТОЛЬКО KAFKA
   --------------------
   Удалить только топики Kafka:

   python scripts/cleanup_all.py --skip-mongo --skip-clickhouse --skip-data --yes

6. ОЧИСТКА ТОЛЬКО CLICKHOUSE
   -------------------------
   Удалить только базы ClickHouse (raw + mart):

   python scripts/cleanup_all.py --skip-mongo --skip-kafka --skip-data --yes

7. КАСТОМНЫЕ ПАРАМЕТРЫ ПОДКЛЮЧЕНИЯ
   -------------------------------
   MongoDB с другим URI:

   python scripts/cleanup_all.py --mongo-uri mongodb://192.168.1.100:27017

   ClickHouse с другим хостом:

   python scripts/cleanup_all.py \\
     --clickhouse-host 192.168.1.100 \\
     --clickhouse-port 9000 \\
     --clickhouse-user admin \\
     --clickhouse-password secret

   Указать имя базы MongoDB:

   python scripts/cleanup_all.py --mongo-database my_mongo_db

   Указать имена баз ClickHouse:

   python scripts/cleanup_all.py \\
     --clickhouse-raw-db raw_data \\
     --clickhouse-mart-db mart_data

8. КАСТОМНАЯ ДИРЕКТОРИЯ DATA
   -------------------------
   Очистить другую директорию с данными:

   python scripts/cleanup_all.py --data-dir /path/to/data --yes

9. СУХОЙ ЗАПУСК (АНАЛИЗ)
   ---------------------
   Показать что будет удалено (требуется доработка скрипта):

   python scripts/cleanup_all.py --dry-run  # TODO

================================================================================
КОМПОНЕНТЫ ДЛЯ ОЧИСТКИ
================================================================================

data/ (папки):
  - customers/   : JSON-файлы клиентов
  - products/    : JSON-файлы товаров
  - purchases/   : JSON-файлы покупок
  - stores/      : JSON-файлы магазинов
  - spark/       : Логи Spark

MongoDB (коллекции):
  - customers    : Коллекция клиентов
  - products     : Коллекция товаров
  - purchases    : Коллекция покупок
  - stores       : Коллекция магазинов

Kafka (топики):
  - stores       : Топик магазинов
  - customers    : Топик клиентов
  - products     : Топик товаров
  - purchases    : Топик покупок

ClickHouse (базы данных):
  - raw          : Сырые данные из Kafka
    * stores, products, customers, purchases
  - mart         : Витрины данных
    * dim_*, fact_*, customer_features_mart
    * Материализованные представления (mv_*)

================================================================================
ЛОГИРОВАНИЕ
================================================================================

  Лог-файл:  logs/cleanup_all.log

================================================================================
ПРЕДУПРЕЖДЕНИЯ
================================================================================

  ⚠️  Это действие НЕОБРАТИМО! Все данные будут удалены безвозвратно.
  ⚠️  Убедитесь, что вам не нужны текущие данные перед запуском.
  ⚠️  По умолчанию требуется подтверждение (yes/no).

================================================================================
АВТОМАТИЗАЦИЯ В CRON / TASK SCHEDULER
================================================================================

  # Linux (cron) - очистка перед новым запуском пайплайна
  # (добавьте в начало скрипта запуска пайплайна)
  python scripts/cleanup_all.py --yes && python scripts/run_pipeline.sh

  # Windows (PowerShell)
  python scripts/cleanup_all.py --yes; Start-Sleep -Seconds 5; python scripts/generate_data.py

  # Windows (Task Scheduler)
  schtasks /create /tn "PIKCHA Cleanup" /tr "python C:\path\scripts\cleanup_all.py --yes" /sc once /st 00:00

================================================================================
ИНТЕГРАЦИЯ С DOCKER
================================================================================

  # Очистка данных в Docker-контейнерах
  docker-compose down -v  # Удаляет volumes (mongodb_data, clickhouse-data...)
  docker-compose up -d    # Пересоздаёт контейнеры

  # Очистка через скрипт внутри контейнера
  docker-compose exec airflow-worker python scripts/cleanup_all.py --yes

================================================================================
"""

from __future__ import annotations

import argparse
import logging
import shutil
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging, get_settings
from pymongo import MongoClient
from clickhouse_driver import Client


# ============================================================================
# КОНСТАНТЫ
# ============================================================================

# Константы удалены — используем get_settings()


# ============================================================================
# ФУНКЦИИ ОЧИСТКИ
# ============================================================================


def cleanup_data_dir(
    data_dir: Path,
    data_subdirs: list[str],
    logger: logging.Logger,
) -> bool:
    """
    Удаляет все подпапки в директории data/.

    Args:
        data_dir: Путь к директории data
        data_subdirs: Список подпапок для удаления
        logger: Логгер

    Returns:
        True если успешно, False если ошибка
    """
    logger.info("📁 Очистка директории данных: %s", data_dir)

    if not data_dir.exists():
        logger.warning("Директория data не найдена: %s", data_dir)
        return True

    success = True
    for subdir in data_subdirs:
        subdir_path = data_dir / subdir
        if subdir_path.exists():
            try:
                shutil.rmtree(subdir_path)
                logger.info("  ✓ Удалено: %s", subdir)
            except Exception as exc:
                logger.error("  ✗ Ошибка удаления %s: %s", subdir, exc)
                success = False
        else:
            logger.info("  ⊘ Пропущено (не существует): %s", subdir)

    return success


def cleanup_mongo(
    mongo_uri: str,
    database: str,
    logger: logging.Logger,
) -> bool:
    """
    Удаляет MongoDB базу данных.

    Args:
        mongo_uri: URI подключения к MongoDB
        database: Имя базы данных
        logger: Логгер

    Returns:
        True если успешно, False если ошибка
    """
    logger.info("🍃 Очистка MongoDB базы данных: %s", database)

    try:
        client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        # Проверка подключения
        client.admin.command("ping")
        logger.info("  ✓ Подключение к MongoDB успешно")

        # Удаление базы данных
        client.drop_database(database)
        logger.info("  ✓ База данных '%s' удалена", database)

        client.close()
        return True

    except Exception as exc:
        logger.error("  ✗ Ошибка очистки MongoDB: %s", exc)
        return False


def cleanup_kafka_topics(
    kafka_broker: str,
    kafka_topics: list[str],
    logger: logging.Logger,
) -> bool:
    """
    Удаляет Kafka топики.

    Args:
        kafka_broker: Адрес Kafka брокера (host:port)
        kafka_topics: Список топиков для удаления
        logger: Логгер

    Returns:
        True если успешно, False если ошибка
    """
    logger.info("📡 Очистка Kafka топиков: %s", kafka_topics)

    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import UnknownTopicOrPartitionError

        # Парсим хост и порт
        host, port = kafka_broker.split(":")

        admin_client = KafkaAdminClient(
            bootstrap_servers=kafka_broker,
            client_id="cleanup_script",
            request_timeout_ms=10000,
        )
        logger.info("  ✓ Подключение к Kafka успешно")

        # Получаем список существующих топиков
        existing_topics = admin_client.list_topics()
        topics_to_delete = [
            topic for topic in kafka_topics if topic in existing_topics
        ]

        if not topics_to_delete:
            logger.info("  ⊘ Топики не найдены")
            admin_client.close()
            return True

        # Удаляем топики
        for topic in topics_to_delete:
            try:
                admin_client.delete_topics([topic])
                logger.info("  ✓ Топик '%s' удалён", topic)
            except UnknownTopicOrPartitionError:
                logger.info("  ⊘ Топик '%s' не найден", topic)
            except Exception as exc:
                logger.error("  ✗ Ошибка удаления топика '%s': %s", topic, exc)

        admin_client.close()
        return True

    except ImportError:
        logger.error("  ✗ Модуль kafka-python не установлен")
        return False
    except Exception as exc:
        logger.error("  ✗ Ошибка очистки Kafka: %s", exc)
        return False


def cleanup_clickhouse_database(
    client: Client,
    database: str,
    logger: logging.Logger,
) -> bool:
    """
    Удаляет базу данных ClickHouse со всеми таблицами и представлениями.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        logger: Логгер

    Returns:
        True если успешно, False если ошибка
    """
    logger.info("📊 Очистка ClickHouse базы данных: %s", database)

    try:
        # Проверяем существование базы данных
        databases = client.execute("SHOW DATABASES")
        db_exists = any(db[0] == database for db in databases)

        if not db_exists:
            logger.info("  ⊘ База данных '%s' не найдена", database)
            return True

        # Получаем список всех таблиц в базе данных
        tables = client.execute(
            f"SHOW TABLES FROM {database}"
        )

        if not tables:
            logger.info("  ⊘ Таблиц в базе '%s' не найдено", database)
            # Просто удаляем базу данных
            client.execute(f"DROP DATABASE IF EXISTS {database}")
            logger.info("  ✓ База данных '%s' удалена", database)
            return True

        # Сначала удаляем материализованные представления
        # (они могут блокировать удаление таблиц)
        # Примечание: MV в ClickHouse — это таблицы, используем DROP TABLE
        mv_list = client.execute(
            f"""
            SELECT name FROM system.tables
            WHERE database = '{database}'
            AND engine LIKE '%MaterializedView%'
            """
        )

        for (mv_name,) in mv_list:
            try:
                client.execute(f"DROP TABLE IF EXISTS {database}.{mv_name}")
                logger.info("  ✓ MV '%s.%s' удалено", database, mv_name)
            except Exception as exc:
                logger.error("  ✗ Ошибка удаления MV '%s': %s", mv_name, exc)

        # Затем удаляем все таблицы
        for (table_name,) in tables:
            try:
                client.execute(f"DROP TABLE IF EXISTS {database}.{table_name}")
                logger.info("  ✓ Таблица '%s.%s' удалена", database, table_name)
            except Exception as exc:
                logger.error("  ✗ Ошибка удаления таблицы '%s': %s", table_name, exc)

        # Удаляем базу данных
        client.execute(f"DROP DATABASE IF EXISTS {database}")
        logger.info("  ✓ База данных '%s' удалена", database)

        return True

    except Exception as exc:
        logger.error("  ✗ Ошибка очистки ClickHouse базы '%s': %s", database, exc)
        return False


def cleanup_clickhouse(
    host: str,
    port: int,
    user: str,
    password: str,
    raw_db: str,
    mart_db: str,
    logger: logging.Logger,
) -> bool:
    """
    Удаляет базы данных ClickHouse (raw и mart).

    Args:
        host: Хост ClickHouse
        port: Порт ClickHouse
        user: Пользователь
        password: Пароль
        raw_db: Имя raw базы данных
        mart_db: Имя mart базы данных
        logger: Логгер

    Returns:
        True если успешно, False если ошибка
    """
    logger.info("🏛 Подключение к ClickHouse: %s:%s", host, port)

    try:
        client = Client(
            host=host,
            port=port,
            user=user,
            password=password,
        )

        # Проверка подключения
        client.execute("SELECT 1")
        logger.info("  ✓ Подключение к ClickHouse успешно")

        # Очищаем обе базы данных
        raw_success = cleanup_clickhouse_database(client, raw_db, logger)
        mart_success = cleanup_clickhouse_database(client, mart_db, logger)

        client.disconnect()
        return raw_success and mart_success

    except Exception as exc:
        logger.error("  ✗ Ошибка подключения к ClickHouse: %s", exc)
        return False


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================


def main() -> None:
    """Точка входа для скрипта очистки."""
    setup_logging(log_file_name="cleanup_all.log")
    logger = logging.getLogger(__name__)
    
    # Получаем настройки из централизованной конфигурации
    settings = get_settings()

    parser = argparse.ArgumentParser(
        description="Полная очистка данных проекта PIKCHA ETL"
    )

    # MongoDB
    parser.add_argument(
        "--mongo-uri",
        "-m",
        default=None,
        help=f"URI подключения к MongoDB (по умолчанию: из .env или {settings.mongodb.uri})",
    )
    parser.add_argument(
        "--mongo-database",
        "-d",
        default=None,
        help=f"Имя MongoDB базы данных (по умолчанию: из .env или {settings.mongodb.database})",
    )

    # ClickHouse
    parser.add_argument(
        "--clickhouse-host",
        default=None,
        help=f"Хост ClickHouse (по умолчанию: из .env или {settings.clickhouse.host})",
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=None,
        help=f"Порт ClickHouse (по умолчанию: из .env или {settings.clickhouse.native_port})",
    )
    parser.add_argument(
        "--clickhouse-user",
        default=None,
        help=f"Пользователь ClickHouse (по умолчанию: из .env или {settings.clickhouse.user})",
    )
    parser.add_argument(
        "--clickhouse-password",
        default=None,
        help=f"Пароль ClickHouse (по умолчанию: из .env или {settings.clickhouse.password})",
    )
    parser.add_argument(
        "--clickhouse-raw-db",
        default=None,
        help=f"Имя raw базы данных (по умолчанию: из .env или {settings.clickhouse.raw_database})",
    )
    parser.add_argument(
        "--clickhouse-mart-db",
        default=None,
        help=f"Имя mart базы данных (по умолчанию: из .env или {settings.clickhouse.database})",
    )

    # Kafka
    parser.add_argument(
        "--kafka-broker",
        default=None,
        help=f"Адрес Kafka брокера (по умолчанию: из .env или {settings.cleanup.kafka_admin_broker})",
    )

    # Data directory
    parser.add_argument(
        "--data-dir",
        default=settings.data_dir,
        help=f"Путь к директории data (по умолчанию: {settings.data_dir})",
    )

    # Flags
    parser.add_argument(
        "--skip-mongo",
        action="store_true",
        help="Пропустить очистку MongoDB",
    )
    parser.add_argument(
        "--skip-kafka",
        action="store_true",
        help="Пропустить очистку Kafka",
    )
    parser.add_argument(
        "--skip-clickhouse",
        action="store_true",
        help="Пропустить очистку ClickHouse",
    )
    parser.add_argument(
        "--skip-data",
        action="store_true",
        help="Пропустить очистку директории data",
    )
    parser.add_argument(
        "--yes",
        "-y",
        action="store_true",
        help="Не запрашивать подтверждение",
    )

    args = parser.parse_args()

    # Применяем настройки по умолчанию из конфигурации
    mongo_uri = args.mongo_uri or settings.mongodb.uri
    mongo_database = args.mongo_database or settings.mongodb.database
    
    clickhouse_host = args.clickhouse_host or settings.clickhouse.host
    clickhouse_port = args.clickhouse_port or settings.clickhouse.native_port
    clickhouse_user = args.clickhouse_user or settings.clickhouse.user
    clickhouse_password = args.clickhouse_password or settings.clickhouse.password
    clickhouse_raw_db = args.clickhouse_raw_db or settings.clickhouse.raw_database
    clickhouse_mart_db = args.clickhouse_mart_db or settings.clickhouse.database
    
    kafka_broker = args.kafka_broker or settings.cleanup.kafka_admin_broker
    kafka_topics = settings.cleanup.kafka_topics
    data_subdirs = settings.cleanup.data_subdirs

    # Предупреждение
    logger.info("=" * 60)
    logger.info("⚠️  ВНИМАНИЕ: Полная очистка данных PIKCHA ETL")
    logger.info("=" * 60)
    logger.info("Будут удалены:")
    if not args.skip_data:
        logger.info("  • Все папки в data/: %s", ", ".join(data_subdirs))
    if not args.skip_mongo:
        logger.info("  • MongoDB база данных: %s", mongo_database)
    if not args.skip_kafka:
        logger.info("  • Kafka топики: %s", ", ".join(kafka_topics))
    if not args.skip_clickhouse:
        logger.info("  • ClickHouse базы: %s, %s", clickhouse_raw_db, clickhouse_mart_db)
    logger.info("=" * 60)

    # Запрос подтверждения
    if not args.yes:
        response = input("\nВы уверены? Это действие нельзя отменить! (yes/no): ")
        if response.lower() not in ("yes", "y"):
            logger.info("❌ Очистка отменена")
            sys.exit(0)

    logger.info("\n🚀 Начало очистки...\n")

    results = {}

    # Очистка data директории
    if args.skip_data:
        logger.info("⊘ Пропущено: очистка data/")
    else:
        data_dir = Path(args.data_dir)
        results["data"] = cleanup_data_dir(data_dir, data_subdirs, logger)

    # Очистка MongoDB
    if args.skip_mongo:
        logger.info("⊘ Пропущено: очистка MongoDB")
    else:
        results["mongo"] = cleanup_mongo(mongo_uri, mongo_database, logger)

    # Очистка Kafka
    if args.skip_kafka:
        logger.info("⊘ Пропущено: очистка Kafka")
    else:
        results["kafka"] = cleanup_kafka_topics(kafka_broker, kafka_topics, logger)

    # Очистка ClickHouse
    if args.skip_clickhouse:
        logger.info("⊘ Пропущено: очистка ClickHouse")
    else:
        results["clickhouse"] = cleanup_clickhouse(
            host=clickhouse_host,
            port=clickhouse_port,
            user=clickhouse_user,
            password=clickhouse_password,
            raw_db=clickhouse_raw_db,
            mart_db=clickhouse_mart_db,
            logger=logger,
        )

    # Итоги
    logger.info("\n" + "=" * 60)
    logger.info("📊 ИТОГИ ОЧИСТКИ")
    logger.info("=" * 60)

    all_success = all(results.values()) if results else True

    if all_success:
        logger.info("✅ Все компоненты успешно очищены!")
    else:
        logger.warning("⚠️  Очистка завершена с ошибками:")
        for component, success in results.items():
            status = "✓" if success else "✗"
            logger.warning("  %s %s", status, component)

    logger.info("=" * 60)

    sys.exit(0 if all_success else 1)


if __name__ == "__main__":
    main()
