#!/usr/bin/env python3
"""
================================================================================
ИНИЦИАЛИЗАЦИЯ CLICKHOUSE: Создание RAW и MART слоёв хранилища
================================================================================

Скрипт последовательно создаёт все базы данных, таблицы и Materialized Views
для работы ETL-пайплайна CYBERPIKCHA_2077.

ПОРЯДОК ВЫПОЛНЕНИЯ:
  1. Создание базы данных raw
  2. Создание таблиц raw-слоя (raw.stores, raw.products, raw.customers, raw.purchases)
  3. Создание базы данных mart
  4. Создание таблиц mart-слоя (dim_*, fact_*, customer_features_mart)
  5. Создание Materialized Views для автоматической очистки данных

================================================================================
БЫСТРЫЙ СТАРТ
================================================================================

  # Инициализация всех таблиц (по умолчанию)
  python scripts/init_clickhouse.py

  # Инициализация с подтверждением
  python scripts/init_clickhouse.py --confirm

  # Сухой запуск (показать что будет сделано)
  python scripts/init_clickhouse.py --dry-run

  # Только RAW-слой
  python scripts/init_clickhouse.py --raw-only

  # Только MART-слой (требует существующего RAW)
  python scripts/init_clickhouse.py --mart-only

  # Удалить существующие таблицы перед созданием
  python scripts/init_clickhouse.py --drop-existing

================================================================================
КОМАНДЫ И ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ
================================================================================

1. ПОЛНАЯ ИНИЦИАЛИЗАЦИЯ
   --------------------
   Создать все базы данных и таблицы:

   python scripts/init_clickhouse.py

2. СУХОЙ ЗАПУСК
   ------------
   Показать SQL-команды без выполнения:

   python scripts/init_clickhouse.py --dry-run

3. ТОЛЬКО RAW-СЛОЙ
   ---------------
   Создать только RAW-таблицы:

   python scripts/init_clickhouse.py --raw-only

4. ТОЛЬКО MART-СЛОЙ
   ----------------
   Создать только MART-таблицы и MV:

   python scripts/init_clickhouse.py --mart-only

5. С УДАЛЕНИЕМ СУЩЕСТВУЮЩИХ ТАБЛИЦ
   --------------------------------
   Полная очистка и пересоздание:

   python scripts/init_clickhouse.py --drop-existing --confirm

6. КАСТОМНОЕ ПОДКЛЮЧЕНИЕ К CLICKHOUSE
   ----------------------------------
   Указать параметры подключения явно:

   python scripts/init_clickhouse.py \\
     --clickhouse-host 192.168.1.100 \\
     --clickhouse-port 9000 \\
     --clickhouse-user admin \\
     --clickhouse-password secret

================================================================================
ЛОГИРОВАНИЕ
================================================================================

  Лог-файл:  logs/init_clickhouse.log

================================================================================
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging
from clickhouse_driver import Client


# ============================================================================
# КОНСТАНТЫ
# ============================================================================

DEFAULT_CLICKHOUSE_HOST = "localhost"
DEFAULT_CLICKHOUSE_PORT = "9000"
DEFAULT_CLICKHOUSE_USER = "clickhouse"
DEFAULT_CLICKHOUSE_PASSWORD = "clickhouse"

SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

SQL_FILES = {
    "raw": "00_create_raw_tables.sql",
    "mart": "01_create_mart_database.sql",
    "mv": "02_create_materialized_views.sql",
    "features": "03_create_customer_features_table.sql",
}


# ============================================================================
# ФУНКЦИИ
# ============================================================================


def execute_sql_file(
    client: Client,
    file_path: Path,
    logger: logging.Logger,
    dry_run: bool = False,
) -> bool:
    """
    Выполнить SQL-файл в ClickHouse.

    Args:
        client: ClickHouse клиент
        file_path: Путь к SQL-файлу
        logger: Логгер
        dry_run: Сухой запуск (без выполнения)

    Returns:
        Успешность выполнения
    """
    if not file_path.exists():
        logger.error("❌ Файл не найден: %s", file_path)
        return False

    logger.info("📄 Чтение файла: %s", file_path.name)

    with open(file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    if dry_run:
        logger.info("  [DRY RUN] Будет выполнено %d символов SQL", len(sql_content))
        return True

    try:
        # Удаляем многострочные комментарии /* ... */
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)

        # Удаляем одиночные комментарии -- ... (до конца строки)
        sql_content = re.sub(r'--.*$', '', sql_content, flags=re.MULTILINE)

        # Выполняем SQL по разделителям
        statements = sql_content.split(";")
        executed = 0

        for statement in statements:
            statement = statement.strip()
            if statement:
                client.execute(statement)
                executed += 1

        logger.info("  ✅ Выполнено %d SQL-операций", executed)
        return True

    except Exception as exc:
        logger.error("  ❌ Ошибка выполнения: %s", exc)
        return False


def create_database(
    client: Client,
    database: str,
    logger: logging.Logger,
    drop_existing: bool = False,
    dry_run: bool = False,
) -> bool:
    """
    Создать базу данных.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        logger: Логгер
        drop_existing: Удалить существующую БД
        dry_run: Сухой запуск

    Returns:
        Успешность выполнения
    """
    try:
        if drop_existing:
            logger.info("🗑 DROP DATABASE IF EXISTS %s", database)
            if not dry_run:
                client.execute(f"DROP DATABASE IF EXISTS {database}")

        logger.info("📊 CREATE DATABASE IF NOT EXISTS %s", database)
        if not dry_run:
            client.execute(f"CREATE DATABASE IF NOT EXISTS {database}")

        return True

    except Exception as exc:
        logger.error("❌ Ошибка создания БД %s: %s", database, exc)
        return False


def get_available_tables(
    client: Client,
    database: str,
    logger: logging.Logger,
) -> list[str]:
    """
    Получить список таблиц в базе данных.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        logger: Логгер

    Returns:
        Список имён таблиц
    """
    try:
        result = client.execute(f"SHOW TABLES FROM {database}")
        return [row[0] for row in result] if result else []
    except Exception as exc:
        logger.debug("Ошибка получения списка таблиц: %s", exc)
        return []


def print_summary(
    client: Client,
    logger: logging.Logger,
) -> None:
    """
    Вывести сводку о созданных таблицах.

    Args:
        client: ClickHouse клиент
        logger: Логгер
    """
    logger.info("\n" + "=" * 70)
    logger.info("📊 ИТОГИ ИНИЦИАЛИЗАЦИИ")
    logger.info("=" * 70)

    # RAW слой
    logger.info("\n📁 RAW-слой (raw):")
    raw_tables = get_available_tables(client, "raw", logger)
    if raw_tables:
        for table in raw_tables:
            count = client.execute(f"SELECT count() FROM raw.{table}")[0][0]
            logger.info("  ✓ %s (%d строк)", table, count)
    else:
        logger.info("  (нет таблиц)")

    # MART слой
    logger.info("\n📁 MART-слой (mart):")
    mart_tables = get_available_tables(client, "mart", logger)
    if mart_tables:
        for table in mart_tables:
            try:
                count = client.execute(f"SELECT count() FROM mart.{table}")[0][0]
                logger.info("  ✓ %s (%d строк)", table, count)
            except Exception:
                logger.info("  ✓ %s (MV, без строк)", table)
    else:
        logger.info("  (нет таблиц)")

    logger.info("\n" + "=" * 70)


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================


def main() -> None:
    """Точка входа для скрипта инициализации."""
    setup_logging(log_file_name="init_clickhouse.log")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Инициализация ClickHouse: создание RAW и MART слоёв"
    )

    # ClickHouse
    parser.add_argument(
        "--clickhouse-host",
        default=DEFAULT_CLICKHOUSE_HOST,
        help=f"Хост ClickHouse (по умолчанию: {DEFAULT_CLICKHOUSE_HOST})",
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        default=int(DEFAULT_CLICKHOUSE_PORT),
        help=f"Порт ClickHouse (по умолчанию: {DEFAULT_CLICKHOUSE_PORT})",
    )
    parser.add_argument(
        "--clickhouse-user",
        default=DEFAULT_CLICKHOUSE_USER,
        help=f"Пользователь ClickHouse (по умолчанию: {DEFAULT_CLICKHOUSE_USER})",
    )
    parser.add_argument(
        "--clickhouse-password",
        default=DEFAULT_CLICKHOUSE_PASSWORD,
        help=f"Пароль ClickHouse (по умолчанию: {DEFAULT_CLICKHOUSE_PASSWORD})",
    )

    # Режимы работы
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help="Создать только RAW-слой",
    )
    parser.add_argument(
        "--mart-only",
        action="store_true",
        help="Создать только MART-слой (требует RAW)",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Удалить существующие БД перед созданием",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Запросить подтверждение перед выполнением",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Сухой запуск (показать что будет сделано)",
    )

    args = parser.parse_args()

    # Подключение к ClickHouse
    logger.info("=" * 70)
    logger.info("🏛 ПОДКЛЮЧЕНИЕ К CLICKHOUSE: %s:%s", args.clickhouse_host, args.clickhouse_port)
    logger.info("=" * 70)

    try:
        client = Client(
            host=args.clickhouse_host,
            port=args.clickhouse_port,
            user=args.clickhouse_user,
            password=args.clickhouse_password,
        )
        client.execute("SELECT 1")
        logger.info("✓ Подключение успешно")
    except Exception as exc:
        logger.error("✗ Ошибка подключения: %s", exc)
        sys.exit(1)

    # Запрос подтверждения
    if args.confirm and not args.dry_run:
        logger.warning("\n⚠️  ВНИМАНИЕ: Будут выполнены SQL-команды в ClickHouse!")
        if args.drop_existing:
            logger.warning("⚠️  Режим --drop-existing: СУЩЕСТВУЮЩИЕ ДАННЫЕ БУДУТ УДАЛЕНЫ!")
        response = input("\nПродолжить? (yes/no): ")
        if response.lower() not in ("yes", "y"):
            logger.info("❌ Отменено пользователем")
            sys.exit(0)

    # Определение режима работы
    create_raw = not args.mart_only
    create_mart = not args.raw_only

    if args.raw_only and args.mart_only:
        logger.error("❌ Нельзя указать одновременно --raw-only и --mart-only")
        sys.exit(1)

    # ========================================================================
    # СОЗДАНИЕ RAW-СЛОЯ
    # ========================================================================

    if create_raw:
        logger.info("\n" + "=" * 70)
        logger.info("📦 СОЗДАНИЕ RAW-СЛОЯ")
        logger.info("=" * 70)

        # Создание БД raw
        if not create_database(
            client, "raw", logger, args.drop_existing, args.dry_run
        ):
            logger.error("❌ Ошибка создания БД raw")
            sys.exit(1)

        # Создание таблиц raw-слоя
        raw_sql = SQL_DIR / SQL_FILES["raw"]
        if not execute_sql_file(client, raw_sql, logger, args.dry_run):
            logger.error("❌ Ошибка создания RAW-таблиц")
            sys.exit(1)

        logger.info("✅ RAW-слой успешно создан")

    # ========================================================================
    # СОЗДАНИЕ MART-СЛОЯ
    # ========================================================================

    if create_mart:
        logger.info("\n" + "=" * 70)
        logger.info("📦 СОЗДАНИЕ MART-СЛОЯ")
        logger.info("=" * 70)

        # Создание БД mart
        if not create_database(
            client, "mart", logger, args.drop_existing, args.dry_run
        ):
            logger.error("❌ Ошибка создания БД mart")
            sys.exit(1)

        # Создание таблиц mart-слоя
        mart_sql = SQL_DIR / SQL_FILES["mart"]
        if not execute_sql_file(client, mart_sql, logger, args.dry_run):
            logger.error("❌ Ошибка создания MART-таблиц")
            sys.exit(1)

        # Создание Materialized Views
        mv_sql = SQL_DIR / SQL_FILES["mv"]
        if not execute_sql_file(client, mv_sql, logger, args.dry_run):
            logger.error("❌ Ошибка создания Materialized Views")
            sys.exit(1)

        # Создание таблицы витрины признаков
        features_sql = SQL_DIR / SQL_FILES["features"]
        if not execute_sql_file(client, features_sql, logger, args.dry_run):
            logger.error("❌ Ошибка создания таблицы витрины признаков")
            sys.exit(1)

        logger.info("✅ MART-слой успешно создан")

    # ========================================================================
    # ИТОГИ
    # ========================================================================

    if not args.dry_run:
        print_summary(client, logger)

    logger.info("\n✅ Инициализация ClickHouse завершена!")

    if args.dry_run:
        logger.info("\n💡 Это был сухой запуск. Для реального выполнения уберите флаг --dry-run")


if __name__ == "__main__":
    main()
