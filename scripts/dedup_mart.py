#!/usr/bin/env python3
r"""
================================================================================
ДЕДУПЛИКАЦИЯ MART-СЛОЯ CLICKHOUSE
================================================================================

Утилита принудительного удаления дубликатов в таблицах mart-слоя.

СТРАТЕГИЯ: OPTIMIZE TABLE ... FINAL
  - Применяется ко всем таблицам с движком ReplacingMergeTree
  - Физически объединяет части таблиц, удаляя дубликаты по версии
  - ReplacingMergeTree не удаляет дубли автоматически — только при merge

================================================================================
БЫСТРЫЙ СТАРТ
================================================================================

  # Анализ дубликатов (без удаления)
  python scripts/dedup_mart.py --analyze-only

  # Дедупликация всех таблиц (с подтверждением)
  python scripts/dedup_mart.py

  # Дедупликация без подтверждения (для скриптов)
  python scripts/dedup_mart.py --force

================================================================================
КОМАНДЫ И ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ
================================================================================

1. АНАЛИЗ ДУБЛИКАТОВ
   -----------------
   Показать количество дубликатов во всех таблицах без удаления:

   python scripts/dedup_mart.py --analyze-only

   Вывод:
     📊 Таблица: dim_customer
       Движок: ReplacingMergeTree
       Строк: 15000
       Частей: 12
       Найдено дубликатов: 2500

2. СУХОЙ ЗАПУСК
   ------------
   Показать что будет сделано без фактического выполнения:

   python scripts/dedup_mart.py --dry-run

3. ДЕДУПЛИКАЦИЯ ВСЕХ ТАБЛИЦ
   ------------------------
   Запустить OPTIMIZE FINAL для всех таблиц из конфигурации:

   python scripts/dedup_mart.py
   python scripts/dedup_mart.py --force  # без подтверждения

4. ВЫБОРОЧНЫЕ ТАБЛИЦЫ
   ------------------
   Обработать только указанные таблицы:

   python scripts/dedup_mart.py -t dim_customer
   python scripts/dedup_mart.py -t dim_customer,dim_store,fact_purchases
   python scripts/dedup_mart.py --tables dim_product,dim_manufacturer

5. СТРАТЕГИЯ DEDUPLICATE BY KEY
   ----------------------------
   Использовать более точную дедупликацию по ключу:

   python scripts/dedup_mart.py --deduplicate-by
   python scripts/dedup_mart.py -d --tables fact_purchases

   Разница:
     - OPTIMIZE FINAL: удаляет полные дубликаты строк
     - DEDUPLICATE BY: оставляет одну строку на каждый ключ

6. СОХРАНЕНИЕ ОТЧЁТА
   -----------------
   Сохранить результаты в JSON:

   python scripts/dedup_mart.py --force --save-report
   python scripts/dedup_mart.py --save-report --report-dir output/reports

   Файл отчёта: output/dedup_report_YYYYMMDD_HHMMSS.json

7. КАСТОМНОЕ ПОДКЛЮЧЕНИЕ К CLICKHOUSE
   ----------------------------------
   Указать параметры подключения явно:

   python scripts/dedup_mart.py \\
     --clickhouse-host 192.168.1.100 \\
     --clickhouse-port 9000 \\
     --clickhouse-user admin \\
     --clickhouse-password secret

   python scripts/dedup_mart.py --clickhouse-database mart

8. КОМБИНИРОВАННЫЕ КОМАНДЫ
   -----------------------
   Анализ + отчёт:
     python scripts/dedup_mart.py --analyze-only --save-report

   Выборочные таблицы + deduplicate-by + отчёт:
     python scripts/dedup_mart.py -t fact_purchases,fact_purchase_items \\
       --deduplicate-by --save-report --force

   Сухой запуск для конкретных таблиц:
     python scripts/dedup_mart.py --dry-run -t dim_customer,dim_store

================================================================================
ТАБЛИЦЫ ДЛЯ ОБРАБОТКИ
================================================================================

Dimension tables (измерения):
  - dim_manufacturer      (ключ: manufacturer_sk)
  - dim_store_location    (ключ: store_location_sk)
  - dim_delivery_address  (ключ: delivery_address_sk)
  - dim_product           (ключ: product_sk)
  - dim_customer          (ключ: customer_sk)
  - dim_store             (ключ: store_sk)

Fact tables (факты):
  - fact_purchases        (ключ: purchase_id)
  - fact_purchase_items   (ключ: fact_sk)

Mart tables (витрины):
  - customer_features_mart (ключ: customer_sk)

Исключения (не обрабатываются):
  - dim_date (MergeTree без Replacing)

================================================================================
ЛОГИРОВАНИЕ
================================================================================

  Лог-файл:  logs/dedup_mart.log
  Отчёты:    output/dedup_report_YYYYMMDD_HHMMSS.json

================================================================================
АВТОМАТИЗАЦИЯ В CRON / TASK SCHEDULER
================================================================================

  # Linux (cron) - ежедневная дедупликация в 02:00
  0 2 * * * cd /path/to/project && .venv/bin/python scripts/dedup_mart.py --force

  # Windows (Task Scheduler)
  schtasks /create /tn "ClickHouse Dedup" /tr "python C:\path\scripts\dedup_mart.py --force" /sc daily /st 02:00

================================================================================
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

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
DEFAULT_CLICKHOUSE_DATABASE = "mart"

# Таблицы для обработки (только ReplacingMergeTree)
TABLES_CONFIG = {
    # Dimension tables
    "dim_manufacturer": {
        "dedup_key": "manufacturer_sk",
        "version_column": "updated_at",
        "description": "Справочник производителей",
    },
    "dim_store_location": {
        "dedup_key": "store_location_sk",
        "version_column": "updated_at",
        "description": "Справочник местоположений магазинов",
    },
    "dim_delivery_address": {
        "dedup_key": "delivery_address_sk",
        "version_column": "updated_at",
        "description": "Справочник адресов доставки",
    },
    "dim_product": {
        "dedup_key": "product_sk",
        "version_column": "updated_at",
        "description": "Справочник продуктов",
    },
    "dim_customer": {
        "dedup_key": "customer_sk",
        "version_column": "updated_at",
        "description": "Справочник клиентов",
    },
    "dim_store": {
        "dedup_key": "store_sk",
        "version_column": "updated_at",
        "description": "Справочник магазинов",
    },
    # Fact tables
    "fact_purchases": {
        "dedup_key": "purchase_id",
        "version_column": "processed_at",
        "description": "Факт покупок (заголовки чеков)",
    },
    "fact_purchase_items": {
        "dedup_key": "fact_sk",
        "version_column": "processed_at",
        "description": "Факт позиций покупок",
    },
    # Mart table
    "customer_features_mart": {
        "dedup_key": "customer_sk",
        "version_column": "created_at",
        "description": "Матрица признаков клиентов",
    },
    # Dimension table - date
    "dim_date": {
        "dedup_key": "date_sk",
        "version_column": "updated_at",
        "description": "Справочник дат",
    },
}

# Таблицы которые НЕ требуют дедупликации (MergeTree без Replacing)
SKIP_TABLES = []


# ============================================================================
# ТИПЫ ДАННЫХ
# ============================================================================


@dataclass
class TableStats:
    """Статистика по таблице."""

    table_name: str
    rows_before: int = 0
    rows_after: int = 0
    duplicates_removed: int = 0
    duration_seconds: float = 0.0
    success: bool = False
    error: str | None = None
    parts_before: int = 0
    parts_after: int = 0


@dataclass
class DedupReport:
    """Отчёт о дедупликации."""

    timestamp: str
    strategy: str
    clickhouse_host: str
    clickhouse_database: str
    tables: dict[str, TableStats]
    total: dict[str, Any]


# ============================================================================
# ФУНКЦИИ АНАЛИЗА
# ============================================================================


def get_table_engine(
    client: Client,
    database: str,
    table: str,
    logger: logging.Logger,
) -> str | None:
    """
    Получить движок таблицы.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        logger: Логгер

    Returns:
        Название движка или None
    """
    try:
        result = client.execute(
            f"""
            SELECT engine
            FROM system.tables
            WHERE database = '{database}' AND name = '{table}'
            """
        )
        return result[0][0] if result else None
    except Exception as exc:
        logger.debug("Ошибка получения движка таблицы %s: %s", table, exc)
        return None


def get_row_count(
    client: Client,
    database: str,
    table: str,
    logger: logging.Logger,
) -> int:
    """
    Получить количество строк в таблице.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        logger: Логгер

    Returns:
        Количество строк
    """
    try:
        result = client.execute(f"SELECT count() FROM {database}.{table}")
        return result[0][0] if result else 0
    except Exception as exc:
        logger.debug("Ошибка получения количества строк в %s: %s", table, exc)
        return 0


def get_parts_count(
    client: Client,
    database: str,
    table: str,
    logger: logging.Logger,
) -> int:
    """
    Получить количество частей таблицы.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        logger: Логгер

    Returns:
        Количество частей
    """
    try:
        result = client.execute(
            f"""
            SELECT count()
            FROM system.parts
            WHERE database = '{database}'
              AND table = '{table}'
              AND active = 1
            """
        )
        return result[0][0] if result else 0
    except Exception as exc:
        logger.debug("Ошибка получения количества частей в %s: %s", table, exc)
        return 0


def count_duplicates_by_key(
    client: Client,
    database: str,
    table: str,
    dedup_key: str,
    logger: logging.Logger,
) -> int:
    """
    Подсчитать количество дубликатов по ключу.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        dedup_key: Ключ дедупликации
        logger: Логгер

    Returns:
        Количество дубликатов
    """
    try:
        result = client.execute(
            f"""
            SELECT count() - count(DISTINCT {dedup_key}) as duplicates
            FROM {database}.{table}
            """
        )
        return result[0][0] if result else 0
    except Exception as exc:
        logger.debug("Ошибка подсчёта дубликатов в %s: %s", table, exc)
        return 0


def analyze_table(
    client: Client,
    database: str,
    table: str,
    config: dict[str, Any],
    logger: logging.Logger,
) -> dict[str, Any]:
    """
    Проанализировать таблицу на наличие дубликатов.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        config: Конфигурация таблицы
        logger: Логгер

    Returns:
        Статистика таблицы
    """
    engine = get_table_engine(client, database, table, logger)
    row_count = get_row_count(client, database, table, logger)
    parts_count = get_parts_count(client, database, table, logger)
    duplicates = count_duplicates_by_key(
        client, database, table, config["dedup_key"], logger
    )

    return {
        "table": table,
        "engine": engine,
        "rows": row_count,
        "parts": parts_count,
        "duplicates": duplicates,
        "dedup_key": config["dedup_key"],
        "version_column": config["version_column"],
    }


# ============================================================================
# ФУНКЦИИ ДЕДУПЛИКАЦИИ
# ============================================================================


def optimize_table_final(
    client: Client,
    database: str,
    table: str,
    logger: logging.Logger,
) -> tuple[bool, str]:
    """
    Выполнить OPTIMIZE TABLE ... FINAL.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        logger: Логгер

    Returns:
        (успех, сообщение)
    """
    try:
        logger.info("  → OPTIMIZE TABLE %s.%s FINAL", database, table)
        client.execute(f"OPTIMIZE TABLE {database}.{table} FINAL")
        return True, "OPTIMIZE выполнен успешно"
    except Exception as exc:
        return False, f"Ошибка OPTIMIZE: {exc}"


def get_sorting_key_columns(
    client: Client,
    database: str,
    table: str,
    logger: logging.Logger,
) -> list[str]:
    """
    Получить колонки sorting_key таблицы.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        logger: Логгер

    Returns:
        Список колонок sorting_key
    """
    try:
        result = client.execute(
            f"""
            SELECT sorting_key
            FROM system.tables
            WHERE database = '{database}' AND name = '{table}'
            """
        )
        if result and result[0][0]:
            # sorting_key возвращается как строка в формате "col1, col2"
            sorting_key_str = result[0][0]
            return [col.strip() for col in sorting_key_str.split(",")]
        return []
    except Exception as exc:
        logger.debug("Ошибка получения sorting_key для %s: %s", table, exc)
        return []


def extract_columns_from_expression(expr: str) -> list[str]:
    """
    Извлечь имена колонок из выражения ClickHouse.

    Например:
      - "toYYYYMM(raw_event_time)" -> ["raw_event_time"]
      - "col1, col2" -> ["col1", "col2"]

    Args:
        expr: Выражение ClickHouse

    Returns:
        Список имён колонок
    """
    import re

    if not expr:
        return []

    # Находим все идентификаторы внутри скобок функций
    # Ищем паттерны типа function_name(column_name)
    columns = []

    # Паттерн для извлечения идентификаторов (имена колонок)
    # Идентификаторы могут содержать буквы, цифры и подчёркивания
    pattern = r'\b([a-zA-Z_][a-zA-Z0-9_]*)\b'

    # Исключаем имена функций ClickHouse
    built_in_functions = {
        'toYYYYMM', 'toYYYY', 'toMM', 'toDD', 'toDate', 'toDateTime',
        'toUInt8', 'toUInt16', 'toUInt32', 'toUInt64',
        'toInt8', 'toInt16', 'toInt32', 'toInt64',
        'toString', 'toFixedString',
        'tuple', 'array', 'map',
        'if', 'multiIf', 'coalesce',
        'CAST', 'cast',
        'NULL', 'null',
    }

    matches = re.findall(pattern, expr)
    for match in matches:
        if match.lower() not in {f.lower() for f in built_in_functions}:
            columns.append(match)

    return columns


def get_partition_key_columns(
    client: Client,
    database: str,
    table: str,
    logger: logging.Logger,
) -> list[str]:
    """
    Получить колонки из partition_key таблицы.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        logger: Логгер

    Returns:
        Список колонок partition_key
    """
    try:
        result = client.execute(
            f"""
            SELECT partition_key
            FROM system.tables
            WHERE database = '{database}' AND name = '{table}'
            """
        )
        if result and result[0][0]:
            partition_key_str = result[0][0]
            # Извлекаем колонки из выражения (например, toYYYYMM(raw_event_time))
            return extract_columns_from_expression(partition_key_str)
        return []
    except Exception as exc:
        logger.debug("Ошибка получения partition_key для %s: %s", table, exc)
        return []


def deduplicate_by_key(
    client: Client,
    database: str,
    table: str,
    dedup_key: str,
    logger: logging.Logger,
) -> tuple[bool, str]:
    """
    Выполнить OPTIMIZE TABLE ... FINAL DEDUPLICATE BY.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        table: Имя таблицы
        dedup_key: Ключ дедупликации
        logger: Логгер

    Returns:
        (успех, сообщение)
    """
    try:
        # Получаем все колонки sorting_key для корректного DEDUPLICATE BY
        sorting_columns = get_sorting_key_columns(client, database, table, logger)
        
        # Получаем колонки из partition_key (если есть)
        partition_columns = get_partition_key_columns(client, database, table, logger)
        
        # Объединяем все колонки: sorting_key + partition_key
        all_columns = sorting_columns + partition_columns
        
        if all_columns:
            # Включаем все колонки sorting_key и partition_key в DEDUPLICATE BY
            dedup_columns = ", ".join(all_columns)
            logger.info(
                "  → OPTIMIZE TABLE %s.%s FINAL DEDUPLICATE BY %s",
                database, table, dedup_columns
            )
            client.execute(
                f"OPTIMIZE TABLE {database}.{table} FINAL DEDUPLICATE BY {dedup_columns}"
            )
        else:
            # Fallback: используем только dedup_key
            logger.info("  → OPTIMIZE TABLE %s.%s FINAL DEDUPLICATE BY %s", database, table, dedup_key)
            client.execute(
                f"OPTIMIZE TABLE {database}.{table} FINAL DEDUPLICATE BY {dedup_key}"
            )
        
        return True, "DEDUPLICATE выполнен успешно"
    except Exception as exc:
        return False, f"Ошибка DEDUPLICATE: {exc}"


# ============================================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================================


def get_available_tables(
    client: Client,
    database: str,
    logger: logging.Logger,
) -> list[str]:
    """
    Получить список доступных таблиц в базе данных.

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
        logger.error("Ошибка получения списка таблиц: %s", exc)
        return []


def run_deduplication(
    client: Client,
    database: str,
    tables_to_process: list[str],
    use_deduplicate_by: bool,
    logger: logging.Logger,
) -> dict[str, TableStats]:
    """
    Запустить дедупликацию для указанных таблиц.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        tables_to_process: Список таблиц для обработки
        use_deduplicate_by: Использовать DEDUPLICATE BY
        logger: Логгер

    Returns:
        Статистика по таблицам
    """
    results: dict[str, TableStats] = {}

    for table in tables_to_process:
        logger.info("\n📊 Обработка таблицы: %s", table)
        config = TABLES_CONFIG.get(table, {
            "dedup_key": "id",
            "version_column": "updated_at",
            "description": table,
        })

        stats = TableStats(table_name=table)

        # Статистика ДО
        stats.rows_before = get_row_count(client, database, table, logger)
        stats.parts_before = get_parts_count(client, database, table, logger)

        logger.info("  Строк до: %s, Частей: %s", stats.rows_before, stats.parts_before)

        # Дедупликация
        start_time = time.time()

        if use_deduplicate_by:
            success, msg = deduplicate_by_key(
                client, database, table, config["dedup_key"], logger
            )
        else:
            success, msg = optimize_table_final(client, database, table, logger)

        stats.duration_seconds = time.time() - start_time
        stats.success = success

        if not success:
            stats.error = msg
            logger.error("  ✗ %s", msg)
            results[table] = stats
            continue

        # Статистика ПОСЛЕ
        stats.rows_after = get_row_count(client, database, table, logger)
        stats.parts_after = get_parts_count(client, database, table, logger)
        stats.duplicates_removed = stats.rows_before - stats.rows_after

        logger.info(
            "  ✓ Строк после: %s, Частей: %s, Удалено дубликатов: %s (%.2f сек)",
            stats.rows_after,
            stats.parts_after,
            stats.duplicates_removed,
            stats.duration_seconds,
        )

        results[table] = stats

    return results


def print_analysis_report(
    client: Client,
    database: str,
    logger: logging.Logger,
) -> None:
    """
    Вывести отчёт об анализе дубликатов.

    Args:
        client: ClickHouse клиент
        database: Имя базы данных
        logger: Логгер
    """
    logger.info("\n" + "=" * 70)
    logger.info("📋 АНАЛИЗ ДУБЛИКАТОВ В MART-СЛОЕ")
    logger.info("=" * 70)

    available_tables = get_available_tables(client, database, logger)

    for table, config in TABLES_CONFIG.items():
        if table not in available_tables:
            logger.info("\n⊘ Таблица не найдена: %s", table)
            continue

        stats = analyze_table(client, database, table, config, logger)
        engine_info = f"{stats['engine']}" if stats['engine'] else "N/A"

        logger.info("\n📊 Таблица: %s", table)
        logger.info("  Описание: %s", config["description"])
        logger.info("  Движок: %s", engine_info)
        logger.info("  Строк: %s", stats["rows"])
        logger.info("  Частей: %s", stats["parts"])
        logger.info("  Ключ дедупликации: %s", stats["dedup_key"])
        logger.info("  Найдено дубликатов: %s", stats["duplicates"])

    logger.info("\n" + "=" * 70)


def print_summary(
    results: dict[str, TableStats],
    logger: logging.Logger,
) -> None:
    """
    Вывести сводный отчёт.

    Args:
        results: Результаты по таблицам
        logger: Логгер
    """
    logger.info("\n" + "=" * 70)
    logger.info("📊 ИТОГОВЫЙ ОТЧЁТ")
    logger.info("=" * 70)

    total_rows_before = 0
    total_rows_after = 0
    total_duplicates = 0
    total_duration = 0.0
    success_count = 0
    error_count = 0

    for table, stats in results.items():
        total_rows_before += stats.rows_before
        total_rows_after += stats.rows_after
        total_duplicates += stats.duplicates_removed
        total_duration += stats.duration_seconds

        if stats.success:
            success_count += 1
        else:
            error_count += 1

        status_icon = "✓" if stats.success else "✗"
        logger.info(
            "%s %s: %s → %s строк (удалено: %s, %.2f сек)",
            status_icon,
            table,
            stats.rows_before,
            stats.rows_after,
            stats.duplicates_removed,
            stats.duration_seconds,
        )

    logger.info("-" * 70)
    logger.info("Всего таблиц обработано: %d", len(results))
    logger.info("  ✓ Успешно: %d", success_count)
    logger.info("  ✗ С ошибками: %d", error_count)
    logger.info("Всего строк: %d → %d", total_rows_before, total_rows_after)
    logger.info("Всего удалено дубликатов: %d", total_duplicates)
    logger.info("Общее время выполнения: %.2f сек", total_duration)
    logger.info("=" * 70)


def save_report(
    report: DedupReport,
    output_dir: Path,
    logger: logging.Logger,
) -> Path | None:
    """
    Сохранить отчёт в JSON файл.

    Args:
        report: Отчёт
        output_dir: Директория для отчёта
        logger: Логгер

    Returns:
        Путь к файлу или None
    """
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"dedup_report_{timestamp}.json"
        filepath = output_dir / filename

        # Преобразуем dataclass в dict
        report_dict = {
            "timestamp": report.timestamp,
            "strategy": report.strategy,
            "clickhouse_host": report.clickhouse_host,
            "clickhouse_database": report.clickhouse_database,
            "tables": {
                name: {
                    "table_name": stats.table_name,
                    "rows_before": stats.rows_before,
                    "rows_after": stats.rows_after,
                    "duplicates_removed": stats.duplicates_removed,
                    "duration_seconds": stats.duration_seconds,
                    "success": stats.success,
                    "error": stats.error,
                    "parts_before": stats.parts_before,
                    "parts_after": stats.parts_after,
                }
                for name, stats in report.tables.items()
            },
            "total": report.total,
        }

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(report_dict, f, indent=2, ensure_ascii=False)

        logger.info("📄 Отчёт сохранён: %s", filepath)
        return filepath

    except Exception as exc:
        logger.error("Ошибка сохранения отчёта: %s", exc)
        return None


# ============================================================================
# ТОЧКА ВХОДА
# ============================================================================


def main() -> None:
    """Точка входа для утилиты дедупликации."""
    setup_logging(log_file_name="dedup_mart.log")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(
        description="Удаление дубликатов в mart-слое ClickHouse (OPTIMIZE FINAL)"
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
    parser.add_argument(
        "--clickhouse-database",
        default=DEFAULT_CLICKHOUSE_DATABASE,
        help=f"База данных (по умолчанию: {DEFAULT_CLICKHOUSE_DATABASE})",
    )

    # Режимы работы
    parser.add_argument(
        "--tables",
        "-t",
        help="Список таблиц через запятую (по умолчанию: все из конфига)",
    )
    parser.add_argument(
        "--analyze-only",
        "-a",
        action="store_true",
        help="Только анализ дубликатов (без удаления)",
    )
    parser.add_argument(
        "--deduplicate-by",
        "-d",
        action="store_true",
        help="Использовать DEDUPLICATE BY KEY вместо простого FINAL",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Сухой запуск (показать что будет сделано)",
    )
    parser.add_argument(
        "--force",
        "-y",
        action="store_true",
        help="Не запрашивать подтверждение",
    )
    parser.add_argument(
        "--save-report",
        "-s",
        action="store_true",
        help="Сохранить отчёт в JSON",
    )
    parser.add_argument(
        "--report-dir",
        default="output",
        help="Директория для отчётов (по умолчанию: output)",
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

    # Получение списка таблиц
    available_tables = get_available_tables(client, args.clickhouse_database, logger)

    # Определение таблиц для обработки
    if args.tables:
        tables_to_process = [t.strip() for t in args.tables.split(",")]
        # Проверка существования таблиц
        missing = [t for t in tables_to_process if t not in available_tables]
        if missing:
            logger.error("Таблицы не найдены: %s", ", ".join(missing))
            sys.exit(1)
    else:
        # Все таблицы из конфига, которые существуют
        tables_to_process = [
            t for t in TABLES_CONFIG.keys() if t in available_tables
        ]

    # Исключение таблиц которые не требуют дедупликации
    tables_to_process = [t for t in tables_to_process if t not in SKIP_TABLES]

    if not tables_to_process:
        logger.warning("Нет таблиц для обработки")
        sys.exit(0)

    logger.info("\n📋 Таблицы для обработки: %s", ", ".join(tables_to_process))

    # Анализ дубликатов
    if args.analyze_only:
        print_analysis_report(client, args.clickhouse_database, logger)
        sys.exit(0)

    # Сухой запуск
    if args.dry_run:
        logger.info("\n🔍 СУХОЙ ЗАПУСК")
        logger.info("=" * 70)
        for table in tables_to_process:
            config = TABLES_CONFIG.get(table, {})
            stats = analyze_table(client, args.clickhouse_database, table, config, logger)
            logger.info("\n📊 Таблица: %s", table)
            logger.info("  Строк: %s", stats["rows"])
            logger.info("  Частей: %s", stats["parts"])
            logger.info("  Дубликатов: %s", stats["duplicates"])
            if args.deduplicate_by:
                logger.info(
                    "  Будет выполнено: OPTIMIZE TABLE %s.%s FINAL DEDUPLICATE BY %s",
                    args.clickhouse_database,
                    table,
                    config.get("dedup_key", "key"),
                )
            else:
                logger.info(
                    "  Будет выполнено: OPTIMIZE TABLE %s.%s FINAL",
                    args.clickhouse_database,
                    table,
                )
        logger.info("\n" + "=" * 70)
        sys.exit(0)

    # Запрос подтверждения
    if not args.force:
        logger.warning("\n⚠️  ВНИМАНИЕ: Будет выполнена дедупликация таблиц")
        response = input("\nПродолжить? (yes/no): ")
        if response.lower() not in ("yes", "y"):
            logger.info("❌ Отменено")
            sys.exit(0)

    # Дедупликация
    logger.info("\n🚀 ЗАПУСК ДЕДУПЛИКАЦИИ")
    logger.info("Стратегия: %s", "DEDUPLICATE BY" if args.deduplicate_by else "OPTIMIZE FINAL")
    logger.info("=" * 70)

    results = run_deduplication(
        client=client,
        database=args.clickhouse_database,
        tables_to_process=tables_to_process,
        use_deduplicate_by=args.deduplicate_by,
        logger=logger,
    )

    # Сводный отчёт
    print_summary(results, logger)

    # Сохранение отчёта
    if args.save_report:
        total_rows_before = sum(s.rows_before for s in results.values())
        total_rows_after = sum(s.rows_after for s in results.values())
        total_duplicates = sum(s.duplicates_removed for s in results.values())
        total_duration = sum(s.duration_seconds for s in results.values())

        report = DedupReport(
            timestamp=datetime.now().isoformat(),
            strategy="deduplicate_by" if args.deduplicate_by else "optimize_final",
            clickhouse_host=args.clickhouse_host,
            clickhouse_database=args.clickhouse_database,
            tables=results,
            total={
                "rows_before": total_rows_before,
                "rows_after": total_rows_after,
                "duplicates_removed": total_duplicates,
                "duration_seconds": total_duration,
                "success_count": sum(1 for s in results.values() if s.success),
                "error_count": sum(1 for s in results.values() if not s.success),
            },
        )

        save_report(report, Path(args.report_dir), logger)

    # Закрытие подключения
    client.disconnect()

    # Код выхода
    has_errors = any(not s.success for s in results.values())
    sys.exit(1 if has_errors else 0)


if __name__ == "__main__":
    main()
