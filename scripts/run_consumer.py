#!/usr/bin/env python3
"""Скрипт запуска консюмера Kafka → ClickHouse."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH при запуске как standalone-скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging
from src.pikcha_etl.pipeline import KafkaClickHouseConsumer


def main() -> None:
    """
    Точка входа для запуска консюмера Kafka → ClickHouse.

    Подписывается на указанные топики Kafka и прокидывает
    сообщения в сырые таблицы ClickHouse.
    """
    setup_logging(log_file_name="run_consumer.log")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Консюмер Kafka → ClickHouse")
    parser.add_argument(
        "--clickhouse-host",
        help="Хост ClickHouse",
    )
    parser.add_argument(
        "--clickhouse-port",
        type=int,
        help="Порт ClickHouse (native)",
    )
    parser.add_argument(
        "--clickhouse-user",
        help="Пользователь ClickHouse",
    )
    parser.add_argument(
        "--clickhouse-password",
        help="Пароль ClickHouse",
    )
    parser.add_argument(
        "--clickhouse-raw-db",
        help="База данных для сырых данных",
    )
    parser.add_argument(
        "--kafka-broker",
        help="Адрес Kafka брокера",
    )
    parser.add_argument(
        "--kafka-group",
        help="ID группы консюмеров",
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        help="Список топиков для подписки",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Обработать сообщения и выйти (режим одноразового запуска)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Таймаут для режима --once в секундах (по умолчанию: 300)",
    )
    args = parser.parse_args()

    consumer = KafkaClickHouseConsumer(
        clickhouse_host=args.clickhouse_host,
        clickhouse_port=args.clickhouse_port,
        clickhouse_user=args.clickhouse_user,
        clickhouse_password=args.clickhouse_password,
        clickhouse_raw_db=args.clickhouse_raw_db,
        kafka_broker=args.kafka_broker,
        kafka_group=args.kafka_group,
        topics=args.topics,
    )

    try:
        consumer.connect()
        consumer.run(once=args.once, timeout_seconds=args.timeout)
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка работы консюмера: %s", exc)
        raise
    finally:
        consumer.close()

    logger.info("Работа консюмера завершена")


if __name__ == "__main__":
    main()
