#!/usr/bin/env python3
"""Скрипт запуска продюсера MongoDB → Kafka."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH при запуске как standalone-скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging
from src.pikcha_etl.pipeline import MongoKafkaProducer


def main() -> None:
    """
    Точка входа для запуска продюсера MongoDB → Kafka.

    Читает данные из MongoDB, анонимизирует чувствительные поля
    и отправляет документы в топики Kafka.
    """
    setup_logging(log_file_name="run_producer.log")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Продюсер MongoDB → Kafka")
    parser.add_argument(
        "--mongo-uri",
        help="URI подключения к MongoDB",
    )
    parser.add_argument(
        "--mongo-db",
        help="Имя базы данных MongoDB",
    )
    parser.add_argument(
        "--kafka-broker",
        help="Адрес Kafka брокера",
    )
    parser.add_argument(
        "--topics",
        nargs="+",
        help="Список топиков для обработки",
    )
    parser.add_argument(
        "--hmac-key",
        help="Секретный ключ для HMAC-хеширования",
    )
    args = parser.parse_args()

    producer = MongoKafkaProducer(
        mongo_uri=args.mongo_uri,
        mongo_db=args.mongo_db,
        kafka_broker=args.kafka_broker,
        topics=args.topics,
        hmac_secret_key=args.hmac_key,
    )

    try:
        producer.connect()
        producer.run()
    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка работы продюсера: %s", exc)
        raise
    finally:
        producer.close()

    logger.info("Работа продюсера завершена")


if __name__ == "__main__":
    main()
