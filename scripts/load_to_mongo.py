#!/usr/bin/env python3
"""Скрипт загрузки данных из JSON-файлов в MongoDB."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH при запуске как standalone-скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging
from src.pikcha_etl.loader import MongoDataLoader


def main() -> None:
    """
    Точка входа для загрузки данных в MongoDB.

    Загружает JSON-документы из локальной директории в коллекции MongoDB,
    опционально очищая их перед загрузкой.
    """
    setup_logging(log_file_name="load_to_mongo.log")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Загрузка данных в MongoDB")
    parser.add_argument(
        "--mongo-uri",
        "-m",
        help="URI подключения к MongoDB (по умолчанию из .env)",
    )
    parser.add_argument(
        "--database",
        "-d",
        help="Имя базы данных (по умолчанию из .env)",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Путь к папке с данными (по умолчанию: data)",
    )
    parser.add_argument(
        "--no-clear",
        action="store_true",
        help="Не очищать коллекции перед загрузкой",
    )
    args = parser.parse_args()

    logger.info("Загрузка данных из директории: %s", args.data_dir)

    try:
        loader = MongoDataLoader(
            mongo_uri=args.mongo_uri,
            database=args.database,
            data_dir=args.data_dir,
        )
        result = loader.load_all(clear_before=not args.no_clear)
        loader.close()

        total = sum(result.values())
        logger.info("Загрузка завершена. Всего загружено документов: %s", total)

        for collection, count in result.items():
            logger.info("  %s: %s", collection, count)

    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка загрузки: %s", exc)
        raise


if __name__ == "__main__":
    main()
