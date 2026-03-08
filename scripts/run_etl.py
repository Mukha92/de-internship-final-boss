#!/usr/bin/env python3
"""Скрипт запуска ETL-процесса для витрины клиентских признаков."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH при запуске как standalone-скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging
from src.pikcha_etl.etl import CustomerFeatureETL
from src.pikcha_etl.etl.upload_to_s3 import S3Uploader


def main() -> None:
    """
    Точка входа для запуска полного ETL-процесса.

    Управляет форматом вывода витрины, опциональной загрузкой результатов
    в S3 и обработкой аргументов командной строки.
    """
    setup_logging(log_file_name="run_etl.log")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="ETL для витрины клиентских признаков")
    parser.add_argument(
        "--output-format",
        "-f",
        choices=["clickhouse", "csv", "json", "all"],
        default="all",
        help="Формат вывода (по умолчанию: all)",
    )
    parser.add_argument(
        "--date",
        help="Дата для имени файла (формат: YYYY-MM-DD)",
    )
    parser.add_argument(
        "--upload-s3",
        action="store_true",
        help="Загрузить результаты в S3 хранилище",
    )
    parser.add_argument(
        "--s3-format",
        choices=["csv", "json", "all"],
        default="all",
        help="Формат файлов для загрузки в S3 (по умолчанию: all)",
    )
    args = parser.parse_args()

    # Парсинг даты
    target_date: datetime | None = None
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            logger.error("Неверный формат даты: %s. Используйте YYYY-MM-DD", args.date)
            return

    logger.info("Запуск ETL с форматом вывода: %s", args.output_format)

    try:
        etl = CustomerFeatureETL()
        result_paths = etl.run(output_format=args.output_format, date=target_date)

        logger.info("ETL процесс завершён успешно")

        if result_paths:
            logger.info("Сохранённые файлы:")
            for fmt, path in result_paths.items():
                logger.info("  %s: %s", fmt, path)

        # Загрузка в S3
        if args.upload_s3:
            logger.info("=" * 60)
            logger.info("ЗАГРУЗКА В S3")
            logger.info("=" * 60)

            try:
                uploader = S3Uploader()

                if args.s3_format == "all":
                    s3_results = uploader.upload_all_latest(target_date)
                    logger.info(
                        "Результаты загрузки S3: CSV=%s, JSON=%s",
                        s3_results.get("csv"),
                        s3_results.get("json"),
                    )
                elif args.s3_format == "csv":
                    success = uploader.upload_latest_csv(target_date)
                    logger.info("Загрузка CSV в S3: %s", "успешно" if success else "ошибка")
                elif args.s3_format == "json":
                    success = uploader.upload_latest_json(target_date)
                    logger.info("Загрузка JSON в S3: %s", "успешно" if success else "ошибка")

            except Exception as exc:  # noqa: BLE001
                logger.error("Ошибка загрузки в S3: %s", exc)

    except Exception as exc:  # noqa: BLE001
        logger.error("Ошибка ETL процесса: %s", exc)
        raise


if __name__ == "__main__":
    main()
