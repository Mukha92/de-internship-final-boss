#!/usr/bin/env python3
"""Скрипт командной строки для генерации синтетических данных."""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH при запуске как standalone-скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config import setup_logging
from src.pikcha_etl.generation import GroceryDataGenerator


def main() -> None:
    """
    Точка входа для генерации синтетических данных.

    Парсит аргументы командной строки и запускает генератор
    тестовых данных продуктового ритейла.
    """
    setup_logging(log_file_name="generate_data.log")
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description="Генерация синтетических данных")
    parser.add_argument(
        "--output-dir",
        "-o",
        default="data",
        help="Директория для сохранения данных (по умолчанию: data)",
    )
    parser.add_argument(
        "--num-stores",
        type=int,
        default=None,
        help="Количество магазинов (по умолчанию: 45 = 30 + 15 по сетям)",
    )
    parser.add_argument(
        "--num-products",
        type=int,
        default=None,
        help="Количество товаров (по умолчанию: 50 = по 10 из категории)",
    )
    parser.add_argument(
        "--num-customers",
        type=int,
        default=None,
        help="Количество покупателей (по умолчанию = количеству магазинов)",
    )
    parser.add_argument(
        "--num-purchases",
        "-n",
        type=int,
        default=200,
        help="Количество покупок для генерации (по умолчанию: 200)",
    )
    args = parser.parse_args()

    logger.info("Старт генерации синтетических данных")
    logger.info("Директория вывода: %s", args.output_dir)
    logger.info("Количество магазинов: %s", args.num_stores or "45 (по умолчанию)")
    logger.info("Количество товаров: %s", args.num_products or "50 (по умолчанию)")
    logger.info("Количество покупателей: %s", args.num_customers or "= магазинам (по умолчанию)")
    logger.info("Количество покупок: %s", args.num_purchases)

    generator = GroceryDataGenerator(output_dir=args.output_dir)
    result = generator.run(
        num_stores=args.num_stores,
        num_products=args.num_products,
        num_customers=args.num_customers,
        num_purchases=args.num_purchases,
    )

    logger.info("Генерация завершена успешно")
    logger.info(f"Сгенерировано: {result}")


if __name__ == "__main__":
    main()
