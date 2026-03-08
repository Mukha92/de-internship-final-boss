"""
Конфигурация ETL процесса.

Содержит настройки подключения к ClickHouse, S3 и параметры витрины
клиентских признаков, используемой в Spark-пайплайне.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

# Добавляем корень проекта в PYTHONPATH для запуска как скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from config import get_settings
from src.pikcha_etl.types import StringMap


@dataclass
class ClickhouseConfig:
    """Конфигурация подключения к Clickhouse."""
    host: str = None
    http_port: int = None
    native_port: int = None
    database: str = None
    user: str = None
    password: str = None
    
    def __post_init__(self):
        """Инициализация из общих настроек."""
        settings = get_settings()
        self.host = self.host or settings.clickhouse.host
        self.http_port = self.http_port or settings.clickhouse.http_port
        self.native_port = self.native_port or settings.clickhouse.native_port
        self.database = self.database or settings.clickhouse.database
        self.user = self.user or settings.clickhouse.user
        self.password = self.password or settings.clickhouse.password
    
    @property
    def jdbc_url(self) -> str:
        """URL для подключения через JDBC."""
        return f"jdbc:clickhouse://{self.host}:{self.http_port}/{self.database}"
    
    @property
    def connection_properties(self) -> StringMap:
        """Свойства подключения для Spark."""
        return {
            "user": self.user,
            "password": self.password,
            "driver": "com.clickhouse.jdbc.ClickHouseDriver",
        }


@dataclass
class S3Config:
    """Конфигурация для работы с S3 хранилищем."""
    enabled: bool = None
    endpoint: str = None
    bucket: str = None
    access_key: str = None
    secret_key: str = None
    region: str = None
    
    def __post_init__(self):
        """Инициализация из общих настроек."""
        settings = get_settings()
        self.enabled = self.enabled or settings.s3.enabled
        self.endpoint = self.endpoint or settings.s3.endpoint
        self.bucket = self.bucket or settings.s3.bucket
        self.access_key = self.access_key or settings.s3.access_key
        self.secret_key = self.secret_key or settings.s3.secret_key
        self.region = self.region or settings.s3.region


@dataclass
class OutputConfig:
    """Конфигурация выходных файлов."""
    output_dir: str = None
    csv_filename_prefix: str = None
    
    def __post_init__(self):
        """Инициализация из общих настроек."""
        settings = get_settings()
        self.output_dir = self.output_dir or settings.output.output_dir
        self.csv_filename_prefix = self.csv_filename_prefix or settings.output.csv_filename_prefix
    
    def get_csv_filename(self, date: datetime = None) -> str:
        """Генерирует имя CSV файла в формате analytic_result_YYYY_MM_DD.csv."""
        target_date = date or datetime.now()
        return f"{self.csv_filename_prefix}_{target_date.strftime('%Y_%m_%d')}.csv"
    
    def get_json_filename(self, date: datetime = None) -> str:
        """Генерирует имя JSON файла в формате analytic_result_YYYY_MM_DD.json."""
        target_date = date or datetime.now()
        return f"{self.csv_filename_prefix}_{target_date.strftime('%Y_%m_%d')}.json"
    
    def get_csv_path(self, date: datetime = None) -> str:
        """Возвращает полный путь к CSV файлу."""
        import os
        return os.path.join(self.output_dir, self.get_csv_filename(date))

    def get_json_path(self, date: datetime = None) -> str:
        """Возвращает полный путь к JSON файлу."""
        import os
        return os.path.join(self.output_dir, self.get_json_filename(date))


@dataclass
class FeatureConfig:
    """Конфигурация признаков для расчёта."""
    # Временные окна (в днях)
    window_7d: int = 7
    window_14d: int = 14
    window_30d: int = 30
    window_90d: int = 90
    
    # Пороговые значения
    min_purchases_recurrent: int = 2
    min_purchases_loyal: int = 3
    high_cart_threshold: float = 1000.0
    low_cart_threshold: float = 200.0
    payment_threshold: float = 0.7
    weekend_threshold: float = 0.6
    single_item_threshold: float = 0.5
    min_categories_varied: int = 4
    min_items_family: int = 4
    fruit_purchases_threshold: int = 3
    recent_high_spender_threshold: float = 2000.0
    
    # Временные интервалы для шоппинга
    morning_hour_end: int = 10
    lunch_hour_start: int = 12
    lunch_hour_end: int = 15
    night_hour_start: int = 20
    
    # Категории продуктов
    dairy_category: str = "молочные продукты"
    fruits_category: str = "фрукты и ягоды"
    veggies_category: str = "овощи и зелень"
    meat_category: str = "мясо, рыба, яйца и бобовые"
    bakery_category: str = "зерновые и хлебобулочные изделия"


# Глобальные экземпляры конфигурации
clickhouse_config = ClickhouseConfig()
feature_config = FeatureConfig()
s3_config = S3Config()
output_config = OutputConfig()
