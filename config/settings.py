"""Централизованная конфигурация проекта через dataclasses."""

import os
from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


def _is_inside_docker() -> bool:
    """Проверяет, запущен ли код внутри Docker контейнера."""
    # Проверка по существованию файла /.dockerenv
    if os.path.exists("/.dockerenv"):
        return True
    # Проверка по cgroup (для некоторых систем)
    try:
        with open("/proc/1/cgroup", "rt") as f:
            return "docker" in f.read()
    except Exception:
        return False


def _get_clickhouse_host() -> str:
    """Возвращает правильный хост для ClickHouse в зависимости от окружения."""
    # Если переменная явно задана - используем её
    env_host = os.getenv("CLICKHOUSE_HOST")
    if env_host:
        return env_host
    # Внутри Docker используем имя сервиса
    if _is_inside_docker():
        return "clickhouse"
    # Локальная разработка
    return "localhost"


def _get_mongo_uri() -> str:
    """Возвращает правильный URI для MongoDB в зависимости от окружения."""
    env_uri = os.getenv("MONGO_URI")
    if env_uri:
        return env_uri
    if _is_inside_docker():
        return "mongodb://mongo_db_pikcha_airflow:27017"
    return "mongodb://localhost:27017"


def _get_kafka_broker() -> str:
    """Возвращает правильный брокер для Kafka в зависимости от окружения."""
    env_broker = os.getenv("KAFKA_BROKER")
    if env_broker:
        return env_broker
    if _is_inside_docker():
        return "kafka:29092"
    return "localhost:9092"


@dataclass
class MongoDBSettings:
    """Настройки MongoDB."""
    uri: str = field(default_factory=_get_mongo_uri)
    database: str = field(default_factory=lambda: os.getenv("MONGO_DATABASE", "mongo_db"))


@dataclass
class ClickHouseSettings:
    """Настройки ClickHouse."""
    host: str = field(default_factory=_get_clickhouse_host)
    http_port: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")))
    native_port: int = field(default_factory=lambda: int(os.getenv("CLICKHOUSE_NATIVE_PORT", "9000")))
    database: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_DATABASE", "mart"))
    raw_database: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_RAW_DB", "raw"))
    user: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "clickhouse"))
    password: str = field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"))


@dataclass
class KafkaSettings:
    """Настройки Kafka."""
    broker: str = field(default_factory=_get_kafka_broker)
    group_id: str = field(default_factory=lambda: os.getenv("KAFKA_GROUP", "pikcha-consumer-group"))
    topics: list[str] = field(default_factory=lambda: ["products", "stores", "customers", "purchases"])


@dataclass
class S3Settings:
    """Настройки S3 (опционально)."""
    enabled: bool = field(default_factory=lambda: os.getenv("S3_ENABLED", "false").lower() == "true")
    endpoint: str = field(default_factory=lambda: os.getenv("S3_ENDPOINT", ""))
    bucket: str = field(default_factory=lambda: os.getenv("S3_BUCKET", ""))
    access_key: str = field(default_factory=lambda: os.getenv("S3_ACCESS_KEY", ""))
    secret_key: str = field(default_factory=lambda: os.getenv("S3_SECRET_KEY", ""))
    region: str = field(default_factory=lambda: os.getenv("S3_REGION", "ru-7"))


@dataclass
class SecuritySettings:
    """Настройки безопасности."""
    hmac_secret_key: str = field(default_factory=lambda: os.getenv("HMAC_SECRET_KEY", ""))


@dataclass
class OutputSettings:
    """Настройки вывода."""
    output_dir: str = field(default_factory=lambda: os.getenv("OUTPUT_DIR", "output"))
    csv_filename_prefix: str = field(default_factory=lambda: os.getenv("CSV_FILENAME_PREFIX", "analytic_result"))


@dataclass
class Settings:
    """Главная конфигурация проекта."""
    mongodb: MongoDBSettings = field(default_factory=MongoDBSettings)
    clickhouse: ClickHouseSettings = field(default_factory=ClickHouseSettings)
    kafka: KafkaSettings = field(default_factory=KafkaSettings)
    s3: S3Settings = field(default_factory=S3Settings)
    security: SecuritySettings = field(default_factory=SecuritySettings)
    output: OutputSettings = field(default_factory=OutputSettings)
    
    # Дополнительные настройки
    data_dir: str = field(default_factory=lambda: os.getenv("DATA_DIR", "data"))


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Получить singleton-экземпляр конфигурации."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
