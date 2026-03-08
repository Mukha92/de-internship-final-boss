"""Пайплайн обработки данных: MongoDB → Kafka → ClickHouse."""

from src.pikcha_etl.pipeline.mongo_kafka_producer import MongoKafkaProducer
from src.pikcha_etl.pipeline.kafka_clickhouse_consumer import KafkaClickHouseConsumer

__all__ = ["MongoKafkaProducer", "KafkaClickHouseConsumer"]
