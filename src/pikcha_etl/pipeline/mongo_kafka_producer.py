"""
Продюсер для пайплайна MongoDB-Kafka-ClickHouse.

Считывает данные из коллекций MongoDB, обрабатывает конфиденциальную информацию
(хэширует email и телефоны) и отправляет данные в топики Kafka.
"""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

# Добавляем корень проекта в PYTHONPATH для запуска как скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from kafka import KafkaProducer
from pymongo import MongoClient

from config import get_settings
from src.pikcha_etl.types import JSONDict
from src.pikcha_etl.utils.hashing import generate_hmac_hash
from src.pikcha_etl.utils.helpers import normalize_phone, normalize_email


class MongoKafkaProducer:
    """Продюсер для чтения данных из MongoDB и отправки в Kafka."""

    # Топики с чувствительными данными (требуют хэширования)
    SENSITIVE_TOPICS = ["stores", "customers", "purchases"]

    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        mongo_db: Optional[str] = None,
        kafka_broker: Optional[str] = None,
        topics: Optional[list[str]] = None,
        hmac_secret_key: Optional[str] = None
    ):
        """
        Инициализация продюсера.

        Args:
            mongo_uri: URI подключения к MongoDB
            mongo_db: Имя базы данных MongoDB
            kafka_broker: Адрес Kafka брокера
            topics: Список топиков для обработки
            hmac_secret_key: Секретный ключ для HMAC-хеширования
        """
        settings = get_settings()
        
        self._mongo_uri = mongo_uri or settings.mongodb.uri
        self._mongo_db = mongo_db or settings.mongodb.database
        self._kafka_broker = kafka_broker or settings.kafka.broker
        self._topics = topics or settings.kafka.topics
        
        # Секретный ключ для HMAC
        key = hmac_secret_key or settings.security.hmac_secret_key
        if not key:
            raise ValueError("HMAC_SECRET_KEY must be set in environment")
        self._hmac_key = key

        self._logger = logging.getLogger(self.__class__.__name__)
        self._mongo_client: Optional[MongoClient] = None
        self._kafka_producer: Optional[KafkaProducer] = None

    def _hash_value(self, value: Any) -> str:
        """Хеширует значение с использованием HMAC-SHA256."""
        return generate_hmac_hash(value, self._hmac_key)

    def _process_data(self, data: JSONDict, topic: str) -> JSONDict:
        """
        Обрабатывает данные, хешируя чувствительные поля при необходимости.

        Args:
            data: Исходный JSON-документ MongoDB.
            topic: Имя Kafka-топика, для которого обрабатываются данные.

        Returns:
            Новый словарь с применённой нормализацией и анонимизацией.
        """
        if not isinstance(data, dict):
            return data

        result = data.copy()

        if topic in self.SENSITIVE_TOPICS:
            for key, value in data.items():
                if isinstance(value, str):
                    if 'phone' in key.lower():
                        result[key] = self._hash_value(normalize_phone(value))
                    elif 'email' in key.lower():
                        result[key] = self._hash_value(normalize_email(value))

        # Рекурсивная обработка вложенных структур
        for key, value in result.items():
            if isinstance(value, dict):
                result[key] = self._process_data(value, topic)
            elif isinstance(value, list):
                result[key] = [
                    self._process_data(item, topic) if isinstance(item, dict) else item
                    for item in value
                ]

        return result

    def connect(self) -> None:
        """Устанавливает подключения к MongoDB и Kafka."""
        self._logger.info(f"Подключение к MongoDB: {self._mongo_uri}")
        self._mongo_client = MongoClient(self._mongo_uri)

        self._logger.info(f"Подключение к Kafka: {self._kafka_broker}")
        self._kafka_producer = KafkaProducer(
            bootstrap_servers=[self._kafka_broker],
            value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode('utf-8')
        )

    def run(self) -> None:
        """Запускает процесс чтения из MongoDB и отправки в Kafka."""
        db = self._mongo_client[self._mongo_db]

        # Проверка наличия данных
        for topic in self._topics:
            count = db[topic].count_documents({})
            self._logger.info(f"Коллекция {topic}: {count} документов")

        # Обработка коллекций
        for topic in self._topics:
            count = 0
            for doc in db[topic].find({}):
                try:
                    # Преобразование ObjectId в строку
                    doc['_id'] = str(doc['_id'])

                    # Обработка данных и отправка в Kafka
                    processed_doc = self._process_data(doc, topic)

                    # Устанавливаем event_time в момент отправки каждого сообщения (UTC+3 - Москва)
                    current_time = datetime.now(timezone.utc).astimezone(
                        timezone(timedelta(hours=3))
                    ).isoformat(timespec='microseconds')

                    self._kafka_producer.send(topic, {
                        'json_data': json.dumps(processed_doc, ensure_ascii=False),
                        'event_time': current_time
                    })

                    count += 1
                    if count % 500 == 0:
                        self._logger.info(f"{topic}: обработано {count} документов")

                except Exception as e:
                    self._logger.error(f"{topic}: ошибка обработки документа: {e}")

            self._kafka_producer.flush()
            self._logger.info(f"{topic}: всего обработано {count} документов")

    def close(self) -> None:
        """Закрывает подключения к MongoDB и Kafka."""
        if self._mongo_client:
            self._mongo_client.close()
        if self._kafka_producer:
            self._kafka_producer.close()
        self._logger.info("Работа завершена")
