"""
Консюмер для пайплайна MongoDB-Kafka-ClickHouse.

Потребляет сообщения из топиков Kafka, обрабатывает данные в формате JSON,
и сохраняет их в таблицы ClickHouse с движком MergeTree.
"""

import json
import logging
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

# Добавляем корень проекта в PYTHONPATH для запуска как скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from clickhouse_driver import Client
from kafka import KafkaConsumer

from config import get_settings


class KafkaClickHouseConsumer:
    """Консюмер для чтения сообщений из Kafka и записи в ClickHouse."""

    # Словарь для сопоставления топиков и полей ID
    TOPIC_ID_MAP = {
        "products": "id",
        "customers": "customer_id",
        "purchases": "purchase_id",
        "stores": "store_id"
    }

    # Служебные поля
    SERVICE_FIELDS = {
        "json_data": "String",
        "event_time": "DateTime64(9)"
    }

    # Определение полей для каждой таблицы
    TABLE_FIELDS = {
        "stores": {
            "store_id": "String",
            "store_name": "String",
            "store_network": "String",
            "store_type_description": "String",
            "type": "String",
            "categories": "String",
            "manager_name": "String",
            "manager_phone": "String",
            "manager_email": "String",
            "location_country": "String",
            "location_city": "String",
            "location_street": "String",
            "location_house": "String",
            "location_postal_code": "String",
            "location_coordinates_latitude": "String",
            "location_coordinates_longitude": "String",
            "opening_hours_mon_fri": "String",
            "opening_hours_sat": "String",
            "opening_hours_sun": "String",
            "accepts_online_orders": "String",
            "delivery_available": "String",
            "warehouse_connected": "String",
            "last_inventory_date": "String"
        },
        "purchases": {
            "purchase_id": "String",
            "customer_customer_id": "String",
            "customer_first_name": "String",
            "customer_last_name": "String",
            "customer_email": "String",
            "customer_phone": "String",
            "customer_is_loyalty_member": "String",
            "customer_loyalty_card_number": "String",
            "store_store_id": "String",
            "store_store_name": "String",
            "store_store_network": "String",
            "store_location_country": "String",
            "store_location_city": "String",
            "store_location_street": "String",
            "store_location_house": "String",
            "store_location_postal_code": "String",
            "store_location_coordinates_latitude": "String",
            "store_location_coordinates_longitude": "String",
            "items": "Nested(product_id String, name String, category String, quantity String, unit String, price_per_unit String, total_price String, kbju_calories String, kbju_protein String, kbju_fat String, kbju_carbohydrates String, manufacturer_name String, manufacturer_country String, manufacturer_website String, manufacturer_inn String)",
            "total_amount": "String",
            "payment_method": "String",
            "is_delivery": "String",
            "delivery_address_country": "String",
            "delivery_address_city": "String",
            "delivery_address_street": "String",
            "delivery_address_house": "String",
            "delivery_address_apartment": "String",
            "delivery_address_postal_code": "String",
            "purchase_datetime": "String"
        },
        "products": {
            "id": "String",
            "name": "String",
            "group": "String",
            "description": "String",
            "kbju_calories": "String",
            "kbju_protein": "String",
            "kbju_fat": "String",
            "kbju_carbohydrates": "String",
            "price": "String",
            "unit": "String",
            "origin_country": "String",
            "expiry_days": "String",
            "is_organic": "String",
            "barcode": "String",
            "manufacturer_name": "String",
            "manufacturer_country": "String",
            "manufacturer_website": "String",
            "manufacturer_inn": "String"
        },
        "customers": {
            "customer_id": "String",
            "first_name": "String",
            "last_name": "String",
            "email": "String",
            "phone": "String",
            "birth_date": "String",
            "gender": "String",
            "registration_date": "String",
            "is_loyalty_member": "String",
            "loyalty_card_number": "String",
            "purchase_location_country": "String",
            "purchase_location_city": "String",
            "purchase_location_street": "String",
            "purchase_location_house": "String",
            "purchase_location_postal_code": "String",
            "purchase_location_coordinates_latitude": "String",
            "purchase_location_coordinates_longitude": "String",
            "delivery_address_country": "String",
            "delivery_address_city": "String",
            "delivery_address_street": "String",
            "delivery_address_house": "String",
            "delivery_address_apartment": "String",
            "delivery_address_postal_code": "String",
            "preferences_preferred_language": "String",
            "preferences_preferred_payment_method": "String",
            "preferences_receive_promotions": "String"
        }
    }

    # Отображение полей JSON на имена столбцов
    FIELD_MAPPING = {
        "products": {
            "manufacturer_name": "manufacturer.name",
            "manufacturer_country": "manufacturer.country",
            "manufacturer_website": "manufacturer.website",
            "manufacturer_inn": "manufacturer.inn",
            "kbju_calories": "kbju.calories",
            "kbju_protein": "kbju.protein",
            "kbju_fat": "kbju.fat",
            "kbju_carbohydrates": "kbju.carbohydrates"
        },
        "stores": {
            "categories": "categories",
            "manager_name": "manager.name",
            "manager_phone": "manager.phone",
            "manager_email": "manager.email",
            "location_country": "location.country",
            "location_city": "location.city",
            "location_street": "location.street",
            "location_house": "location.house",
            "location_postal_code": "location.postal_code",
            "location_coordinates_latitude": "location.coordinates.latitude",
            "location_coordinates_longitude": "location.coordinates.longitude",
            "opening_hours_mon_fri": "opening_hours.mon_fri",
            "opening_hours_sat": "opening_hours.sat",
            "opening_hours_sun": "opening_hours.sun"
        },
        "customers": {
            "purchase_location_country": "purchase_location.country",
            "purchase_location_city": "purchase_location.city",
            "purchase_location_street": "purchase_location.street",
            "purchase_location_house": "purchase_location.house",
            "purchase_location_postal_code": "purchase_location.postal_code",
            "purchase_location_coordinates_latitude": "purchase_location.coordinates.latitude",
            "purchase_location_coordinates_longitude": "purchase_location.coordinates.longitude",
            "delivery_address_country": "delivery_address.country",
            "delivery_address_city": "delivery_address.city",
            "delivery_address_street": "delivery_address.street",
            "delivery_address_house": "delivery_address.house",
            "delivery_address_apartment": "delivery_address.apartment",
            "delivery_address_postal_code": "delivery_address.postal_code",
            "preferences_preferred_language": "preferences.preferred_language",
            "preferences_preferred_payment_method": "preferences.preferred_payment_method",
            "preferences_receive_promotions": "preferences.receive_promotions"
        },
        "purchases": {
            "customer_customer_id": "customer.customer_id",
            "customer_first_name": "customer.first_name",
            "customer_last_name": "customer.last_name",
            "customer_email": "customer.email",
            "customer_phone": "customer.phone",
            "customer_is_loyalty_member": "customer.is_loyalty_member",
            "customer_loyalty_card_number": "customer.loyalty_card_number",
            "store_store_id": "store.store_id",
            "store_store_name": "store.store_name",
            "store_store_network": "store.store_network",
            "store_location_country": "store.location.country",
            "store_location_city": "store.location.city",
            "store_location_street": "store.location.street",
            "store_location_house": "store.location.house",
            "store_location_postal_code": "store.location.postal_code",
            "store_location_coordinates_latitude": "store.location.coordinates.latitude",
            "store_location_coordinates_longitude": "store.location.coordinates.longitude",
            "delivery_address_country": "delivery_address.country",
            "delivery_address_city": "delivery_address.city",
            "delivery_address_street": "delivery_address.street",
            "delivery_address_house": "delivery_address.house",
            "delivery_address_apartment": "delivery_address.apartment",
            "delivery_address_postal_code": "delivery_address.postal_code"
        }
    }

    def __init__(
        self,
        clickhouse_host: Optional[str] = None,
        clickhouse_port: Optional[int] = None,
        clickhouse_user: Optional[str] = None,
        clickhouse_password: Optional[str] = None,
        clickhouse_raw_db: Optional[str] = None,
        kafka_broker: Optional[str] = None,
        kafka_group: Optional[str] = None,
        topics: Optional[list[str]] = None
    ):
        """
        Инициализация консюмера.

        Args:
            clickhouse_host: Хост ClickHouse
            clickhouse_port: Порт ClickHouse (native)
            clickhouse_user: Пользователь ClickHouse
            clickhouse_password: Пароль ClickHouse
            clickhouse_raw_db: База данных для сырых данных
            kafka_broker: Адрес Kafka брокера
            kafka_group: ID группы консюмеров
            topics: Список топиков для подписки
        """
        settings = get_settings()
        
        self._clickhouse_host = clickhouse_host or settings.clickhouse.host
        self._clickhouse_port = clickhouse_port or settings.clickhouse.native_port
        self._clickhouse_user = clickhouse_user or settings.clickhouse.user
        self._clickhouse_password = clickhouse_password or settings.clickhouse.password
        self._clickhouse_raw_db = clickhouse_raw_db or settings.clickhouse.raw_database
        self._kafka_broker = kafka_broker or settings.kafka.broker
        self._kafka_group = kafka_group or settings.kafka.group_id
        self._topics = topics or settings.kafka.topics

        self._logger = logging.getLogger(self.__class__.__name__)
        self._ch_client: Optional[Client] = None
        self._kafka_consumer: Optional[KafkaConsumer] = None

    def _get_safe_value(self, json_obj: Any, path: str, default: str = "") -> str:
        """
        Безопасно извлекает значение из JSON по пути.

        Args:
            json_obj: JSON объект
            path: Путь к значению (например, "location.city")
            default: Значение по умолчанию

        Returns:
            Извлеченное значение или default
        """
        if not json_obj:
            return default

        # Особая обработка категорий для stores
        if path == "categories" and isinstance(json_obj.get("categories"), list):
            categories = json_obj.get("categories", [])
            return ", ".join(categories) if categories else default

        # Проверка на путь с индексом массива
        if '.' in path and any(part.isdigit() for part in path.split('.')):
            parts = path.split('.')
            current = json_obj

            try:
                for part in parts:
                    if part.isdigit():
                        idx = int(part)
                        if not isinstance(current, list) or idx >= len(current):
                            return default
                        current = current[idx]
                    elif current is None or not isinstance(current, dict) or part not in current:
                        return default
                    else:
                        current = current[part]

                if current is None:
                    return default
                elif isinstance(current, (dict, list)):
                    return json.dumps(current, ensure_ascii=False)
                else:
                    return str(current)
            except Exception:
                return default
        else:
            parts = path.split('.')
            current = json_obj

            try:
                for part in parts:
                    if current is None or not isinstance(current, dict) or part not in current:
                        return default
                    current = current[part]

                if current is None:
                    return default
                elif isinstance(current, (dict, list)):
                    if isinstance(current, list):
                        return ", ".join(str(item) for item in current)
                    return json.dumps(current, ensure_ascii=False)
                else:
                    return str(current)
            except Exception:
                return default

    def _extract_items_arrays(self, items: list) -> dict:
        """
        Извлекает массивы значений из списка товаров для Nested-структуры ClickHouse.

        Args:
            items: Список товаров из поля items

        Returns:
            Словарь с массивами для каждого поля товара
        """
        if not items or not isinstance(items, list):
            return {
                "items.product_id": [],
                "items.name": [],
                "items.category": [],
                "items.quantity": [],
                "items.unit": [],
                "items.price_per_unit": [],
                "items.total_price": [],
                "items.kbju_calories": [],
                "items.kbju_protein": [],
                "items.kbju_fat": [],
                "items.kbju_carbohydrates": [],
                "items.manufacturer_name": [],
                "items.manufacturer_country": [],
                "items.manufacturer_website": [],
                "items.manufacturer_inn": []
            }

        result = {
            "items.product_id": [],
            "items.name": [],
            "items.category": [],
            "items.quantity": [],
            "items.unit": [],
            "items.price_per_unit": [],
            "items.total_price": [],
            "items.kbju_calories": [],
            "items.kbju_protein": [],
            "items.kbju_fat": [],
            "items.kbju_carbohydrates": [],
            "items.manufacturer_name": [],
            "items.manufacturer_country": [],
            "items.manufacturer_website": [],
            "items.manufacturer_inn": []
        }

        for item in items:
            if not isinstance(item, dict):
                continue

            result["items.product_id"].append(str(item.get("product_id", "")))
            result["items.name"].append(str(item.get("name", "")))
            result["items.category"].append(str(item.get("category", "")))
            result["items.quantity"].append(str(item.get("quantity", "")))
            result["items.unit"].append(str(item.get("unit", "")))
            result["items.price_per_unit"].append(str(item.get("price_per_unit", "")))
            result["items.total_price"].append(str(item.get("total_price", "")))

            kbju = item.get("kbju", {}) or {}
            result["items.kbju_calories"].append(str(kbju.get("calories", "")))
            result["items.kbju_protein"].append(str(kbju.get("protein", "")))
            result["items.kbju_fat"].append(str(kbju.get("fat", "")))
            result["items.kbju_carbohydrates"].append(str(kbju.get("carbohydrates", "")))

            manufacturer = item.get("manufacturer", {}) or {}
            result["items.manufacturer_name"].append(str(manufacturer.get("name", "")))
            result["items.manufacturer_country"].append(str(manufacturer.get("country", "")))
            result["items.manufacturer_website"].append(str(manufacturer.get("website", "")))
            result["items.manufacturer_inn"].append(str(manufacturer.get("inn", "")))

        return result

    def _create_database(self) -> None:
        """Создает базу данных для сырых данных, если она не существует."""
        query = f"CREATE DATABASE IF NOT EXISTS {self._clickhouse_raw_db}"
        try:
            self._ch_client.execute(query)
            self._logger.info(f"База данных {self._clickhouse_raw_db} готова")
        except Exception as e:
            self._logger.error(f"Ошибка создания базы данных {self._clickhouse_raw_db}: {e}")

    def _check_table_exists(self, topic: str) -> bool:
        """
        Проверяет существование таблицы в ClickHouse.

        Args:
            topic: Имя топика (имя таблицы)

        Returns:
            True если таблица существует, иначе False
        """
        query = f"EXISTS TABLE {self._clickhouse_raw_db}.{topic}"
        try:
            result = self._ch_client.execute(query)
            return result[0][0] == 1 if result else False
        except Exception as e:
            self._logger.error(f"Ошибка проверки таблицы {topic}: {e}")
            return False

    def _process_message(self, topic: str, message_value: dict) -> None:
        """
        Обрабатывает сообщение из Kafka и вставляет данные в ClickHouse.

        Args:
            topic: Имя топика
            message_value: Значение сообщения
        """
        try:
            json_data_string = message_value.get('json_data')
            if not json_data_string:
                self._logger.error(f"{topic}: отсутствует поле json_data")
                return

            try:
                parsed_json = json.loads(json_data_string)
            except json.JSONDecodeError as e:
                self._logger.error(f"{topic}: ошибка парсинга JSON: {e}")
                return

            event_time_str = message_value.get('event_time')
            if not event_time_str:
                event_time_str = datetime.now(timezone.utc).isoformat()

            # Парсим время с часовым поясом
            event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))

            # Конвертируем в московское время (UTC+3) для консистентного хранения в ClickHouse
            if event_time.tzinfo is not None:
                event_time = event_time.astimezone(timezone(timedelta(hours=3))).replace(tzinfo=None)

            data_dict = {
                "json_data": json_data_string,
                "event_time": event_time
            }

            if topic in self.TABLE_FIELDS:
                for field_name in self.TABLE_FIELDS[topic].keys():
                    if field_name == "items":
                        continue

                    json_path = field_name
                    if topic in self.FIELD_MAPPING and field_name in self.FIELD_MAPPING[topic]:
                        json_path = self.FIELD_MAPPING[topic][field_name]
                    data_dict[field_name] = self._get_safe_value(parsed_json, json_path, "")

                if topic == "purchases":
                    items_arrays = self._extract_items_arrays(parsed_json.get("items", []))
                    data_dict.update(items_arrays)

            fields = ", ".join(data_dict.keys())
            insert_sql = f"INSERT INTO {self._clickhouse_raw_db}.{topic} ({fields}) VALUES"
            values = [list(data_dict.values())]

            self._ch_client.execute(insert_sql, values)

            id_field = self.TOPIC_ID_MAP.get(topic, f"{topic}_id")
            id_value = self._get_safe_value(parsed_json, id_field, "?")
            self._logger.info(f"{topic}: сохранено, {id_field}={id_value}")

        except Exception as e:
            self._logger.error(f"{topic}: ошибка обработки: {e}")
            self._logger.debug(traceback.format_exc())

    def connect(self) -> None:
        """Устанавливает подключения к ClickHouse и Kafka."""
        self._logger.info(f"Подключение к ClickHouse: {self._clickhouse_host}")
        self._ch_client = Client(
            host=self._clickhouse_host,
            port=self._clickhouse_port,
            user=self._clickhouse_user,
            password=self._clickhouse_password,
            settings={'use_client_time_zone': True}
        )

        self._logger.info(f"Подключение к Kafka: {self._kafka_broker}")
        self._kafka_consumer = KafkaConsumer(
            *self._topics,
            bootstrap_servers=[self._kafka_broker],
            group_id=self._kafka_group,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self._logger.info(f"Consumer group: {self._kafka_group}, auto_offset_reset: earliest")

    def run(self, once: bool = False, timeout_seconds: int = 300) -> None:
        """
        Запускает основной цикл обработки сообщений.

        Args:
            once: Если True, прочитать все сообщения и выйти
            timeout_seconds: Таймаут для режима once (максимальное время работы)
        """
        self._logger.info(f"Запуск consumer для топиков: {', '.join(self._topics)}")

        self._create_database()

        missing_tables = []
        for topic in self._topics:
            if not self._check_table_exists(topic):
                missing_tables.append(topic)

        if missing_tables:
            self._logger.error(
                f"Таблицы не найдены: {', '.join(missing_tables)}. "
                f"Выполните sql/00_create_raw_tables.sql перед запуском consumer."
            )
            return

        self._logger.info("Все таблицы готовы к работе")

        try:
            if once:
                # Режим одноразового чтения: используем итератор с таймаутом
                import time
                from kafka import TopicPartition, KafkaConsumer
                
                self._logger.info("Режим --once: чтение всех доступных сообщений")
                
                # Создаём временного консюмера без group_id для чтения с начала
                temp_consumer = KafkaConsumer(
                    *self._topics,
                    bootstrap_servers=[self._kafka_broker],
                    group_id=None,  # Без consumer group - читаем с начала
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=10000  # 10 секунд таймаут
                )
                
                messages_processed = 0
                
                for message in temp_consumer:
                    self._process_message(message.topic, message.value)
                    messages_processed += 1
                    
                    if messages_processed % 100 == 0:
                        self._logger.info(f"Обработано {messages_processed} сообщений")
                
                temp_consumer.close()
                
                if messages_processed == 0:
                    self._logger.info("Режим --once: сообщений не найдено")
                else:
                    self._logger.info(f"Режим --once: всего обработано {messages_processed} сообщений")
            else:
                # Постоянный режим
                for message in self._kafka_consumer:
                    self._process_message(message.topic, message.value)
                    self._kafka_consumer.commit()

        except KeyboardInterrupt:
            self._logger.info("Остановлено пользователем")
        except Exception as e:
            self._logger.error(f"Критическая ошибка: {e}")
            self._logger.debug(traceback.format_exc())

    def close(self) -> None:
        """Закрывает подключения к ClickHouse и Kafka."""
        if self._kafka_consumer:
            self._kafka_consumer.close()
        if self._ch_client:
            self._ch_client.disconnect()
        self._logger.info("Работа завершена")
