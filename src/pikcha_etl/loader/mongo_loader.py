"""Загрузчик данных из JSON-файлов в MongoDB."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Добавляем корень проекта в PYTHONPATH для запуска как скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from pymongo import MongoClient, errors

from config import get_settings
from src.pikcha_etl.types import JSONList


class MongoDataLoader:
    """Загрузчик данных из JSON-файлов в MongoDB."""

    COLLECTION_MAPPING = {
        "stores": "stores",
        "products": "products",
        "customers": "customers",
        "purchases": "purchases",
    }

    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        database: Optional[str] = None,
        data_dir: Optional[str] = None,
    ) -> None:
        """
        Инициализация загрузчика.

        Args:
            mongo_uri: URI подключения к MongoDB
            database: Имя базы данных
            data_dir: Путь к папке с данными
        """
        settings = get_settings()
        
        self.mongo_uri = mongo_uri or settings.mongodb.uri
        self.database = database or settings.mongodb.database
        self.data_dir = Path(data_dir or settings.data_dir)
        self._client: Optional[MongoClient] = None
        self._logger = logging.getLogger(self.__class__.__name__)

        self._connect()

    def _connect(self) -> None:
        """Подключение к MongoDB."""
        try:
            self._client = MongoClient(self.mongo_uri)
            self._client.admin.command("ping")
            self._logger.info(f"Подключено к MongoDB: {self.mongo_uri}")
        except errors.ConnectionFailure as e:
            self._logger.error(f"Ошибка подключения к MongoDB: {e}")
            raise

    def close(self) -> None:
        """Закрыть соединение с MongoDB."""
        if self._client:
            self._client.close()
            self._client = None
            self._logger.info("Соединение с MongoDB закрыто.")

    def load_all(self, clear_before: bool = True) -> dict[str, int]:
        """
        Загрузить все данные из JSON-файлов в MongoDB.

        Args:
            clear_before: Очистить коллекции перед загрузкой

        Returns:
            Словарь {имя_коллекции: количество_документов}
        """
        if not self._client:
            raise RuntimeError("Нет подключения к MongoDB.")

        db = self._client[self.database]
        result = {}

        # Очистка коллекций
        if clear_before:
            for collection in self.COLLECTION_MAPPING.values():
                db[collection].delete_many({})
                self._logger.info(f"Коллекция '{collection}' очищена.")

        # Загрузка данных
        for folder, collection in self.COLLECTION_MAPPING.items():
            folder_path = self.data_dir / folder
            documents = self._load_json_files(folder_path)

            if documents:
                db[collection].insert_many(documents)
                result[collection] = len(documents)
                self._logger.info(f"В '{collection}' загружено: {len(documents)} документов")
            else:
                result[collection] = 0
                self._logger.info(f"Папка '{folder}' пуста или не существует")

        return result

    def _load_json_files(self, directory: Path) -> JSONList:
        """Загрузить все JSON-файлы из директории.

        Args:
            directory: Путь к директории с ``*.json`` файлами.

        Returns:
            Список JSON-документов, успешно считанных из файлов.
        """
        if not directory.exists():
            return []

        documents = []
        for file_path in directory.glob("*.json"):
            try:
                with open(file_path, encoding="utf-8") as f:
                    documents.append(json.load(f))
            except (json.JSONDecodeError, IOError) as e:
                self._logger.error(f"Ошибка чтения {file_path.name}: {e}")

        return documents
