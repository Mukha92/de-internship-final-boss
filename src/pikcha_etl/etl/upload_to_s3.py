"""
Скрипт для загрузки CSV файлов в S3 хранилище.
Поддерживает различные S3-совместимые хранилища (Yandex Cloud, Selectel, AWS и др.)
"""
import argparse
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Добавляем корень проекта в PYTHONPATH для запуска как скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from src.pikcha_etl.etl.config import output_config, s3_config

# Загружаем переменные окружения из корня проекта
env_path = Path(__file__).resolve().parent.parent.parent.parent / ".env"
load_dotenv(env_path)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class S3Uploader:
    """Класс для загрузки файлов в S3 хранилище."""
    
    def __init__(self):
        """Инициализация S3 клиента."""
        self.config = s3_config
        self.client = self._create_s3_client()
    
    def _create_s3_client(self) -> boto3.client:
        """Создаёт и возвращает S3 клиент."""
        if not self.config.enabled:
            logger.warning("S3 загрузка отключена в конфигурации")
            return None
        
        # Проверяем наличие учётных данных
        if not all([self.config.access_key, self.config.secret_key]):
            raise ValueError("Отсутствуют учётные данные S3 (access_key или secret_key)")
        
        # Создаём конфигурацию для boto3
        boto_config = Config(
            region_name=self.config.region,
            signature_version='s3v4',
            retries={
                'max_attempts': 3,
                'mode': 'standard'
            }
        )
        
        # Определяем endpoint_url
        endpoint_url = self.config.endpoint if self.config.endpoint else None
        
        client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=self.config.access_key,
            aws_secret_access_key=self.config.secret_key,
            config=boto_config,
            verify=False  
        )
        
        logger.info(f"S3 клиент создан: endpoint={endpoint_url}, bucket={self.config.bucket}")
        return client
    
    def upload_file(self, local_path: str, s3_key: str = None) -> bool:
        """
        Загружает файл в S3.
        
        Args:
            local_path: Путь к локальному файлу.
            s3_key: Ключ (путь) файла в S3. Если None, используется имя файла.
            
        Returns:
            True если загрузка успешна, иначе False.
        """
        if not self.client:
            logger.error("S3 клиент не инициализирован")
            return False
        
        if not os.path.exists(local_path):
            logger.error(f"Файл не найден: {local_path}")
            return False
        
        # Если ключ не указан, используем имя файла
        if s3_key is None:
            s3_key = os.path.basename(local_path)
        
        try:
            logger.info(f"Загрузка {local_path} в s3://{self.config.bucket}/{s3_key}")
            
            # Определяем Content-Type
            content_type = self._get_content_type(local_path)
            
            extra_args = {}
            if content_type:
                extra_args['ContentType'] = content_type
            
            self.client.upload_file(
                local_path,
                self.config.bucket,
                s3_key,
                ExtraArgs=extra_args
            )
            
            logger.info(f"Файл успешно загружен: s3://{self.config.bucket}/{s3_key}")
            return True
            
        except ClientError as e:
            logger.error(f"Ошибка при загрузке файла: {e}")
            return False
        except NoCredentialsError:
            logger.error("Неверные учётные данные S3")
            return False
    
    def upload_latest_csv(self, date: datetime = None) -> bool:
        """
        Загружает последний CSV файл с результатами аналитики.
        
        Args:
            date: Дата файла. Если None, используется текущая дата.
            
        Returns:
            True если загрузка успешна, иначе False.
        """
        target_date = date or datetime.now()
        csv_path = output_config.get_csv_path(target_date)
        
        if not os.path.exists(csv_path):
            logger.error(f"CSV файл не найден: {csv_path}")
            return False
        
        # Формируем ключ S3 с датой
        s3_key = f"analytics/{output_config.get_csv_filename(target_date)}"
        
        return self.upload_file(csv_path, s3_key)
    
    def upload_latest_json(self, date: datetime = None) -> bool:
        """
        Загружает последний JSON файл с результатами аналитики.
        
        Args:
            date: Дата файла. Если None, используется текущая дата.
            
        Returns:
            True если загрузка успешна, иначе False.
        """
        target_date = date or datetime.now()
        json_path = output_config.get_json_path(target_date)
        
        if not os.path.exists(json_path):
            logger.error(f"JSON файл не найден: {json_path}")
            return False
        
        # Формируем ключ S3 с датой
        s3_key = f"analytics/{output_config.get_json_filename(target_date)}"
        
        return self.upload_file(json_path, s3_key)
    
    def upload_all_latest(self, date: datetime = None) -> dict:
        """
        Загружает все последние файлы (CSV и JSON) в S3.
        
        Args:
            date: Дата файла. Если None, используется текущая дата.
            
        Returns:
            Словарь с результатами загрузки по каждому формату.
        """
        target_date = date or datetime.now()
        results = {}
        
        # Загружаем CSV
        csv_path = output_config.get_csv_path(target_date)
        if os.path.exists(csv_path):
            results["csv"] = self.upload_latest_csv(target_date)
        else:
            logger.warning(f"CSV файл не найден: {csv_path}")
            results["csv"] = False
        
        # Загружаем JSON
        json_path = output_config.get_json_path(target_date)
        if os.path.exists(json_path):
            results["json"] = self.upload_latest_json(target_date)
        else:
            logger.warning(f"JSON файл не найден: {json_path}")
            results["json"] = False
        
        return results
    
    def list_bucket_contents(self, prefix: str = "") -> list:
        """
        Возвращает список объектов в бакете.
        
        Args:
            prefix: Префикс для фильтрации объектов.
            
        Returns:
            Список словарей с информацией об объектах.
        """
        if not self.client:
            return []
        
        try:
            response = self.client.list_objects_v2(
                Bucket=self.config.bucket,
                Prefix=prefix
            )
            
            if 'Contents' not in response:
                return []
            
            return response['Contents']
            
        except ClientError as e:
            logger.error(f"Ошибка при получении списка объектов: {e}")
            return []
    
    def _get_content_type(self, file_path: str) -> str:
        """Определяет Content-Type по расширению файла."""
        ext = os.path.splitext(file_path)[1].lower()
        content_types = {
            '.csv': 'text/csv',
            '.json': 'application/json',
            '.parquet': 'application/octet-stream',
            '.txt': 'text/plain',
            '.gz': 'application/gzip',
            '.zip': 'application/zip'
        }
        return content_types.get(ext, 'application/octet-stream')


def main():
    """Точка входа для загрузки файлов в S3."""
    parser = argparse.ArgumentParser(description="Загрузка файлов в S3 хранилище")
    parser.add_argument(
        "--file", "-f",
        type=str,
        help="Путь к файлу для загрузки (если не указан, загружаются последние файлы)"
    )
    parser.add_argument(
        "--date", "-d",
        type=str,
        help="Дата файла в формате YYYY-MM-DD (по умолчанию текущая дата)"
    )
    parser.add_argument(
        "--s3-key", "-k",
        type=str,
        help="Ключ (путь) в S3 (по умолчанию analytics/имя_файла)"
    )
    parser.add_argument(
        "--format", "-t",
        type=str,
        choices=["csv", "json", "all"],
        default="all",
        help="Формат файлов для загрузки: csv, json или all (по умолчанию all)"
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="Показать содержимое бакета"
    )
    
    args = parser.parse_args()
    
    try:
        uploader = S3Uploader()
        
        # Показать содержимое бакета
        if args.list:
            logger.info(f"Содержимое бакета {s3_config.bucket}:")
            objects = uploader.list_bucket_contents()
            for obj in objects:
                print(f"  {obj['Key']} ({obj['Size']} bytes, {obj['LastModified']})")
            return 0
        
        # Определяем дату
        target_date = None
        if args.date:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        
        # Загрузка указанного файла
        if args.file:
            s3_key = args.s3_key or f"analytics/{os.path.basename(args.file)}"
            success = uploader.upload_file(args.file, s3_key)
            return 0 if success else 1
        
        # Загрузка файлов по формату
        if args.format == "all":
            results = uploader.upload_all_latest(target_date)
            success = all(results.values())
            logger.info(f"Результаты загрузки: CSV={results.get('csv')}, JSON={results.get('json')}")
        elif args.format == "csv":
            success = uploader.upload_latest_csv(target_date)
        elif args.format == "json":
            success = uploader.upload_latest_json(target_date)
        
        return 0 if success else 1
        
    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
