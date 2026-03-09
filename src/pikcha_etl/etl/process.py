"""
ETL процесс для построения витрины клиентских признаков.
Использует PySpark для обработки данных из Clickhouse MART.
"""

import findspark
findspark.init()

import logging
import os
import sys
import glob
import shutil
from datetime import datetime
from pathlib import Path
from typing import Final

# Добавляем корень проекта в PYTHONPATH для запуска как скрипта
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent.parent))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.pikcha_etl.etl.config import clickhouse_config, feature_config, output_config

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CustomerFeatureETL:
    """
    ETL процесс для создания витрины клиентских признаков.
    
    Извлекает данные из Clickhouse MART, рассчитывает 30 признаков
    для каждого клиента и сохраняет результат.
    """
    
    # Имена таблиц в Clickhouse
    TABLE_FACT_PURCHASES: Final[str] = "fact_purchases"
    TABLE_FACT_PURCHASE_ITEMS: Final[str] = "fact_purchase_items"
    TABLE_DIM_CUSTOMER: Final[str] = "dim_customer"
    TABLE_DIM_PRODUCT: Final[str] = "dim_product"
    TABLE_DIM_STORE: Final[str] = "dim_store"
    TABLE_DIM_DATE: Final[str] = "dim_date"
    
    # Имя выходной таблицы
    OUTPUT_TABLE: Final[str] = "customer_features_mart"
    
    def __init__(self, spark: SparkSession | None = None):
        """
        Инициализация ETL процесса.
        
        Args:
            spark: Существующая SparkSession. Если None, создаётся новая.
        """
        self.spark = spark or self._create_spark_session()
        self.reference_date = datetime.now()
        
    def _create_spark_session(self) -> SparkSession:
        """Создаёт и возвращает SparkSession с поддержкой Clickhouse."""
        logger.info("Создание SparkSession...")
        
        spark = (
            SparkSession.builder
            .appName("CustomerFeatureETL")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate()
        )
        
        spark.sparkContext.setLogLevel("WARN")
        logger.info("SparkSession создана успешно")
        return spark
    
    def _read_clickhouse_table(self, table_name: str) -> DataFrame:
        """
        Читает таблицу из Clickhouse.
        
        Args:
            table_name: Имя таблицы в базе данных MART.
            
        Returns:
            DataFrame с данными таблицы.
        """
        logger.info(f"Чтение таблицы {table_name} из Clickhouse...")
        
        df = (
            self.spark.read
            .format("jdbc")
            .option("url", clickhouse_config.jdbc_url)
            .option("dbtable", f"{clickhouse_config.database}.{table_name}")
            .option("user", clickhouse_config.user)
            .option("password", clickhouse_config.password)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .load()
        )
        
        logger.info(f"Загружено {df.count():,} строк из {table_name}")
        return df
    
    def extract(self) -> dict[str, DataFrame]:
        """
        Извлекает все необходимые данные из Clickhouse MART.
        
        Returns:
            Словарь с DataFrame для каждой таблицы.
        """
        logger.info("=== НАЧАЛО ИЗВЛЕЧЕНИЯ ДАННЫХ ===")
        
        data = {
            "purchases": self._read_clickhouse_table(self.TABLE_FACT_PURCHASES),
            "items": self._read_clickhouse_table(self.TABLE_FACT_PURCHASE_ITEMS),
            "customers": self._read_clickhouse_table(self.TABLE_DIM_CUSTOMER),
            "products": self._read_clickhouse_table(self.TABLE_DIM_PRODUCT),
            "stores": self._read_clickhouse_table(self.TABLE_DIM_STORE),
            "dates": self._read_clickhouse_table(self.TABLE_DIM_DATE),
        }
        
        logger.info("=== ИЗВЛЕЧЕНИЕ ЗАВЕРШЕНО ===")
        return data
    
    def transform(self, data: dict[str, DataFrame]) -> DataFrame:
        """
        Трансформирует данные и рассчитывает признаки клиентов.
        
        Args:
            data: Словарь с исходными DataFrame.
            
        Returns:
            DataFrame с признаками клиентов.
        """
        logger.info("=== НАЧАЛО ТРАНСФОРМАЦИИ ===")
        
        purchases = data["purchases"]
        items = data["items"]
        customers = data["customers"]
        products = data["products"]
        stores = data["stores"]
        dates = data["dates"]
        
        # Кэшируем часто используемые DataFrame
        purchases.cache()
        items.cache()
        customers.cache()
        
        # Текущая дата для расчётов
        now = F.lit(self.reference_date)
        now_date = F.to_date(now)
        
        # === Подготовка данных ===
        
        # Присоединяем информацию о магазине к покупкам
        purchases_with_store = purchases.join(
            stores.select(
                F.col("store_id"),
                F.col("store_location_sk")
            ),
            on="store_id",
            how="left"
        )
        
        # Присоединяем информацию о продуктах к позициям
        items_with_product = items.join(
            products.select(
                F.col("product_id"),
                F.col("product_group"),
                F.col("is_organic")
            ),
            on="product_id",
            how="left"
        )
        
        # Объединяем покупки с позициями
        purchases_full = purchases_with_store.join(
            items_with_product,
            on="purchase_id",
            how="left"
        )
        
        # Добавляем временную информацию
        purchases_full = purchases_full.join(
            dates.select(
                F.col("date_sk").alias("purchase_date_sk"),
                F.col("day_of_week"),
                F.col("is_weekend")
            ),
            on="purchase_date_sk",
            how="left"
        )
        
        # Кэшируем объединённый DataFrame
        purchases_full.cache()
        
        # === Расчёт признаков ===
        
        logger.info("Расчёт агрегированных метрик...")
        
        # 1. Базовые метрики по клиентам
        customer_metrics = purchases_full.groupBy("customer_id").agg(
            # Общее количество покупок
            F.countDistinct("purchase_id").alias("total_purchases"),
            
            # Даты первой и последней покупки
            F.min("raw_event_time").alias("first_purchase_date"),
            F.max("raw_event_time").alias("last_purchase_date"),
            
            # Метрики корзины
            F.avg("total_amount").alias("avg_cart_amount"),
            F.sum("total_amount").alias("total_spent"),
            F.avg(F.col("items_count")).alias("avg_items_count"),
            
            # Доставка
            F.sum(F.col("is_delivery").cast("int")).alias("delivery_count"),
            
            # Способы оплаты
            F.sum(F.when(F.col("payment_method") == "cash", 1).otherwise(0)).alias("cash_payments"),
            F.sum(F.when(F.col("payment_method") == "card", 1).otherwise(0)).alias("card_payments"),
            
            # Покупки в выходные/будни
            F.sum(F.col("is_weekend").cast("int")).alias("weekend_purchases"),
            F.count("purchase_id").alias("total_purchases_for_weekend"),
            
            # Временные паттерны
            F.sum(F.when(F.hour("raw_event_time") >= 20, 1).otherwise(0)).alias("night_purchases"),
            F.sum(F.when(F.hour("raw_event_time") < 10, 1).otherwise(0)).alias("morning_purchases"),
            F.sum(F.when(
                (F.hour("raw_event_time") >= 12) & (F.hour("raw_event_time") < 15),
                1
            ).otherwise(0)).alias("lunch_purchases"),
            
            # Количество магазинов
            F.countDistinct("store_id").alias("unique_stores"),
            
            # Количество городов (через stores)
            F.countDistinct("store_location_sk").alias("unique_locations"),
            
            # Категории продуктов
            F.countDistinct("product_group").alias("unique_categories"),
            
            # Покупки по категориям
            F.sum(F.when(
                F.lower(F.col("product_group")).contains("молочн"),
                1
            ).otherwise(0)).alias("dairy_purchases"),
            
            F.sum(F.when(
                F.lower(F.col("product_group")).contains("фрукт"),
                1
            ).otherwise(0)).alias("fruit_purchases"),
            
            F.sum(F.when(
                F.lower(F.col("product_group")).contains("овощ"),
                1
            ).otherwise(0)).alias("veggie_purchases"),
            
            F.sum(F.when(
                F.lower(F.col("product_group")).contains("мясо") | 
                F.lower(F.col("product_group")).contains("рыб") |
                F.lower(F.col("product_group")).contains("яйц"),
                1
            ).otherwise(0)).alias("meat_purchases"),
            
            F.sum(F.when(
                F.lower(F.col("product_group")).contains("хлеб") | 
                F.lower(F.col("product_group")).contains("зернов"),
                1
            ).otherwise(0)).alias("bakery_purchases"),
            
            # Органические продукты
            F.sum(F.col("is_organic").cast("int")).alias("organic_purchases"),
            
            # Корзины с одним товаром
            F.sum(F.when(F.col("items_count") == 1, 1).otherwise(0)).alias("single_item_purchases"),
            
            # Последние покупки
            F.sum(F.when(
                F.datediff(now_date, F.to_date("raw_event_time")) <= 7,
                F.col("total_amount")
            ).otherwise(0)).alias("spent_last_7d"),
            
            F.sum(F.when(
                F.datediff(now_date, F.to_date("raw_event_time")) <= 14,
                1
            ).otherwise(0)).alias("purchases_last_14d"),
            
            F.sum(F.when(
                F.datediff(now_date, F.to_date("raw_event_time")) <= 30,
                1
            ).otherwise(0)).alias("purchases_last_30d"),
            
            # Покупки молочных за 30 дней
            F.sum(F.when(
                (F.lower(F.col("product_group")).contains("молочн")) &
                (F.datediff(now_date, F.to_date("raw_event_time")) <= 30),
                1
            ).otherwise(0)).alias("dairy_purchases_30d"),
            
            # Покупки фруктов за 14 и 30 дней
            F.sum(F.when(
                (F.lower(F.col("product_group")).contains("фрукт")) &
                (F.datediff(now_date, F.to_date("raw_event_time")) <= 14),
                1
            ).otherwise(0)).alias("fruit_purchases_14d"),
            
            F.sum(F.when(
                (F.lower(F.col("product_group")).contains("фрукт")) &
                (F.datediff(now_date, F.to_date("raw_event_time")) <= 30),
                1
            ).otherwise(0)).alias("fruit_purchases_30d"),
            
            # Покупки овощей за 14 дней
            F.sum(F.when(
                (F.lower(F.col("product_group")).contains("овощ")) &
                (F.datediff(now_date, F.to_date("raw_event_time")) <= 14),
                1
            ).otherwise(0)).alias("veggie_purchases_14d"),
            
            # Покупки мяса за 7 и 90 дней
            F.sum(F.when(
                (F.lower(F.col("product_group")).contains("мясо") | 
                 F.lower(F.col("product_group")).contains("рыб") |
                 F.lower(F.col("product_group")).contains("яйц")) &
                (F.datediff(now_date, F.to_date("raw_event_time")) <= 7),
                1
            ).otherwise(0)).alias("meat_purchases_7d"),
            
            F.sum(F.when(
                (F.lower(F.col("product_group")).contains("мясо") | 
                 F.lower(F.col("product_group")).contains("рыб") |
                 F.lower(F.col("product_group")).contains("яйц")) &
                (F.datediff(now_date, F.to_date("raw_event_time")) <= 90),
                1
            ).otherwise(0)).alias("meat_purchases_90d"),
        )
        
        # 2. Присоединяем информацию о клиентах (дата регистрации, карта лояльности)
        customers_info = customers.select(
            F.col("customer_id"),
            F.col("customer_sk"),
            F.col("registration_date"),
            F.col("is_loyalty_member"),
            F.col("loyalty_card_number"),
            F.col("purchase_location_sk")
        )
        
        customer_metrics = customer_metrics.join(
            customers_info,
            on="customer_id",
            how="right"
        )
        
        # 3. Вычисляем итоговые признаки
        logger.info("Вычисление итоговых признаков...")
        
        result = customer_metrics.select(
            # Идентификаторы
            F.col("customer_id"),
            F.col("customer_sk"),
            
            # 1. bought_milk_last_30d
            (F.col("dairy_purchases_30d") > 0).alias("bought_milk_last_30d"),
            
            # 2. bought_fruits_last_14d
            (F.col("fruit_purchases_14d") > 0).alias("bought_fruits_last_14d"),
            
            # 3. not_bought_veggies_14d
            (F.coalesce(F.col("veggie_purchases_14d"), F.lit(0)) == 0).alias("not_bought_veggies_14d"),
            
            # 4. recurrent_buyer
            (F.coalesce(F.col("purchases_last_30d"), F.lit(0)) > feature_config.min_purchases_recurrent).alias("recurrent_buyer"),
            
            # 5. inactive_14_30
            (
                (F.datediff(now_date, F.to_date("last_purchase_date")) >= 14) &
                (F.datediff(now_date, F.to_date("last_purchase_date")) <= 30)
            ).alias("inactive_14_30"),
            
            # 6. new_customer
            (
                F.datediff(now_date, F.to_date("registration_date")) <= 30
            ).alias("new_customer"),
            
            # 7. delivery_user
            (F.coalesce(F.col("delivery_count"), F.lit(0)) > 0).alias("delivery_user"),
            
            # 8. organic_preference
            (F.coalesce(F.col("organic_purchases"), F.lit(0)) > 0).alias("organic_preference"),
            
            # 9. bulk_buyer
            (F.coalesce(F.col("avg_cart_amount"), F.lit(0)) > feature_config.high_cart_threshold).alias("bulk_buyer"),
            
            # 10. low_cost_buyer
            (F.coalesce(F.col("avg_cart_amount"), F.lit(0)) < feature_config.low_cart_threshold).alias("low_cost_buyer"),
            
            # 11. buys_bakery
            (F.coalesce(F.col("bakery_purchases"), F.lit(0)) > 0).alias("buys_bakery"),
            
            # 12. loyal_customer
            (
                (F.col("is_loyalty_member") == 1) &
                (F.coalesce(F.col("total_purchases"), F.lit(0)) >= feature_config.min_purchases_loyal)
            ).alias("loyal_customer"),
            
            # 13. multicity_buyer
            (F.coalesce(F.col("unique_locations"), F.lit(0)) > 1).alias("multicity_buyer"),
            
            # 14. bought_meat_last_week
            (F.coalesce(F.col("meat_purchases_7d"), F.lit(0)) > 0).alias("bought_meat_last_week"),
            
            # 15. night_shopper
            (F.coalesce(F.col("night_purchases"), F.lit(0)) > 0).alias("night_shopper"),
            
            # 16. morning_shopper
            (F.coalesce(F.col("morning_purchases"), F.lit(0)) > 0).alias("morning_shopper"),
            
            # 17. prefers_cash
            (
                F.coalesce(F.col("cash_payments"), F.lit(0)) / 
                F.greatest(F.coalesce(F.col("total_purchases"), F.lit(1)), F.lit(1)) >= 
                feature_config.payment_threshold
            ).alias("prefers_cash"),
            
            # 18. prefers_card
            (
                F.coalesce(F.col("card_payments"), F.lit(0)) / 
                F.greatest(F.coalesce(F.col("total_purchases"), F.lit(1)), F.lit(1)) >= 
                feature_config.payment_threshold
            ).alias("prefers_card"),
            
            # 19. weekend_shopper
            (
                F.coalesce(F.col("weekend_purchases"), F.lit(0)) / 
                F.greatest(F.coalesce(F.col("total_purchases_for_weekend"), F.lit(1)), F.lit(1)) >= 
                feature_config.weekend_threshold
            ).alias("weekend_shopper"),
            
            # 20. weekday_shopper
            (
                (F.coalesce(F.col("total_purchases_for_weekend"), F.lit(0)) - 
                 F.coalesce(F.col("weekend_purchases"), F.lit(0))) / 
                F.greatest(F.coalesce(F.col("total_purchases_for_weekend"), F.lit(1)), F.lit(1)) >= 
                feature_config.weekend_threshold
            ).alias("weekday_shopper"),
            
            # 21. single_item_buyer
            (
                F.coalesce(F.col("single_item_purchases"), F.lit(0)) / 
                F.greatest(F.coalesce(F.col("total_purchases"), F.lit(1)), F.lit(1)) >= 
                feature_config.single_item_threshold
            ).alias("single_item_buyer"),
            
            # 22. varied_shopper
            (F.coalesce(F.col("unique_categories"), F.lit(0)) >= feature_config.min_categories_varied).alias("varied_shopper"),
            
            # 23. store_loyal
            (F.coalesce(F.col("unique_stores"), F.lit(0)) == 1).alias("store_loyal"),
            
            # 24. switching_store
            (F.coalesce(F.col("unique_stores"), F.lit(0)) > 1).alias("switching_store"),
            
            # 25. family_shopper
            (F.coalesce(F.col("avg_items_count"), F.lit(0)) >= feature_config.min_items_family).alias("family_shopper"),
            
            # 26. early_bird
            (F.coalesce(F.col("lunch_purchases"), F.lit(0)) > 0).alias("early_bird"),
            
            # 27. no_purchases
            (F.coalesce(F.col("total_purchases"), F.lit(0)) == 0).alias("no_purchases"),
            
            # 28. recent_high_spender
            (F.coalesce(F.col("spent_last_7d"), F.lit(0)) > feature_config.recent_high_spender_threshold).alias("recent_high_spender"),
            
            # 29. fruit_lover
            (F.coalesce(F.col("fruit_purchases_30d"), F.lit(0)) >= feature_config.fruit_purchases_threshold).alias("fruit_lover"),
            
            # 30. vegetarian_profile
            (
                (F.coalesce(F.col("meat_purchases_90d"), F.lit(0)) == 0) &
                (F.coalesce(F.col("total_purchases"), F.lit(0)) > 0)
            ).alias("vegetarian_profile"),
            
            # Дополнительные метрики для анализа
            F.col("total_purchases"),
            F.col("total_spent"),
            F.col("avg_cart_amount"),
            
            # Метка времени создания записи
            F.lit(self.reference_date).alias("created_at")
        )
        
        # Освобождаем кэш
        purchases.unpersist()
        items.unpersist()
        customers.unpersist()
        purchases_full.unpersist()
        
        logger.info("=== ТРАНСФОРМАЦИЯ ЗАВЕРШЕНА ===")
        return result
    
    def load_to_clickhouse(self, df: DataFrame, table_name: str | None = None) -> None:
        """
        Сохраняет результаты в Clickhouse.
        
        Args:
            df: DataFrame с результатами.
            table_name: Имя таблицы для сохранения.
        """
        table = table_name or self.OUTPUT_TABLE
        logger.info(f"=== НАЧАЛО ЗАГРУЗКИ в {table} ===")
        
        from clickhouse_driver import Client
        
        client = Client(
            host=clickhouse_config.host,
            port=clickhouse_config.native_port,
            database=clickhouse_config.database,
            user=clickhouse_config.user,
            password=clickhouse_config.password
        )
        
        # Проверяем существование таблицы
        tables = client.execute(
            "SELECT name FROM system.tables "
            f"WHERE database = '{clickhouse_config.database}' AND name = '{table}'"
        )
        
        if not tables:
            logger.error(f"Таблица {table} не найдена. Выполните SQL скрипт создания таблицы.")
            raise RuntimeError(f"Таблица {table} не существует.")
        
        # Очищаем таблицу перед загрузкой
        logger.info(f"Очистка таблицы {table}...")
        client.execute(f"TRUNCATE TABLE {clickhouse_config.database}.{table}")
        
        # Записываем данные в Clickhouse
        df.write \
            .format("jdbc") \
            .option("url", clickhouse_config.jdbc_url) \
            .option("dbtable", f"{clickhouse_config.database}.{table}") \
            .option("user", clickhouse_config.user) \
            .option("password", clickhouse_config.password) \
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
            .mode("append") \
            .save()
        
        logger.info(f"=== ЗАГРУЗКА ЗАВЕРШЕНА: сохранено {df.count():,} записей ===")
    
    def save_to_csv(self, df: DataFrame, date: datetime = None) -> str:
        """
        Сохраняет результаты в CSV файл.
        
        Args:
            df: DataFrame с результатами.
            date: Дата для имени файла.
            
        Returns:
            Путь к сохранённому файлу.
        """
        target_date = date or self.reference_date
        output_path = output_config.get_csv_path(target_date)
        
        logger.info(f"Сохранение результатов в {output_path}...")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        csv_df = df.drop("customer_sk", "created_at")
        
        csv_df.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .option("encoding", "UTF-8") \
            .csv(output_path + "_tmp")
        
        tmp_dir = output_path + "_tmp"
        csv_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
        
        if csv_files:
            shutil.move(csv_files[0], output_path)
            shutil.rmtree(tmp_dir)
            logger.info(f"Результаты сохранены в {output_path}")
        else:
            raise FileNotFoundError(f"CSV файл не найден в {tmp_dir}")
        
        return output_path
    
    def save_to_json(self, df: DataFrame, date: datetime = None) -> str:
        """
        Сохраняет результаты в JSON файл.
        
        Args:
            df: DataFrame с результатами.
            date: Дата для имени файла.
            
        Returns:
            Путь к сохранённому файлу.
        """
        target_date = date or self.reference_date
        output_path = output_config.get_json_path(target_date)
        
        logger.info(f"Сохранение результатов в {output_path}...")
        
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        json_df = df.drop("customer_sk", "created_at")
        
        json_df.coalesce(1).write \
            .mode("overwrite") \
            .option("encoding", "UTF-8") \
            .json(output_path + "_tmp")
        
        tmp_dir = output_path + "_tmp"
        json_files = glob.glob(os.path.join(tmp_dir, "part-*.json"))
        
        if json_files:
            shutil.move(json_files[0], output_path)
            shutil.rmtree(tmp_dir)
            logger.info(f"Результаты сохранены в {output_path}")
        else:
            raise FileNotFoundError(f"JSON файл не найден в {tmp_dir}")
        
        return output_path
    
    def run(self, output_format: str = "all", date: datetime = None) -> dict:
        """
        Запускает полный ETL процесс.
        
        Args:
            output_format: Формат вывода - "clickhouse", "csv", "json" или "all".
            date: Дата для имени файла.
            
        Returns:
            Словарь с путями к сохранённым файлам.
        """
        logger.info("=" * 60)
        logger.info("ЗАПУСК ETL ПРОЦЕССА")
        logger.info("=" * 60)
        
        result_paths = {}
        
        # Extract
        data = self.extract()
        
        # Transform
        result_df = self.transform(data)
        
        # Load
        if output_format in ("clickhouse", "all"):
            self.load_to_clickhouse(result_df)
        
        if output_format in ("csv", "all"):
            result_paths["csv"] = self.save_to_csv(result_df, date)
        
        if output_format in ("json", "all"):
            result_paths["json"] = self.save_to_json(result_df, date)
        
        logger.info("=" * 60)
        logger.info("ETL ПРОЦЕСС УСПЕШНО ЗАВЕРШЁН")
        logger.info("=" * 60)
        
        return result_paths
