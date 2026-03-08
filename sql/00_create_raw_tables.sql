-- ============================================================================
--           RAW LAYER: ТАБЛИЦЫ СЫРЫХ ДАННЫХ ИЗ KAFKA
-- ============================================================================
-- Проект:      ETL-конвейер для ритейл-аналитики
-- Назначение:  Создание таблиц для приёма сырых данных из Kafka
-- База данных: raw
-- ============================================================================
-- ДВИЖОК:       MergeTree с партиционированием по месяцам
-- TTL:          180 дней (6 месяцев)
-- ПОРЯДОК ВЫПОЛНЕНИЯ:
--   1. Создать базу данных raw (этот скрипт)
--   2. Запустить Kafka Connect / consumer для записи данных
--   3. Выполнить скрипты создания mart-слоя (01, 02, 03)
-- ============================================================================


-- ============================================================================
--                              ОГЛАВЛЕНИЕ
-- ============================================================================
--   0. СОЗДАНИЕ БАЗЫ ДАННЫХ
--      0.1  CREATE DATABASE raw
--
--   1. ТАБЛИЦЫ СЫРЫХ ДАННЫХ
--      1.1  raw.stores      - магазины сети
--      1.2  raw.products    - товары
--      1.3  raw.customers   - клиенты
--      1.4  raw.purchases   - покупки (чеки) с Nested-структурой позиций
-- ============================================================================


-- ============================================================================
--                    0. СОЗДАНИЕ БАЗЫ ДАННЫХ
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0.1 Создание базы данных raw
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS raw;


-- ============================================================================
--                    1. ТАБЛИЦЫ СЫРЫХ ДАННЫХ
-- ============================================================================


-- ============================================================================
-- 1.1 ТАБЛИЦА: raw.stores
-- ============================================================================
-- Источник:     Kafka topic "stores"
-- Описание:     Магазины сети с атрибутами локации, менеджмента, часов работы
-- Движок:       MergeTree
-- Партицион:    toYYYYMM(event_time)
-- TTL:          event_time + INTERVAL 180 DAY
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: raw.stores - сырые данные магазинов
-- Источник: Kafka topic "stores"
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.stores (
    -- Служебные поля
    json_data String COMMENT 'Полный JSON документа',
    event_time DateTime64(9) COMMENT 'Время события из Kafka',

    -- Атрибуты магазина
    store_id String,
    store_name String,
    store_network String,
    store_type_description String,
    type String,
    categories String,
    manager_name String,
    manager_phone String,
    manager_email String,
    location_country String,
    location_city String,
    location_street String,
    location_house String,
    location_postal_code String,
    location_coordinates_latitude String,
    location_coordinates_longitude String,
    opening_hours_mon_fri String,
    opening_hours_sat String,
    opening_hours_sun String,
    accepts_online_orders String,
    delivery_available String,
    warehouse_connected String,
    last_inventory_date String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY event_time
TTL event_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192
COMMENT 'Сырые данные магазинов из Kafka';


-- ============================================================================
-- 1.2 ТАБЛИЦА: raw.products
-- ============================================================================
-- Источник:     Kafka topic "products"
-- Описание:     Товары с атрибутами продукта, КБЖУ, цены, производителя
-- Движок:       MergeTree
-- Партицион:    toYYYYMM(event_time)
-- TTL:          event_time + INTERVAL 180 DAY
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: raw.products - сырые данные товаров
-- Источник: Kafka topic "products"
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.products (
    -- Служебные поля
    json_data String COMMENT 'Полный JSON документа',
    event_time DateTime64(9) COMMENT 'Время события из Kafka',

    -- Атрибуты товара
    id String,
    name String,
    group String,
    description String,
    kbju_calories String,
    kbju_protein String,
    kbju_fat String,
    kbju_carbohydrates String,
    price String,
    unit String,
    origin_country String,
    expiry_days String,
    is_organic String,
    barcode String,
    manufacturer_name String,
    manufacturer_country String,
    manufacturer_website String,
    manufacturer_inn String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY event_time
TTL event_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192
COMMENT 'Сырые данные товаров из Kafka';


-- ============================================================================
-- 1.3 ТАБЛИЦА: raw.customers
-- ============================================================================
-- Источник:     Kafka topic "customers"
-- Описание:     Клиенты с персональными данными, предпочтениями, адресами
-- Движок:       MergeTree
-- Партицион:    toYYYYMM(event_time)
-- TTL:          event_time + INTERVAL 180 DAY
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: raw.customers - сырые данные клиентов
-- Источник: Kafka topic "customers"
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.customers (
    -- Служебные поля
    json_data String COMMENT 'Полный JSON документа',
    event_time DateTime64(9) COMMENT 'Время события из Kafka',

    -- Атрибуты клиента
    customer_id String,
    first_name String,
    last_name String,
    email String,
    phone String,
    birth_date String,
    gender String,
    registration_date String,
    is_loyalty_member String,
    loyalty_card_number String,
    purchase_location_country String,
    purchase_location_city String,
    purchase_location_street String,
    purchase_location_house String,
    purchase_location_postal_code String,
    purchase_location_coordinates_latitude String,
    purchase_location_coordinates_longitude String,
    delivery_address_country String,
    delivery_address_city String,
    delivery_address_street String,
    delivery_address_house String,
    delivery_address_apartment String,
    delivery_address_postal_code String,
    preferences_preferred_language String,
    preferences_preferred_payment_method String,
    preferences_receive_promotions String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY event_time
TTL event_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192
COMMENT 'Сырые данные клиентов из Kafka';


-- ============================================================================
-- 1.4 ТАБЛИЦА: raw.purchases
-- ============================================================================
-- Источник:     Kafka topic "purchases"
-- Описание:     Покупки (чеки) с позициями товаров в Nested-структуре
-- Особенность:  Массив товаров items хранится как Nested (набор массивов)
-- Движок:       MergeTree
-- Партицион:    toYYYYMM(event_time)
-- TTL:          event_time + INTERVAL 180 DAY
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: raw.purchases - сырые данные покупок
-- Источник: Kafka topic "purchases"
-- Особенность: Nested-структура для позиций чека
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.purchases (
    -- Служебные поля
    json_data String COMMENT 'Полный JSON документа',
    event_time DateTime64(9) COMMENT 'Время события из Kafka',

    -- Атрибуты чека
    purchase_id String,

    -- Данные клиента (денормализованные)
    customer_customer_id String,
    customer_first_name String,
    customer_last_name String,
    customer_email String,
    customer_phone String,
    customer_is_loyalty_member String,
    customer_loyalty_card_number String,

    -- Данные магазина (денормализованные)
    store_store_id String,
    store_store_name String,
    store_store_network String,
    store_location_country String,
    store_location_city String,
    store_location_street String,
    store_location_house String,
    store_location_postal_code String,
    store_location_coordinates_latitude String,
    store_location_coordinates_longitude String,

    -- Позиции чека (Nested-структура - набор массивов одинаковой длины)
    items Nested (
        product_id String,
        name String,
        category String,
        quantity String,
        unit String,
        price_per_unit String,
        total_price String,
        kbju_calories String,
        kbju_protein String,
        kbju_fat String,
        kbju_carbohydrates String,
        manufacturer_name String,
        manufacturer_country String,
        manufacturer_website String,
        manufacturer_inn String
    ),

    -- Финансы и оплата
    total_amount String,
    payment_method String,

    -- Доставка
    is_delivery String,
    delivery_address_country String,
    delivery_address_city String,
    delivery_address_street String,
    delivery_address_house String,
    delivery_address_apartment String,
    delivery_address_postal_code String,

    -- Время покупки
    purchase_datetime String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
ORDER BY event_time
TTL event_time + INTERVAL 180 DAY
SETTINGS index_granularity = 8192
COMMENT 'Сырые данные покупок из Kafka с Nested-структурой для позиций';
