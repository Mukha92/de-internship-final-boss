-- ============================================================================
--           ТАБЛИЦЫ MART-СЛОЯ ХРАНИЛИЩА ДАННЫХ
-- ============================================================================
-- Проект:      ETL-конвейер для ритейл-аналитики
-- Назначение:  Создание таблиц для витрин данных (схема снежинка)
-- База данных: mart
-- ============================================================================
-- ПОРЯДОК ВЫПОЛНЕНИЯ:
--   1. Создать базу данных (этот скрипт)
--   2. Создать материализованные представления (02_create_materialized_views.sql)
--   3. Загрузить данные в raw-слой
-- ============================================================================


-- ============================================================================
--                              ОГЛАВЛЕНИЕ
-- ============================================================================
--   0. СОЗДАНИЕ БАЗЫ ДАННЫХ
--      0.1  CREATE DATABASE mart
--
--   1. ИЗМЕРЕНИЯ (DIMENSIONS)
--      1.1  dim_manufacturer   - производители (из products, purchases)
--      1.2  dim_store_location - местоположения магазинов (из stores, purchases)
--      1.3  dim_delivery_address - адреса доставки (из customers, purchases)
--      1.4  dim_product        - продукты (из products, purchases)
--      1.5  dim_customer       - клиенты (из customers, purchases)
--      1.6  dim_store          - магазины (из stores, purchases)
--      1.7  dim_date           - календарь (из purchases)
--
--   2. ТАБЛИЦЫ ФАКТОВ (FACT TABLES)
--      2.1  fact_purchases     - заголовки чеков
--      2.2  fact_purchase_items - позиции в чеках
-- ============================================================================


-- ============================================================================
--                    0. СОЗДАНИЕ БАЗЫ ДАННЫХ
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 0.1 Создание базы данных MART
-- ----------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS mart;


-- ============================================================================
--                    1. ИЗМЕРЕНИЯ (DIMENSIONS)
-- ============================================================================


-- ============================================================================
-- 1.1 ИЗМЕРЕНИЕ: dim_manufacturer
-- ============================================================================
-- Источники:     raw.products.manufacturer, raw.purchases.items[].manufacturer
-- Ключ:          manufacturer_sk = xxHash32(manufacturer_inn)
-- Атрибуты:      ИНН, название, страна, сайт
-- Дедупликация:  ReplacingMergeTree по updated_at
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: dim_manufacturer - справочник производителей
-- Источник: Справочник продуктов, позиции покупок
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_manufacturer (
    manufacturer_sk     UInt32,           -- Суррогатный ключ
    manufacturer_inn    String,           -- ИНН (естественный ключ)
    manufacturer_name   String,           -- Название производителя
    manufacturer_country String,          -- Страна производителя
    manufacturer_website String,          -- Веб-сайт
    
    -- Технические поля
    created_at          DateTime DEFAULT now(),
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (manufacturer_sk)
PRIMARY KEY (manufacturer_sk)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- 1.2 ИЗМЕРЕНИЕ: dim_store_location
-- ============================================================================
-- Источники:     raw.stores.location, raw.purchases.store.location
-- Ключ:          store_location_sk = xxHash32(country, city, street, house)
-- Атрибуты:      Страна, город, улица, дом, почтовый индекс, координаты
-- Дедупликация:  ReplacingMergeTree по updated_at
-- Особенности:   Содержит координаты для геоаналитики (latitude, longitude)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: dim_store_location - справочник местоположений магазинов
-- Источник: Справочник магазинов, покупки
-- Особенность: Содержит координаты для геоаналитики
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_store_location (
    store_location_sk   UInt32,           -- Суррогатный ключ

    -- Географические атрибуты
    country             String,           -- Страна
    city                String,           -- Город
    street              String,           -- Улица
    house               String,           -- Дом
    postal_code         String,           -- Почтовый индекс

    -- Координаты для геоаналитики
    latitude            Nullable(Float64),-- Широта
    longitude           Nullable(Float64),-- Долгота

    -- Технические поля
    created_at          DateTime DEFAULT now(),
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (store_location_sk)
PRIMARY KEY (store_location_sk)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 1.3 ИЗМЕРЕНИЕ: dim_delivery_address
-- ============================================================================
-- Источники:     raw.customers.delivery_address, raw.purchases.delivery_address
-- Ключ:          delivery_address_sk = xxHash32(country, city, street, house, apartment)
-- Атрибуты:      Страна, город, улица, дом, квартира, почтовый индекс
-- Дедупликация:  ReplacingMergeTree по updated_at
-- Особенности:   Без координат, содержит номер квартиры
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: dim_delivery_address - справочник адресов доставки
-- Источник: Профили клиентов, покупки
-- Особенность: Содержит номер квартиры; координаты отсутствуют
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_delivery_address (
    delivery_address_sk UInt32,           -- Суррогатный ключ
    
    -- Адресные атрибуты
    country             String,           -- Страна
    city                String,           -- Город
    street              String,           -- Улица
    house               String,           -- Дом
    apartment           String,           -- Квартира
    postal_code         String,           -- Почтовый индекс
    
    -- Технические поля
    created_at          DateTime DEFAULT now(),
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (delivery_address_sk)
PRIMARY KEY (delivery_address_sk)
SETTINGS index_granularity = 8192;

-- ============================================================================
-- 1.4 ИЗМЕРЕНИЕ: dim_product
-- ============================================================================
-- Источники:     raw.products, raw.purchases.items[]
-- Ключ:          product_sk = xxHash32(product_id)
-- Атрибуты:      ID, название, группа, описание, КБЖУ, цена, единица измерения,
--                страна происхождения, срок годности, органик, штрихкод
-- Связи:         manufacturer_sk → dim_manufacturer
-- Дедупликация:  ReplacingMergeTree по updated_at
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: dim_product - справочник продуктов
-- Источник: Справочник продуктов, позиции покупок
-- Ссылается на: dim_manufacturer
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_product (
    product_sk          UInt32,           -- Суррогатный ключ
    product_id          String,           -- Естественный ключ (prd-XXXX)

    -- Основные атрибуты
    product_name        String,           -- Название продукта
    product_group       String,           -- Группа/категория продукта
    product_description String,           -- Описание

    -- КБЖУ (калорийность и нутриенты)
    calories            Nullable(Float64),-- Калории
    protein             Nullable(Float64),-- Белки
    fat                 Nullable(Float64),-- Жиры
    carbohydrates       Nullable(Float64),-- Углеводы

    -- Ценовые и торговые атрибуты
    price               Nullable(Decimal(10, 2)), -- Базовая цена
    unit                String,           -- Единица измерения
    origin_country      String,           -- Страна происхождения
    expiry_days         Nullable(UInt16), -- Срок годности в днях
    is_organic          UInt8,            -- Органический продукт (0/1)
    barcode             String,           -- Штрих-код

    -- Ссылка на производителя (FK)
    manufacturer_sk     Nullable(UInt32), -- Суррогатный ключ производителя

    -- Технические поля
    created_at          DateTime DEFAULT now(),
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (product_sk)
PRIMARY KEY (product_sk)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 1.5 ИЗМЕРЕНИЕ: dim_customer
-- ============================================================================
-- Источники:     raw.customers, raw.purchases.customer
-- Ключ:          customer_sk = xxHash32(customer_id)
-- Атрибуты:      ID, ФИО, email, телефон, дата рождения, пол, дата регистрации,
--                участие в программе лояльности, предпочтения
-- Связи:         purchase_location_sk → dim_store_location
--                delivery_address_sk → dim_delivery_address
-- Дедупликация:  ReplacingMergeTree по updated_at
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: dim_customer - справочник клиентов
-- Источник: Профили клиентов, покупки
-- Ссылается на: dim_store_location (предпочтительная локация покупок)
--               dim_delivery_address (адрес доставки)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_sk         UInt32,           -- Суррогатный ключ
    customer_id         String,           -- Естественный ключ (cus-XXXX)

    -- Персональные данные
    first_name          String,           -- Имя
    last_name           String,           -- Фамилия
    full_name           String,           -- Полное имя (вычисляемое)
    email               String,           -- Email
    phone               String,           -- Телефон
    birth_date          Nullable(Date),   -- Дата рождения
    gender              String,           -- Пол

    -- Регистрация и лояльность
    registration_date   Nullable(DateTime),-- Дата регистрации
    is_loyalty_member   UInt8,            -- Участник программы лояльности
    loyalty_card_number Nullable(String), -- Номер карты лояльности

    -- Предпочтения
    preferred_language  String,           -- Предпочитаемый язык
    preferred_payment_method String,      -- Предпочитаемый способ оплаты
    receive_promotions  UInt8,            -- Получать рассылку (0/1)

    -- Ссылки на локации (FK)
    purchase_location_sk Nullable(UInt32),-- СК локации покупок (dim_store_location)
    delivery_address_sk  Nullable(UInt32),-- СК адреса доставки (dim_delivery_address)

    -- Технические поля
    created_at          DateTime DEFAULT now(),
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (customer_sk)
PRIMARY KEY (customer_sk)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 1.6 ИЗМЕРЕНИЕ: dim_store
-- ============================================================================
-- Источники:     raw.stores, raw.purchases.store
-- Ключ:          store_sk = xxHash32(store_id)
-- Атрибуты:      ID, название, сеть, тип, категории, менеджер, часы работы,
--                флаги (online-заказы, доставка, связь со складом)
-- Связи:         store_location_sk → dim_store_location
-- Дедупликация:  ReplacingMergeTree по updated_at
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: dim_store - справочник магазинов
-- Источник: Справочник магазинов, покупки
-- Ссылается на: dim_store_location
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_store (
    store_sk            UInt32,           -- Суррогатный ключ
    store_id            String,           -- Естественный ключ (store-XXX)

    -- Основные атрибуты
    store_name          String,           -- Название магазина
    store_network       String,           -- Сеть магазинов
    store_type_description String,        -- Описание типа магазина
    store_type          String,           -- Тип (offline/online)

    -- Категории товаров (массив как строка)
    categories          String,           -- Категории товаров через запятую

    -- Управляющий
    manager_name        String,           -- Имя управляющего
    manager_phone       String,           -- Телефон управляющего
    manager_email       String,           -- Email управляющего

    -- Часы работы
    opening_hours_mon_fri String,         -- Пн-Пт
    opening_hours_sat   String,           -- Суббота
    opening_hours_sun   String,           -- Воскресенье

    -- Дополнительные флаги
    accepts_online_orders UInt8,          -- Принимает онлайн заказы
    delivery_available  UInt8,            -- Доступна доставка
    warehouse_connected UInt8,            -- Подключен к складу
    last_inventory_date Nullable(Date),   -- Дата последней инвентаризации

    -- Ссылка на локацию (FK)
    store_location_sk   Nullable(UInt32), -- СК локации магазина (dim_store_location)

    -- Технические поля
    created_at          DateTime DEFAULT now(),
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (store_sk)
PRIMARY KEY (store_sk)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 1.7 ИЗМЕРЕНИЕ: dim_date (календарь)
-- ============================================================================
-- Источники:     raw.purchases.purchase_datetime
-- Ключ:          date_sk = YYYYMMDD (числовой формат)
-- Атрибуты:      Год, квартал, месяц, неделя, день
--                (названия на русском языке)
-- Флаги:         is_leap_year, is_weekend, is_holiday
-- Особенность:   Даты > today() исключаются
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: dim_date - календарь
-- Источник: Покупки (извлечение уникальных дат)
-- Особенность: Генерируется автоматически для аналитики
-- Дедупликация: ReplacingMergeTree (защита от повторных вставок)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_sk             UInt32,           -- Суррогатный ключ (YYYYMMDD)
    full_date           Date,             -- Полная дата

    -- Атрибуты года
    year                UInt16,           -- Год
    year_name           String,           -- Название года
    is_leap_year        UInt8,            -- Високосный год

    -- Атрибуты квартала
    quarter             UInt8,            -- Квартал (1-4)
    quarter_name        String,           -- Название квартала (Q1, Q2, ...)
    year_quarter        String,           -- Год-Квартал (2024-Q1)

    -- Атрибуты месяца
    month               UInt8,            -- Месяц (1-12)
    month_name          String,           -- Название месяца
    year_month          String,           -- Год-Месяц (2024-01)
    days_in_month       UInt8,            -- Дней в месяце

    -- Атрибуты недели
    week_of_year        UInt8,            -- Неделя года
    week_of_month       UInt8,            -- Неделя месяца
    year_week           String,           -- Год-Неделя (2024-W05)

    -- Атрибуты дня
    day_of_month        UInt8,            -- День месяца
    day_of_year         UInt16,           -- День года
    day_of_week         UInt8,            -- День недели (1=Пн, 7=Вс)
    day_name            String,           -- Название дня недели
    is_weekend          UInt8,            -- Выходной день
    is_holiday          UInt8,            -- Праздничный день (заполняется отдельно)

    -- Технические поля
    created_at          DateTime DEFAULT now(),
    updated_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (date_sk)
PRIMARY KEY (date_sk)
SETTINGS index_granularity = 8192;

-- ============================================================================
--                    2. ТАБЛИЦЫ ФАКТОВ (FACT TABLES)
-- ============================================================================


-- ============================================================================
-- 2.1 ФАКТ: fact_purchases (заголовки чеков)
-- ============================================================================
-- Источник:      raw.purchases
-- Гранулярность: Одна запись = одна покупка (чек)
-- Ключ:          purchase_id (естественный ключ)
-- Атрибуты:      ID покупки, клиент, магазин, дата, адрес доставки,
--                способ оплаты, общая сумма, количество позиций
-- Связи:         customer_sk → dim_customer
--                store_sk → dim_store
--                purchase_date_sk → dim_date
--                delivery_address_sk → dim_delivery_address
-- Дедупликация:  ReplacingMergeTree по processed_at
-- Партиционирование: По месяцу raw_event_time
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: fact_purchases - факты покупок (заголовки чеков)
-- Источник: Покупки
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.fact_purchases (
    -- Естественный ключ
    purchase_id         String,           -- ID покупки (ord-XXXXX)

    -- Естественные ключи (для отладки и связки с источником)
    customer_id         String,           -- ID клиента (cus-XXXX)
    store_id            String,           -- ID магазина (store-XXX)

    -- Внешние ключи на измерения (суррогатные ключи)
    customer_sk         UInt32,           -- СК клиента
    store_sk            UInt32,           -- СК магазина
    purchase_date_sk    UInt32,           -- СК даты покупки
    delivery_address_sk Nullable(UInt32), -- СК адреса доставки (dim_delivery_address)

    -- Атрибуты покупки
    payment_method      String,           -- Способ оплаты (cash, card, online, etc.)
    total_amount        Decimal(12, 2),   -- Общая сумма покупки
    is_delivery         UInt8,            -- Признак доставки (0/1)
    items_count         UInt8,            -- Количество позиций в чеке

    -- Исходные данные
    raw_event_time      DateTime64(9),    -- Время события из raw

    -- Технические поля
    processed_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(processed_at)
PARTITION BY toYYYYMM(raw_event_time)
ORDER BY (purchase_id)
PRIMARY KEY (purchase_id)
SETTINGS index_granularity = 8192;


-- ============================================================================
-- 2.2 ФАКТ: fact_purchase_items (позиции чеков)
-- ============================================================================
-- Источник:      raw.purchases.items[]
-- Гранулярность: Одна запись = одна позиция в чеке
-- Ключ:          fact_sk (суррогатный ключ записи)
-- Атрибуты:      ID покупки, ID продукта, количество, цена, сумма, КБЖУ
-- Связи:         purchase_id → fact_purchases
--                product_sk → dim_product
--                manufacturer_sk → dim_manufacturer
-- Дедупликация:  ReplacingMergeTree по processed_at
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Таблица: fact_purchase_items - факты позиций покупок (детали чека)
-- Источник: Позиции покупок
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart.fact_purchase_items (
    -- Суррогатный ключ записи
    fact_sk             UInt64,           -- Уникальный ключ записи

    -- Естественные ключи
    purchase_id         String,           -- ID покупки (ord-XXXXX) - связь с fact_purchases
    product_id          String,           -- ID продукта (prd-XXXX)

    -- Внешние ключи на измерения (суррогатные ключи)
    product_sk          UInt32,           -- СК продукта
    manufacturer_sk     Nullable(UInt32), -- СК производителя

    -- Метрики позиции
    quantity            Decimal(10, 3),   -- Количество
    unit                String,           -- Единица измерения
    price_per_unit      Decimal(10, 2),   -- Цена за единицу
    total_item_price    Decimal(12, 2),   -- Сумма по позиции

    -- КБЖУ позиции (денормализовано для быстрой аналитики)
    item_calories       Nullable(Float64),-- Калории
    item_protein        Nullable(Float64),-- Белки
    item_fat            Nullable(Float64),-- Жиры
    item_carbohydrates  Nullable(Float64),-- Углеводы

    -- Категория продукта (денормализовано)
    product_category    String,           -- Категория продукта

    -- Технические поля
    processed_at        DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(processed_at)
ORDER BY (purchase_id, product_id)
PRIMARY KEY (purchase_id)
SETTINGS index_granularity = 8192;



