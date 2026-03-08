-- ============================================================================
--           MATERIALIZED VIEWS ДЛЯ MART-СЛОЯ ХРАНИЛИЩА ДАННЫХ
-- ============================================================================
-- Проект:      ETL-конвейер для ритейл-аналитики
-- Назначение:  Создание материализованных представлений для витрин данных
-- База данных: mart
-- ============================================================================
-- ВАЖНО:
--   MV в ClickHouse срабатывает при INSERT в исходную таблицу (raw.*).
--   Для дедупликации целевые таблицы используют ReplacingMergeTree.
--   При изменении структуры MV необходимо выполнить DROP + CREATE.
-- ============================================================================
-- ПОРЯДОК ВЫПОЛНЕНИЯ:
--   1. Создать таблицы mart-слоя (01_create_mart_database.sql)
--   2. Выполнить данный скрипт
--   3. Загрузить данные в raw-слой
-- ============================================================================


-- ============================================================================
--                              ОГЛАВЛЕНИЕ
-- ============================================================================
--   1. ИЗМЕРЕНИЯ (DIMENSIONS)
--      1.1  dim_manufacturer   - производители (из products, purchases)
--      1.2  dim_location       - местоположения (из stores, customers, purchases)
--      1.3  dim_product        - продукты
--      1.4  dim_customer       - клиенты
--      1.5  dim_store          - магазины
--      1.6  dim_date           - календарь
--
--   2. ТАБЛИЦЫ ФАКТОВ (FACT TABLES)
--      2.1  fact_purchases     - заголовки чеков
--      2.2  fact_purchase_items - позиции в чеках
-- ============================================================================


-- ============================================================================
--                    1. ИЗМЕРЕНИЯ (DIMENSIONS)
-- ============================================================================


-- ============================================================================
-- 1.1 ИЗМЕРЕНИЕ: dim_manufacturer
-- ============================================================================
-- Источники:     raw.products, raw.purchases
-- Ключ:          manufacturer_sk = xxHash32(manufacturer_inn)
-- Атрибуты:      ИНН, название, страна, сайт
-- Дедупликация:  ReplacingMergeTree по updated_at
-- ============================================================================

-- ----------------------------------------------------------------------------
-- MV: dim_manufacturer из products
-- Источник: Справочник продуктов
-- ----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_dim_manufacturer_from_products
TO mart.dim_manufacturer AS
SELECT
    -- Суррогатный ключ: хеш из ИНН производителя
    xxHash32(
        if(manufacturer_inn = '' OR manufacturer_inn IS NULL, 'unknown', lowerUTF8(trim(manufacturer_inn)))
    ) AS manufacturer_sk,
    
    -- Атрибуты производителя
    if(manufacturer_inn = '' OR manufacturer_inn IS NULL, 'unknown', lowerUTF8(trim(manufacturer_inn))) AS manufacturer_inn,
    if(manufacturer_name = '' OR manufacturer_name IS NULL, 'unknown', lowerUTF8(trim(manufacturer_name))) AS manufacturer_name,
    if(manufacturer_country = '' OR manufacturer_country IS NULL, 'unknown', lowerUTF8(trim(manufacturer_country))) AS manufacturer_country,
    if(manufacturer_website = '' OR manufacturer_website IS NULL, '', lowerUTF8(trim(manufacturer_website))) AS manufacturer_website,
    
    -- Технические поля
    now() AS created_at,
    now() AS updated_at
FROM raw.products
WHERE manufacturer_inn != '' AND manufacturer_inn IS NOT NULL;


-- ============================================================================
-- 1.2 ИЗМЕРЕНИЕ: dim_store_location
-- ============================================================================
-- Источники:     raw.stores, raw.customers.purchase_location, raw.purchases.store
-- Ключ:          store_location_sk = xxHash32(country, city, street, house)
-- Атрибуты:      Страна, город, улица, дом, почтовый индекс, координаты
-- Дедупликация:  ReplacingMergeTree по updated_at
-- Особенности:   Координаты валидируются на диапазоны (lat: -90..90, lon: -180..180)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- MV: dim_store_location из stores
-- Источник: Справочник магазинов
-- ----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_dim_store_location_from_stores
TO mart.dim_store_location AS
SELECT
    -- Суррогатный ключ: хеш из адресных компонентов (страна, город, улица, дом)
    xxHash32(
        if(location_country = '' OR location_country IS NULL, 'unknown', lowerUTF8(trim(location_country))),
        if(location_city = '' OR location_city IS NULL, 'unknown', lowerUTF8(trim(location_city))),
        if(location_street = '' OR location_street IS NULL, 'unknown', lowerUTF8(trim(location_street))),
        if(location_house = '' OR location_house IS NULL, 'unknown', lowerUTF8(trim(location_house)))
    ) AS store_location_sk,
    
    -- Адресные атрибуты
    if(location_country = '' OR location_country IS NULL, 'unknown', lowerUTF8(trim(location_country))) AS country,
    if(location_city = '' OR location_city IS NULL, 'unknown', lowerUTF8(trim(location_city))) AS city,
    if(location_street = '' OR location_street IS NULL, 'unknown', lowerUTF8(trim(location_street))) AS street,
    if(location_house = '' OR location_house IS NULL, 'unknown', lowerUTF8(trim(location_house))) AS house,
    if(location_postal_code = '' OR location_postal_code IS NULL, '', trim(location_postal_code)) AS postal_code,
    
    -- Координаты с валидацией: широта [-90, 90], долгота [-180, 180]
    if(location_coordinates_latitude = '' OR location_coordinates_latitude IS NULL 
       OR toFloat64OrNull(trim(location_coordinates_latitude)) IS NULL
       OR toFloat64OrNull(trim(location_coordinates_latitude)) < -90 
       OR toFloat64OrNull(trim(location_coordinates_latitude)) > 90,
       NULL,
       toFloat64OrNull(trim(location_coordinates_latitude))) AS latitude,
    
    if(location_coordinates_longitude = '' OR location_coordinates_longitude IS NULL 
       OR toFloat64OrNull(trim(location_coordinates_longitude)) IS NULL
       OR toFloat64OrNull(trim(location_coordinates_longitude)) < -180 
       OR toFloat64OrNull(trim(location_coordinates_longitude)) > 180,
       NULL,
       toFloat64OrNull(trim(location_coordinates_longitude))) AS longitude,
    
    -- Технические поля для аудита и дедупликации
    now() AS created_at,
    now() AS updated_at
FROM raw.stores;


-- ============================================================================
-- 1.2.1 ИЗМЕРЕНИЕ: dim_delivery_address
-- ============================================================================
-- Источники:     raw.customers.delivery_address, raw.purchases.delivery_address
-- Ключ:          delivery_address_sk = xxHash32(country, city, street, house, apartment)
-- Атрибуты:      Страна, город, улица, дом, квартира, почтовый индекс
-- Дедупликация:  ReplacingMergeTree по updated_at
-- Особенности:   Без координат, содержит номер квартиры
-- ============================================================================

-- ----------------------------------------------------------------------------
-- MV: dim_delivery_address из customers (delivery_address)
-- Источник: Адрес доставки клиента
-- Особенность: Содержит номер квартиры; координаты отсутствуют
-- ----------------------------------------------------------------------------

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_dim_delivery_address_from_customers
TO mart.dim_delivery_address AS
SELECT
    -- Суррогатный ключ: хеш из адресных компонентов (включая квартиру)
    xxHash32(
        if(delivery_address_country = '' OR delivery_address_country IS NULL, 'unknown', lowerUTF8(trim(delivery_address_country))),
        if(delivery_address_city = '' OR delivery_address_city IS NULL, 'unknown', lowerUTF8(trim(delivery_address_city))),
        if(delivery_address_street = '' OR delivery_address_street IS NULL, 'unknown', lowerUTF8(trim(delivery_address_street))),
        if(delivery_address_house = '' OR delivery_address_house IS NULL, 'unknown', lowerUTF8(trim(delivery_address_house))),
        if(delivery_address_apartment = '' OR delivery_address_apartment IS NULL, '', lowerUTF8(trim(delivery_address_apartment)))
    ) AS delivery_address_sk,
    
    -- Адресные атрибуты
    if(delivery_address_country = '' OR delivery_address_country IS NULL, 'unknown', lowerUTF8(trim(delivery_address_country))) AS country,
    if(delivery_address_city = '' OR delivery_address_city IS NULL, 'unknown', lowerUTF8(trim(delivery_address_city))) AS city,
    if(delivery_address_street = '' OR delivery_address_street IS NULL, 'unknown', lowerUTF8(trim(delivery_address_street))) AS street,
    if(delivery_address_house = '' OR delivery_address_house IS NULL, 'unknown', lowerUTF8(trim(delivery_address_house))) AS house,
    if(delivery_address_apartment = '' OR delivery_address_apartment IS NULL, '', lowerUTF8(trim(delivery_address_apartment))) AS apartment,
    if(delivery_address_postal_code = '' OR delivery_address_postal_code IS NULL, '', trim(delivery_address_postal_code)) AS postal_code,
    
    -- Технические поля
    now() AS created_at,
    now() AS updated_at
FROM raw.customers
WHERE delivery_address_country != '' AND delivery_address_country IS NOT NULL;


-- ============================================================================
-- 1.3 ИЗМЕРЕНИЕ: dim_product
-- ============================================================================
-- Источник:      raw.products
-- Ключ:          product_sk = xxHash32(id)
-- Атрибуты:      ИД, название, группа, описание, КБЖУ, цена, единица измерения,
--                страна происхождения, срок годности, органик, штрихкод
-- Связи:         manufacturer_sk → dim_manufacturer
-- Дедупликация:  ReplacingMergeTree по updated_at
-- Валидация:     КБЖУ ≥ 0, цена ≥ 0, срок годности > 0
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_dim_product
TO mart.dim_product AS
SELECT
    -- Суррогатный ключ: хеш из ИД продукта
    xxHash32(id) AS product_sk,
    
    -- Натуральный ключ и основные атрибуты
    if(id = '' OR id IS NULL, 'unknown', lowerUTF8(trim(id))) AS product_id,
    if(name = '' OR name IS NULL, 'unknown', lowerUTF8(trim(name))) AS product_name,
    if(`group` = '' OR `group` IS NULL, 'unknown', 
           lowerUTF8(trim(replaceRegexpAll(`group`, '[^а-яА-ЯёЁa-zA-Z0-9\s-]', '')))) AS product_group,
    if(description = '' OR description IS NULL, '', lowerUTF8(trim(description))) AS product_description,
    
    -- КБЖУ: валидация на неотрицательные значения
    if(kbju_calories = '' OR kbju_calories IS NULL 
       OR toFloat64OrNull(trim(kbju_calories)) IS NULL
       OR toFloat64OrNull(trim(kbju_calories)) < 0,
       NULL,
       toFloat64OrNull(trim(kbju_calories))) AS calories,
    
    if(kbju_protein = '' OR kbju_protein IS NULL 
       OR toFloat64OrNull(trim(kbju_protein)) IS NULL
       OR toFloat64OrNull(trim(kbju_protein)) < 0,
       NULL,
       toFloat64OrNull(trim(kbju_protein))) AS protein,
    
    if(kbju_fat = '' OR kbju_fat IS NULL 
       OR toFloat64OrNull(trim(kbju_fat)) IS NULL
       OR toFloat64OrNull(trim(kbju_fat)) < 0,
       NULL,
       toFloat64OrNull(trim(kbju_fat))) AS fat,
    
    if(kbju_carbohydrates = '' OR kbju_carbohydrates IS NULL 
       OR toFloat64OrNull(trim(kbju_carbohydrates)) IS NULL
       OR toFloat64OrNull(trim(kbju_carbohydrates)) < 0,
       NULL,
       toFloat64OrNull(trim(kbju_carbohydrates))) AS carbohydrates,
    
    -- Цена: Decimal(18,2) для точных денежных расчётов
    if(price = '' OR price IS NULL 
       OR toDecimal128OrNull(trim(price), 2) IS NULL
       OR toDecimal128OrNull(trim(price), 2) < 0,
       NULL,
       toDecimal128OrNull(trim(price), 2)) AS price,
    
    -- Дополнительные атрибуты
    if(unit = '' OR unit IS NULL, 'unknown', lowerUTF8(trim(unit))) AS unit,
    if(origin_country = '' OR origin_country IS NULL, 'unknown', lowerUTF8(trim(origin_country))) AS origin_country,
    
    -- Срок годности: NULL если 0 или невалидное значение
    if(expiry_days = '' OR expiry_days IS NULL 
       OR toUInt16OrNull(trim(expiry_days)) IS NULL
       OR toUInt16OrNull(trim(expiry_days)) = 0,
       NULL,
       toUInt16OrNull(trim(expiry_days))) AS expiry_days,
    
    -- Флаг "органик": нормализация разных форматов (true/1/yes → 1)
    if(is_organic = '' OR is_organic IS NULL, 0,
       if(lowerUTF8(trim(is_organic)) IN ('true', '1', 'yes'), 1, 0)) AS is_organic,
    
    if(barcode = '' OR barcode IS NULL, '', trim(barcode)) AS barcode,
    
    -- Внешний ключ к производителю (NULL если ИНН не указан)
    if(manufacturer_inn = '' OR manufacturer_inn IS NULL, NULL,
       xxHash32(lowerUTF8(trim(manufacturer_inn)))) AS manufacturer_sk,
    
    -- Технические поля для аудита и дедупликации
    now() AS created_at,
    now() AS updated_at
FROM raw.products;


-- ============================================================================
-- 1.4 ИЗМЕРЕНИЕ: dim_customer
-- ============================================================================
-- Источник:      raw.customers
-- Ключ:          customer_sk = xxHash32(customer_id)
-- Атрибуты:      ИД, ФИО, email, телефон, дата рождения, пол, дата регистрации,
--                участие в программе лояльности, предпочтения
-- Связи:         purchase_location_sk → dim_store_location (адрес покупок)
--                delivery_address_sk → dim_delivery_address (адрес доставки)
-- Дедупликация:  ReplacingMergeTree по updated_at
-- Валидация:     birth_date в диапазоне [1900-01-01, today()]
--                registration_date ≤ now()
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_dim_customer
TO mart.dim_customer AS
SELECT
    -- Суррогатный ключ: хеш из ИД клиента
    xxHash32(customer_id) AS customer_sk,
    
    -- Натуральный ключ и ФИО
    if(customer_id = '' OR customer_id IS NULL, 'unknown', lowerUTF8(trim(customer_id))) AS customer_id,
    if(first_name = '' OR first_name IS NULL, 'unknown', lowerUTF8(trim(first_name))) AS first_name,
    if(last_name = '' OR last_name IS NULL, 'unknown', lowerUTF8(trim(last_name))) AS last_name,
    
    -- Вычисляемое полное имя
    lowerUTF8(concat(
        if(first_name = '' OR first_name IS NULL, 'unknown', trim(first_name)),
        ' ',
        if(last_name = '' OR last_name IS NULL, 'unknown', trim(last_name))
    )) AS full_name,
    
    -- Контактные данные
    if(email = '' OR email IS NULL, 'unknown', lowerUTF8(trim(email))) AS email,
    if(phone = '' OR phone IS NULL, '', trim(phone)) AS phone,
    
    -- Дата рождения: валидация на разумный диапазон [1900, сегодня]
    if(birth_date = '' OR birth_date IS NULL
       OR toDateOrNull(trim(birth_date)) IS NULL
       OR toDateOrNull(trim(birth_date)) > today()
       OR toDateOrNull(trim(birth_date)) < toDate('1900-01-01'),
       NULL,
       toDateOrNull(trim(birth_date))) AS birth_date,
    
    if(gender = '' OR gender IS NULL, 'unknown', lowerUTF8(trim(gender))) AS gender,
    
    -- Дата регистрации: не может быть в будущем
     toDateTimeOrNull(substring(trimBoth(registration_date), 1, 19)) AS registration_date,
    
    -- Программа лояльности
    if(is_loyalty_member = '' OR is_loyalty_member IS NULL, 0,
       if(lowerUTF8(trim(is_loyalty_member)) IN ('true', '1', 'yes'), 1, 0)) AS is_loyalty_member,
    
    if(loyalty_card_number = '' OR loyalty_card_number IS NULL, '',
       lowerUTF8(trim(loyalty_card_number))) AS loyalty_card_number,
    
    -- Предпочтения клиента
    if(preferences_preferred_language = '' OR preferences_preferred_language IS NULL, 'unknown',
       lowerUTF8(trim(preferences_preferred_language))) AS preferred_language,
    
    if(preferences_preferred_payment_method = '' OR preferences_preferred_payment_method IS NULL, 'unknown',
       lowerUTF8(trim(preferences_preferred_payment_method))) AS preferred_payment_method,
    
    if(preferences_receive_promotions = '' OR preferences_receive_promotions IS NULL, 0,
       if(lowerUTF8(trim(preferences_receive_promotions)) IN ('true', '1', 'yes'), 1, 0)) AS receive_promotions,
    
    -- Внешний ключ к локации покупок (dim_store_location)
    if(purchase_location_country = '' OR purchase_location_country IS NULL, NULL,
       xxHash32(
           lowerUTF8(trim(purchase_location_country)),
           lowerUTF8(trim(purchase_location_city)),
           lowerUTF8(trim(purchase_location_street)),
           lowerUTF8(trim(purchase_location_house))
       )) AS purchase_location_sk,
    
    -- Внешний ключ к адресу доставки (dim_delivery_address) - включает квартиру
    if(delivery_address_country = '' OR delivery_address_country IS NULL, NULL,
       xxHash32(
           lowerUTF8(trim(delivery_address_country)),
           lowerUTF8(trim(delivery_address_city)),
           lowerUTF8(trim(delivery_address_street)),
           lowerUTF8(trim(delivery_address_house)),
           if(delivery_address_apartment = '' OR delivery_address_apartment IS NULL, '', lowerUTF8(trim(delivery_address_apartment)))
       )) AS delivery_address_sk,
    
    -- Технические поля для аудита и дедупликации
    now() AS created_at,
    now() AS updated_at
FROM raw.customers;


-- ============================================================================
-- 1.5 ИЗМЕРЕНИЕ: dim_store
-- ============================================================================
-- Источник:      raw.stores
-- Ключ:          store_sk = xxHash32(store_id)
-- Атрибуты:      ИД, название, сеть, тип, категории, менеджер, часы работы,
--                флаги (online-заказы, доставка, связь со складом)
-- Связи:         store_location_sk → dim_store_location
-- Дедупликация:  ReplacingMergeTree по updated_at
-- Валидация:     last_inventory_date ≤ today()
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_dim_store
TO mart.dim_store AS
SELECT
    -- Суррогатный ключ: хеш из ИД магазина
    xxHash32(store_id) AS store_sk,
    
    -- Натуральный ключ и основные атрибуты
    if(store_id = '' OR store_id IS NULL, 'unknown', lowerUTF8(trim(store_id))) AS store_id,
    if(store_name = '' OR store_name IS NULL, 'unknown', lowerUTF8(trim(store_name))) AS store_name,
    if(store_network = '' OR store_network IS NULL, 'unknown', lowerUTF8(trim(store_network))) AS store_network,
    if(store_type_description = '' OR store_type_description IS NULL, '',
        lowerUTF8(trim(store_type_description))) AS store_type_description,
    if(`type` = '' OR `type` IS NULL, 'unknown', lowerUTF8(trim(`type`))) AS store_type,
    
    -- Категории товаров в магазине
    if(categories = '' OR categories IS NULL, '', 
           lowerUTF8(trim(replaceRegexpAll(categories, '[^а-яА-ЯёЁa-zA-Z0-9\s,.-]', '')))) AS categories,
    
    -- Контактные данные менеджера
    if(manager_name = '' OR manager_name IS NULL, '', lowerUTF8(trim(manager_name))) AS manager_name,
    if(manager_phone = '' OR manager_phone IS NULL, '', trim(manager_phone)) AS manager_phone,
    if(manager_email = '' OR manager_email IS NULL, '', lowerUTF8(trim(manager_email))) AS manager_email,
    
    -- Часы работы по дням недели
    if(opening_hours_mon_fri = '' OR opening_hours_mon_fri IS NULL, '', trim(opening_hours_mon_fri)) AS opening_hours_mon_fri,
    if(opening_hours_sat = '' OR opening_hours_sat IS NULL, '', trim(opening_hours_sat)) AS opening_hours_sat,
    if(opening_hours_sun = '' OR opening_hours_sun IS NULL, '', trim(opening_hours_sun)) AS opening_hours_sun,
    
    -- Функциональные флаги (нормализация true/1/yes → 1)
    if(accepts_online_orders = '' OR accepts_online_orders IS NULL, 0,
       if(lowerUTF8(trim(accepts_online_orders)) IN ('true', '1', 'yes'), 1, 0)) AS accepts_online_orders,
    
    if(delivery_available = '' OR delivery_available IS NULL, 0,
       if(lowerUTF8(trim(delivery_available)) IN ('true', '1', 'yes'), 1, 0)) AS delivery_available,
    
    if(warehouse_connected = '' OR warehouse_connected IS NULL, 0,
       if(lowerUTF8(trim(warehouse_connected)) IN ('true', '1', 'yes'), 1, 0)) AS warehouse_connected,
    
    -- Дата последней инвентаризации: не может быть в будущем
    if(last_inventory_date = '' OR last_inventory_date IS NULL
       OR toDateOrNull(trim(last_inventory_date)) IS NULL
       OR toDateOrNull(trim(last_inventory_date)) > today(),
       NULL,
       toDateOrNull(trim(last_inventory_date))) AS last_inventory_date,
    
    -- Внешний ключ к локации магазина (dim_store_location)
    if(location_country = '' OR location_country IS NULL, NULL,
       xxHash32(
           lowerUTF8(trim(location_country)),
           lowerUTF8(trim(location_city)),
           lowerUTF8(trim(location_street)),
           lowerUTF8(trim(location_house))
       )) AS store_location_sk,
    
    -- Технические поля для аудита и дедупликации
    now() AS created_at,
    now() AS updated_at
FROM raw.stores;


-- ============================================================================
-- 1.6 ИЗМЕРЕНИЕ: dim_date (календарь)
-- ============================================================================
-- Источник:      raw.purchases.purchase_datetime (извлечение уникальных дат)
-- Гранулярность: Одна строка = один день
-- Ключ:          date_sk = YYYYMMDD (числовой формат, например 20241215)
-- Атрибуты:      Год, квартал, месяц, неделя, день
--                (названия на русском языке)
-- Флаги:         is_leap_year, is_weekend, is_holiday
-- Особенность:   Даты > today() исключаются (нельзя иметь покупки в будущем)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_dim_date
TO mart.dim_date AS
SELECT DISTINCT
    -- Суррогатный ключ: числовой формат YYYYMMDD
    toUInt32(formatDateTime(assumeNotNull(date_val), '%Y%m%d')) AS date_sk,
    assumeNotNull(date_val) AS full_date,
    
    -- Год
    toYear(assumeNotNull(date_val)) AS year,
    concat('Год ', toString(toYear(assumeNotNull(date_val)))) AS year_name,
    -- Високосный год: делится на 4, но не на 100, либо делится на 400
    toUInt8((toYear(assumeNotNull(date_val)) % 4 = 0) 
        AND ((toYear(assumeNotNull(date_val)) % 100 != 0) 
             OR (toYear(assumeNotNull(date_val)) % 400 = 0))) AS is_leap_year,
    
    -- Квартал
    toQuarter(assumeNotNull(date_val)) AS quarter,
    concat('Q', toString(toQuarter(assumeNotNull(date_val)))) AS quarter_name,
    concat(toString(toYear(assumeNotNull(date_val))), '-Q', toString(toQuarter(assumeNotNull(date_val)))) AS year_quarter,
    
    -- Месяц
    toMonth(assumeNotNull(date_val)) AS month,
    ['Январь', 'Февраль', 'Март', 'Апрель', 'Май', 'Июнь', 
     'Июль', 'Август', 'Сентябрь', 'Октябрь', 'Ноябрь', 'Декабрь'][toMonth(assumeNotNull(date_val))] AS month_name,
    formatDateTime(assumeNotNull(date_val), '%Y-%m') AS year_month,
    toDayOfMonth(toLastDayOfMonth(assumeNotNull(date_val))) AS days_in_month,
    
    -- Неделя
    toWeek(assumeNotNull(date_val), 1) AS week_of_year,
    toUInt8(toWeek(assumeNotNull(date_val), 1) - toWeek(toStartOfMonth(assumeNotNull(date_val)), 1) + 1) AS week_of_month,
    concat(toString(toYear(assumeNotNull(date_val))), '-W', lpad(toString(toWeek(assumeNotNull(date_val), 1)), 2, '0')) AS year_week,
    
    -- День
    toDayOfMonth(assumeNotNull(date_val)) AS day_of_month,
    toDayOfYear(assumeNotNull(date_val)) AS day_of_year,
    -- День недели: 1=Пн ... 7=Вс (ISO 8601)
    if(toDayOfWeek(assumeNotNull(date_val)) = 7, toUInt8(7), toDayOfWeek(assumeNotNull(date_val))) AS day_of_week,
    ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье'][
        if(toDayOfWeek(assumeNotNull(date_val)) = 7, toUInt8(7), toDayOfWeek(assumeNotNull(date_val)))
    ] AS day_name,
    
    -- Флаги выходного и праздничного дня
    if(toDayOfWeek(assumeNotNull(date_val)) >= 6, toUInt8(1), toUInt8(0)) AS is_weekend,
    toUInt8(0) AS is_holiday,  -- Можно обогатить справочником праздников

    -- Технические поля
    now() AS created_at,
    now() AS updated_at
FROM (
    SELECT toDateOrNull(trim(purchase_datetime)) AS date_val
    FROM raw.purchases
    WHERE purchase_datetime != '' AND purchase_datetime IS NOT NULL
)
WHERE date_val IS NOT NULL AND date_val <= today();


-- ============================================================================
-- 2. ТАБЛИЦЫ ФАКТОВ (FACT TABLES)
-- ============================================================================


-- ============================================================================
-- 2.1 ФАКТ: fact_purchases (заголовки чеков)
-- ============================================================================
-- Источник:      raw.purchases
-- Гранулярность: Одна строка = один чек (заголовок)
-- Ключи:         customer_sk → dim_customer
--                store_sk → dim_store
--                purchase_date_sk → dim_date
--                delivery_address_sk → dim_delivery_address
-- Метрики:       total_amount (сумма чека), items_count (кол-во позиций)
-- Атрибуты:      payment_method, is_delivery, raw_event_time
-- Валидация:     total_amount ≥ 0, purchase_datetime ≤ now()
-- Особенность:   items_count вычисляется через length(items.product_id)
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_fact_purchases
TO mart.fact_purchases AS
SELECT
    -- Натуральный ключ чека
    if(purchase_id = '' OR purchase_id IS NULL, 'unknown', lowerUTF8(trim(purchase_id))) AS purchase_id,
    
    -- Натуральные ключи клиента и магазина
    if(customer_customer_id = '' OR customer_customer_id IS NULL, 'unknown', lowerUTF8(trim(customer_customer_id))) AS customer_id,
    if(store_store_id = '' OR store_store_id IS NULL, 'unknown', lowerUTF8(trim(store_store_id))) AS store_id,
    
    -- Суррогатные ключи (внешние ключи к измерениям)
    xxHash32(lowerUTF8(trim(customer_customer_id))) AS customer_sk,
    xxHash32(lowerUTF8(trim(store_store_id))) AS store_sk,
    
    -- Ключ даты: числовой формат YYYYMMDD для связи с dim_date
    toUInt32(formatDateTime(
        if(purchase_datetime = '' OR purchase_datetime IS NULL
           OR toDateTimeOrNull(trim(purchase_datetime)) IS NULL
           OR toDateTimeOrNull(trim(purchase_datetime)) > now(),
           now(),
           toDateTimeOrNull(trim(purchase_datetime))),
        '%Y%m%d'
    )) AS purchase_date_sk,
    
    -- Внешний ключ к адресу доставки (dim_delivery_address) - включает квартиру
    if(delivery_address_country = '' OR delivery_address_country IS NULL, NULL,
       xxHash32(
           lowerUTF8(trim(delivery_address_country)),
           lowerUTF8(trim(delivery_address_city)),
           lowerUTF8(trim(delivery_address_street)),
           lowerUTF8(trim(delivery_address_house)),
           if(delivery_address_apartment = '' OR delivery_address_apartment IS NULL, '', lowerUTF8(trim(delivery_address_apartment)))
       )) AS delivery_address_sk,
    
    -- Атрибуты транзакции
    if(payment_method = '' OR payment_method IS NULL, 'unknown', lowerUTF8(trim(payment_method))) AS payment_method,
    
    -- Метрика: сумма чека (валидация на неотрицательное значение)
    if(total_amount = '' OR total_amount IS NULL 
       OR toDecimal128OrNull(trim(total_amount), 2) IS NULL
       OR toDecimal128OrNull(trim(total_amount), 2) < 0,
       toDecimal64(0, 2),
       toDecimal128OrNull(trim(total_amount), 2)) AS total_amount,
    
    -- Флаг доставки (нормализация true/1/yes → 1)
    if(is_delivery = '' OR is_delivery IS NULL, 0,
       if(lowerUTF8(trim(is_delivery)) IN ('true', '1', 'yes'), 1, 0)) AS is_delivery,
    
    -- Счётчик позиций в чеке: вычисляется из длины массива items.product_id
    toUInt8(length(`items.product_id`)) AS items_count,
    
    -- Исходная метка времени с миллисекундами для аналитики
    if(purchase_datetime = '' OR purchase_datetime IS NULL
       OR toDateTime64OrNull(trim(purchase_datetime), 9) IS NULL
       OR toDateTime64OrNull(trim(purchase_datetime), 9) > now64(9),
       now64(9),
       toDateTime64OrNull(trim(purchase_datetime), 9)) AS raw_event_time,
    
    -- Техническое поле: время обработки записи
    now() AS processed_at
FROM raw.purchases
WHERE purchase_id != '' AND purchase_id IS NOT NULL;


-- ============================================================================
-- 2.2 ФАКТ: fact_purchase_items (позиции в чеках)
-- ============================================================================
-- Источник:      raw.purchases (Nested-массив items)
-- Гранулярность: Одна строка = одна позиция в чеке
-- Ключи:         product_sk → dim_product
--                manufacturer_sk → dim_manufacturer
--                purchase_id → fact_purchases (связь с заголовком)
-- Метрики:       quantity, price_per_unit, total_item_price
--                item_calories, item_protein, item_fat, item_carbohydrates
-- Атрибуты:      unit, product_category
-- Валидация:     quantity > 0, price_per_unit ≥ 0, total_item_price ≥ 0
-- Особенность:   Использует ARRAY JOIN для разворачивания Nested-массивов
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mart.mv_fact_purchase_items
TO mart.fact_purchase_items AS
SELECT
    -- Суррогатный ключ: хеш из purchase_id + product_id
    xxHash64(purchase_id, `items.product_id`) AS fact_sk,
    
    -- Натуральные ключи чека и продукта
    if(purchase_id = '' OR purchase_id IS NULL, 'unknown', lowerUTF8(trim(purchase_id))) AS purchase_id,
    if(`items.product_id` = '' OR `items.product_id` IS NULL, 'unknown', lowerUTF8(trim(`items.product_id`))) AS product_id,
    
    -- Внешние ключи к измерениям
    xxHash32(lowerUTF8(trim(`items.product_id`))) AS product_sk,
    
    if(`items.manufacturer_inn` = '' OR `items.manufacturer_inn` IS NULL, NULL,
       xxHash32(lowerUTF8(trim(`items.manufacturer_inn`)))) AS manufacturer_sk,
    
    -- Метрики: количество и цена
    if(`items.quantity` = '' OR `items.quantity` IS NULL 
       OR toDecimal128OrNull(trim(`items.quantity`), 3) IS NULL
       OR toDecimal128OrNull(trim(`items.quantity`), 3) <= 0,
       toDecimal64(1, 3),
       toDecimal128OrNull(trim(`items.quantity`), 3)) AS quantity,
    
    if(`items.unit` = '' OR `items.unit` IS NULL, 'unknown', lowerUTF8(trim(`items.unit`))) AS unit,
    
    if(`items.price_per_unit` = '' OR `items.price_per_unit` IS NULL 
       OR toDecimal128OrNull(trim(`items.price_per_unit`), 2) IS NULL
       OR toDecimal128OrNull(trim(`items.price_per_unit`), 2) < 0,
       toDecimal64(0, 2),
       toDecimal128OrNull(trim(`items.price_per_unit`), 2)) AS price_per_unit,
    
    if(`items.total_price` = '' OR `items.total_price` IS NULL 
       OR toDecimal128OrNull(trim(`items.total_price`), 2) IS NULL
       OR toDecimal128OrNull(trim(`items.total_price`), 2) < 0,
       toDecimal64(0, 2),
       toDecimal128OrNull(trim(`items.total_price`), 2)) AS total_item_price,
    
    -- КБЖУ позиции (валидация на неотрицательные значения)
    if(`items.kbju_calories` = '' OR `items.kbju_calories` IS NULL 
       OR toFloat64OrNull(trim(`items.kbju_calories`)) IS NULL
       OR toFloat64OrNull(trim(`items.kbju_calories`)) < 0,
       NULL, toFloat64OrNull(trim(`items.kbju_calories`))) AS item_calories,
    
    if(`items.kbju_protein` = '' OR `items.kbju_protein` IS NULL 
       OR toFloat64OrNull(trim(`items.kbju_protein`)) IS NULL
       OR toFloat64OrNull(trim(`items.kbju_protein`)) < 0,
       NULL, toFloat64OrNull(trim(`items.kbju_protein`))) AS item_protein,
    
    if(`items.kbju_fat` = '' OR `items.kbju_fat` IS NULL 
       OR toFloat64OrNull(trim(`items.kbju_fat`)) IS NULL
       OR toFloat64OrNull(trim(`items.kbju_fat`)) < 0,
       NULL, toFloat64OrNull(trim(`items.kbju_fat`))) AS item_fat,
    
    if(`items.kbju_carbohydrates` = '' OR `items.kbju_carbohydrates` IS NULL 
       OR toFloat64OrNull(trim(`items.kbju_carbohydrates`)) IS NULL
       OR toFloat64OrNull(trim(`items.kbju_carbohydrates`)) < 0,
       NULL, toFloat64OrNull(trim(`items.kbju_carbohydrates`))) AS item_carbohydrates,
    
    -- Категория продукта
    if(`items.category` = '' OR `items.category` IS NULL, 'unknown', 
           lowerUTF8(trim(replaceRegexpAll(`items.category`, '[^а-яА-ЯёЁa-zA-Z0-9\s-]', '')))) AS product_category,
    
    -- Техническое поле: время обработки записи
    now() AS processed_at
FROM raw.purchases
ARRAY JOIN
    `items.product_id` AS `items.product_id`,
    `items.name` AS `items.name`,
    `items.category` AS `items.category`,
    `items.quantity` AS `items.quantity`,
    `items.unit` AS `items.unit`,
    `items.price_per_unit` AS `items.price_per_unit`,
    `items.total_price` AS `items.total_price`,
    `items.kbju_calories` AS `items.kbju_calories`,
    `items.kbju_protein` AS `items.kbju_protein`,
    `items.kbju_fat` AS `items.kbju_fat`,
    `items.kbju_carbohydrates` AS `items.kbju_carbohydrates`,
    `items.manufacturer_name` AS `items.manufacturer_name`,
    `items.manufacturer_country` AS `items.manufacturer_country`,
    `items.manufacturer_website` AS `items.manufacturer_website`,
    `items.manufacturer_inn` AS `items.manufacturer_inn`
WHERE `items.product_id` != '' AND `items.product_id` IS NOT NULL;