# Проверка партиций в MART-слое ClickHouse

Команды для проверки состояния партиций и частей таблиц в базе данных `mart`.

---

## 📋 Общая информация по всем таблицам

### Все партиции и их размер

```bash
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    database,
    table,
    partition,
    count() as parts_count,
    sum(rows) as total_rows,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size
FROM system.parts 
WHERE database = 'mart' 
  AND active = 1
GROUP BY database, table, partition
ORDER BY table, partition
"
```

### Сводка по таблицам (количество партиций и частей)

```bash
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    table,
    count(DISTINCT partition) as partitions_count,
    count() as active_parts,
    sum(rows) as total_rows
FROM system.parts 
WHERE database = 'mart' 
  AND active = 1
GROUP BY table
ORDER BY table
"
```

---

## 📊 Таблицы с партиционированием

### fact_purchases (PARTITION BY toYYYYMM(raw_event_time))

```bash
# Все активные части по партициям
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    partition,
    name as part_name,
    rows,
    active
FROM system.parts 
WHERE database = 'mart' 
  AND table = 'fact_purchases' 
  AND active = 1
ORDER BY partition, name
"

# Агрегация по партициям
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    partition,
    count() as parts_count,
    sum(rows) as total_rows,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size
FROM system.parts 
WHERE database = 'mart' 
  AND table = 'fact_purchases' 
  AND active = 1
GROUP BY partition
ORDER BY partition
"

# Общее количество строк в таблице
docker exec clickhouse_airflow clickhouse-client --query "
SELECT count() as total_rows FROM mart.fact_purchases
"

# Проверка на дубликаты
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    count() - count(DISTINCT purchase_id) as duplicates,
    count() as total_rows
FROM mart.fact_purchases
"
```

---

## 📊 Таблицы без партиционирования

Следующие таблицы **не имеют партиционирования** (пустой `partition_key`):

| Таблица | Ключ сортировки |
|---------|-----------------|
| `customer_features_mart` | customer_sk, customer_id |
| `dim_customer` | customer_sk |
| `dim_date` | date_sk |
| `dim_delivery_address` | delivery_address_sk |
| `dim_manufacturer` | manufacturer_sk |
| `dim_product` | product_sk |
| `dim_store` | store_sk |
| `dim_store_location` | store_location_sk |
| `fact_purchase_items` | purchase_id, product_id |

### Проверка для таблиц без партиционирования

```bash
# Количество частей (должно стремиться к 1 после OPTIMIZE)
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    name as part_name,
    rows,
    active
FROM system.parts 
WHERE database = 'mart' 
  AND table = 'dim_customer' 
  AND active = 1
ORDER BY name
"

# Общая статистика
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    count() as parts_count,
    sum(rows) as total_rows,
    formatReadableSize(sum(data_compressed_bytes)) as compressed_size
FROM system.parts 
WHERE database = 'mart' 
  AND table = 'dim_customer' 
  AND active = 1
"

# Проверка на дубликаты
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    count() - count(DISTINCT customer_sk) as duplicates,
    count() as total_rows
FROM mart.dim_customer
"
```

---

## 🔧 Команды для оптимизации

### OPTIMIZE для конкретной таблицы

```bash
# Простое объединение частей (для ReplacingMergeTree)
docker exec clickhouse_airflow clickhouse-client --query "
OPTIMIZE TABLE mart.fact_purchases FINAL
"

# Для всех таблиц dim_*
for table in dim_customer dim_date dim_delivery_address dim_manufacturer dim_product dim_store dim_store_location; do
  docker exec clickhouse_airflow clickhouse-client --query "OPTIMIZE TABLE mart.$table FINAL"
done
```

### Проверка состояния после OPTIMIZE

```bash
# Проверить количество частей (должно быть 1 на партицию)
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    table,
    partition,
    count() as parts_count
FROM system.parts 
WHERE database = 'mart' 
  AND active = 1
GROUP BY table, partition
HAVING parts_count > 1
ORDER BY table, partition
"
```

---

## 📈 Мониторинг слияний (MERGES)

### Текущие слияния

```bash
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    table,
    partition_id,
    result_part_name,
    formatReadableSize(total_size_bytes_compressed) as total_size,
    progress,
    is_mutation
FROM system.merges
WHERE database = 'mart'
ORDER BY elapsed DESC
"
```

### История слияний

```bash
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    table,
    partition_id,
    formatReadableSize(read_rows) as read_rows,
    formatReadableSize(written_rows) as written_rows,
    elapsed_ms / 1000 as elapsed_sec,
    event_date
FROM system.merges_history
WHERE database = 'mart'
  AND event_date >= today() - 7
ORDER BY event_date DESC, elapsed_ms DESC
LIMIT 20
"
```

---

## 🚀 Быстрая проверка всех таблиц (one-liner)

```bash
docker exec clickhouse_airflow clickhouse-client --query "
SELECT 
    table,
    count(DISTINCT partition) as partitions,
    count() as parts,
    sum(rows) as rows,
    formatReadableSize(sum(data_compressed_bytes)) as size
FROM system.parts 
WHERE database = 'mart' 
  AND active = 1
GROUP BY table
ORDER BY table
" FORMAT Table
```

---

## 📝 Шпаргалка

| Метрика | Норма | Тревога |
|---------|-------|---------|
| Parts per partition | 1-3 | > 10 |
| Total parts (fact_purchases) | ≤ 10 | > 50 |
| Total parts (dim_*) | 1-2 | > 5 |
| Duplicates | 0 | > 0 |

---

**Последнее обновление:** 2026-03-08
