-- =====================================================================
-- П.2. Базовая (неоптимизированная) схема: Kafka -> MV -> MergeTree
-- =====================================================================
-- Типы взяты «как в CSV»: строки — String, дробные — Float64,
-- сортировки нет. Это точка отсчёта для сравнения с 02_ddl_optimized.sql.

CREATE DATABASE IF NOT EXISTS fraud;

-- 1) Таблица-«окно» в топик Kafka. Данные в ней не хранятся:
--    каждое чтение забирает новые сообщения consumer group'ой.
CREATE TABLE IF NOT EXISTS fraud.kafka_transactions_baseline
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        Int64,
    lat              Float64,
    lon              Float64,
    population_city  Int64,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_baseline',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

-- 2) Таблица хранения
CREATE TABLE IF NOT EXISTS fraud.transactions_baseline
(
    transaction_time String,
    merch            String,
    cat_id           String,
    amount           Float64,
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        Int64,
    lat              Float64,
    lon              Float64,
    population_city  Int64,
    jobs             String,
    merchant_lat     Float64,
    merchant_lon     Float64,
    target           Int64
)
ENGINE = MergeTree
ORDER BY tuple();

-- 3) Materialized view переносит сообщения из Kafka-таблицы в MergeTree
CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.mv_transactions_baseline
TO fraud.transactions_baseline
AS
SELECT *
FROM fraud.kafka_transactions_baseline;
