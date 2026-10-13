-- =====================================================================
-- П.4. Оптимизированная схема (изменённый DDL из п.2)
-- =====================================================================
-- Что изменено относительно 01_ddl_baseline.sql:
--   * типы:  DateTime вместо String, Decimal32(2) для суммы, Float32 для
--            координат, UInt* вместо Int64, Enum8 для пола, UInt8 для target;
--   * LowCardinality(String) для колонок с малым числом уникальных значений
--            (штат — 50, категория — 14, мерчант/город/профессия — сотни);
--   * ORDER BY (us_state, cat_id, transaction_time) — данные одного штата
--            лежат рядом: GROUP BY us_state читает их последовательно,
--            фильтры по штату/категории отсекают гранулы по первичному ключу;
--   * без PARTITION BY: запрос не фильтрует по дате, а на таком объёме
--            помесячные партиции лишь дробят данные на мелкие куски
--            (проверено: +12% к размеру на диске, см. README);
--   * кодеки сжатия: Delta+ZSTD для времени, ZSTD для остального;
--   * PROJECTION с предагрегацией «максимальная транзакция по штату» —
--            запрос из п.3 читает не все строки, а по строке на штат в каждом куске данных.
-- Запрос из п.3 при этом не меняется: ClickHouse сам выбирает проекцию.

CREATE DATABASE IF NOT EXISTS fraud;

-- Отдельная consumer group: оптимизированная таблица получает
-- тот же поток сообщений независимо от базовой.
CREATE TABLE IF NOT EXISTS fraud.kafka_transactions
(
    transaction_time String,                 -- '2019-12-27 15:21', разбирается в MV
    merch            String,
    cat_id           String,
    amount           Decimal32(2),           -- 148.04 разбирается точно, без Float
    name_1           String,
    name_2           String,
    gender           String,
    street           String,
    one_city         String,
    us_state         String,
    post_code        UInt32,
    lat              Float32,
    lon              Float32,
    population_city  UInt32,
    jobs             String,
    merchant_lat     Float32,
    merchant_lon     Float32,
    target           UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'clickhouse_optimized',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS fraud.transactions
(
    transaction_time DateTime                 CODEC(Delta, ZSTD(1)),
    merch            LowCardinality(String),
    cat_id           LowCardinality(String),
    amount           Decimal32(2)             CODEC(ZSTD(1)),
    name_1           LowCardinality(String),
    name_2           LowCardinality(String),
    gender           Enum8('F' = 1, 'M' = 2),
    street           String                   CODEC(ZSTD(3)),
    one_city         LowCardinality(String),
    us_state         LowCardinality(String),
    post_code        UInt32                   CODEC(ZSTD(1)),
    lat              Float32                  CODEC(ZSTD(1)),
    lon              Float32                  CODEC(ZSTD(1)),
    population_city  UInt32                   CODEC(ZSTD(1)),
    jobs             LowCardinality(String),
    merchant_lat     Float32                  CODEC(ZSTD(1)),
    merchant_lon     Float32                  CODEC(ZSTD(1)),
    target           UInt8,

    -- предагрегация под запрос «категория наибольшей транзакции по штату»
    PROJECTION prj_max_amount_by_state
    (
        SELECT
            us_state,
            argMax(cat_id, amount),
            max(amount)
        GROUP BY us_state
    )
)
ENGINE = MergeTree
ORDER BY (us_state, cat_id, transaction_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS fraud.mv_transactions
TO fraud.transactions
AS
SELECT
    parseDateTimeBestEffort(transaction_time) AS transaction_time,
    merch,
    cat_id,
    amount,
    name_1,
    name_2,
    CAST(gender AS Enum8('F' = 1, 'M' = 2)) AS gender,
    street,
    one_city,
    us_state,
    post_code,
    lat,
    lon,
    population_city,
    jobs,
    merchant_lat,
    merchant_lon,
    target
FROM fraud.kafka_transactions;
