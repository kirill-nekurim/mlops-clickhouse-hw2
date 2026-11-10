CREATE DATABASE IF NOT EXISTS transactions_db;
CREATE DATABASE IF NOT EXISTS fraud_analytics;

CREATE TABLE IF NOT EXISTS transactions_db.transactions
(
    transaction_time Nullable(DateTime),
    merch Nullable(String),
    cat_id Nullable(String),
    amount Nullable(Float64),
    name_1 Nullable(String),
    name_2 Nullable(String),
    gender Nullable(String),
    street Nullable(String),
    one_city Nullable(String),
    us_state Nullable(String),
    post_code Nullable(String),
    lat Nullable(Float64),
    lon Nullable(Float64),
    population_city Nullable(Float64),
    jobs Nullable(String),
    merchant_lat Nullable(Float64),
    merchant_lon Nullable(Float64),
    target Nullable(UInt8)
)
ENGINE = MergeTree()
ORDER BY tuple();
