with normalized as (
    select
        assumeNotNull(raw_transactions.transaction_time) as transaction_time,
        trimBoth(coalesce(raw_transactions.merch, 'UNKNOWN')) as merchant,
        lowerUTF8(trimBoth(coalesce(raw_transactions.cat_id, 'unknown'))) as category,
        round(toFloat64(assumeNotNull(raw_transactions.amount)), 2) as amount,
        trimBoth(coalesce(raw_transactions.name_1, '')) as first_name,
        trimBoth(coalesce(raw_transactions.name_2, '')) as last_name,
        upperUTF8(trimBoth(coalesce(raw_transactions.gender, 'UNKNOWN'))) as gender,
        upperUTF8(trimBoth(coalesce(raw_transactions.us_state, 'UNKNOWN'))) as state_code,
        coalesce(toString(raw_transactions.lat), 'UNKNOWN') as lat,
        coalesce(toString(raw_transactions.lon), 'UNKNOWN') as lon,
        coalesce(toString(raw_transactions.merchant_lat), 'UNKNOWN') as merchant_lat,
        coalesce(toString(raw_transactions.merchant_lon), 'UNKNOWN') as merchant_lon,
        toUInt8(assumeNotNull(raw_transactions.target)) as is_fraud
    from {{ source('raw', 'transactions') }} as raw_transactions
    where
        raw_transactions.transaction_time is not null
        and raw_transactions.amount is not null
        and raw_transactions.amount >= 0
        and isFinite(toFloat64(raw_transactions.amount))
        and raw_transactions.target in (0, 1)
),

identified as (
    select
        *,
        {{ dbt_utils.generate_surrogate_key([
            'first_name', 'last_name', 'gender', 'lat', 'lon'
        ]) }} as customer_id,
        {{ dbt_utils.generate_surrogate_key([
            'transaction_time', 'merchant', 'category', 'amount',
            'first_name', 'last_name', 'gender', 'state_code',
            'lat', 'lon', 'merchant_lat', 'merchant_lon', 'is_fraud'
        ]) }} as transaction_id
    from normalized
)

select distinct
    transaction_id,
    customer_id,
    transaction_time,
    merchant,
    category,
    state_code,
    gender,
    amount,
    is_fraud,
    toDate(transaction_time) as transaction_date,
    {{ dbt_date.day_of_week('transaction_time', isoweek=true) }} as day_of_week,
    toHour(transaction_time) as transaction_hour,
    {{ amount_bucket('amount') }} as amount_bucket
from identified
