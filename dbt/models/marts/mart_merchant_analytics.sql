select
    merchant,
    count() as transaction_count,
    sum(amount) as total_amount,
    avg(amount) as average_amount,
    sum(is_fraud) as fraud_count,
    100.0 * sum(is_fraud) / count() as fraud_rate,
    uniqExact(customer_id) as customer_count,
    toUInt8(fraud_rate >= 5 and fraud_count >= 2) as is_suspicious
from {{ ref('stg_transactions') }}
group by merchant
