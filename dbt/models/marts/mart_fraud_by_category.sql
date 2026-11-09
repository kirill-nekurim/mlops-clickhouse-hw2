select
    category,
    count() as transaction_count,
    sum(is_fraud) as fraud_count,
    100.0 * sum(is_fraud) / count() as fraud_rate,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount
from {{ ref('stg_transactions') }}
group by category
