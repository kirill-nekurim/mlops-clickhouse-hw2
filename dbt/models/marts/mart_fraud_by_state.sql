select
    state_code,
    count() as transaction_count,
    sum(is_fraud) as fraud_count,
    100.0 * sum(is_fraud) / count() as fraud_rate,
    uniqExact(customer_id) as customer_count,
    uniqExact(merchant) as merchant_count,
    sum(amount) as total_amount,
    sumIf(amount, is_fraud = 1) as fraud_amount
from {{ ref('stg_transactions') }}
group by state_code
