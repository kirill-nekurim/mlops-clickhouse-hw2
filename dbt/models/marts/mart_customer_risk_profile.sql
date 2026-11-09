with customer_metrics as (
    select
        customer_id,
        min(transaction_time) as first_transaction_at,
        max(transaction_time) as last_transaction_at,
        count() as transaction_count,
        sum(is_fraud) as fraud_count,
        100.0 * sum(is_fraud) / count() as fraud_rate,
        sum(amount) as total_amount,
        avg(amount) as average_amount
    from {{ ref('stg_transactions') }}
    group by customer_id
)

select
    *,
    case
        when fraud_rate >= 5 then 'HIGH'
        when fraud_count > 0 then 'MEDIUM'
        else 'LOW'
    end as risk_level
from customer_metrics
