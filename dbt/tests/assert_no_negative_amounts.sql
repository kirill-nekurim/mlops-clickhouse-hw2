select
    transaction_id,
    amount
from {{ ref('stg_transactions') }}
where amount < 0 or not isFinite(amount)
