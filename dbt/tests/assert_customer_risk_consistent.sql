select
    customer_id,
    risk_level,
    fraud_rate,
    fraud_count
from {{ ref('mart_customer_risk_profile') }}
where
    (risk_level = 'HIGH' and fraud_rate < 5)
    or (risk_level = 'MEDIUM' and (fraud_count = 0 or fraud_rate >= 5))
    or (risk_level = 'LOW' and fraud_count != 0)
