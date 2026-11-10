{% set marts = [
    'mart_fraud_by_category', 'mart_fraud_by_state',
    'mart_customer_risk_profile', 'mart_hourly_fraud_pattern',
    'mart_merchant_analytics'
] %}
{% for model in marts %}
    select '{{ model }}' as model_name
    from {{ ref(model) }} as mart
    having
        sum(mart.fraud_count) != (
            select sum(staged.is_fraud) from {{ ref('stg_transactions') }} as staged
        )
    {% if not loop.last %}union all{% endif %}
{% endfor %}
