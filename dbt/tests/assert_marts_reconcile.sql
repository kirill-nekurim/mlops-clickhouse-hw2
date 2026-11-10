-- В каждой витрине все операции должны учитываться ровно один раз.
{% set marts = [
    'mart_daily_state_metrics', 'mart_fraud_by_category', 'mart_fraud_by_state',
    'mart_customer_risk_profile', 'mart_hourly_fraud_pattern',
    'mart_merchant_analytics'
] %}
{% for model in marts %}
    select '{{ model }}' as model_name
    from {{ ref(model) }} as mart
    having
        sum(mart.transaction_count) != (select count() from {{ ref('stg_transactions') }})
        or abs(sum(mart.total_amount) - (
            select sum(staged.amount) from {{ ref('stg_transactions') }} as staged
        )) > 0.01
    {% if not loop.last %}union all{% endif %}
{% endfor %}
