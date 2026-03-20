with payments as (
    select * from {{ ref('stg_payments') }}
)

select
    order_id,
    {{ pivot_payments('payment_type') }},
    -- Kamu juga bisa menghitung cicilan maksimal di sini
    MAX(payment_installments) as max_installments
from payments
group by 1