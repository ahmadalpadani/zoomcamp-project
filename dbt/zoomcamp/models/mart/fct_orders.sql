with orders as (
    select * from {{ ref('int_order_enriched') }}
),

payments as (
    select * from {{ ref('int_payments_pivoted') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_status,
    o.order_purchase_timestamp,
    o.purchase_time_segment,
    o.delivery_time_in_days,
    o.delay_from_estimate,
    o.is_late,
    -- Menjumlahkan semua kolom payment hasil pivot untuk dpt Total Order Value
    p.credit_card_amount,
    p.boleto_amount,
    p.voucher_amount,
    p.debit_card_amount,
    (p.credit_card_amount + p.boleto_amount + p.voucher_amount + p.debit_card_amount) as total_order_value
from orders o
left join payments p on o.order_id = p.order_id