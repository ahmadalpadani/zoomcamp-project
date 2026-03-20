select 
    cast(order_id as string) as order_id,
    cast(payment_sequential as integer) as payment_sequential,
    cast(payment_type as string) as payment_type,
    cast(payment_installments as integer) as payment_installments,
    cast(payment_value as float64) as payment_value
from {{ source('ecommerce_dataset', 'ext_payments') }}
where order_id is not null
