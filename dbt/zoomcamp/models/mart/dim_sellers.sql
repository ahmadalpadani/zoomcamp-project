with seller_performance as (
    select * from {{ ref('int_seller_performance') }}
),

seller as (
    select * from {{ ref('stg_sellers') }}
)

select 
    -- Seller Info
    s.seller_id,
    s.seller_zip_code_prefix,
    s.seller_city,
    s.seller_state,
    
    -- Performance Metrics
    sp.total_revenue,
    sp.total_orders,
    sp.average_seller_rating,
    sp.total_reviews,
    sp.avg_shipping_limit_buffer_days,

    -- Segmentation
    {{ get_seller_quality_segment('sp.average_seller_rating') }} as seller_quality_segment,
    {{ get_seller_revenue_tier('sp.total_revenue') }} as seller_revenue_tier

from seller_performance sp
left join seller s on sp.seller_id = s.seller_id        




