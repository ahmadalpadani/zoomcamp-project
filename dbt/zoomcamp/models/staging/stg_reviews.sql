select
    cast(review_id as string) as review_id,
    cast(order_id as string) as order_id,
    cast(review_score as integer) as review_score,
    cast(review_comment_title as string) as review_comment_title,
    cast(review_comment_message as string) as review_comment_message,
    cast(review_creation_date as timestamp) as review_creation_date,
    cast(review_answer_timestamp as timestamp) as review_answer_timestamp
from {{ source('ecommerce_dataset', 'ext_reviews') }}
where review_id is not null and order_id is not null 
