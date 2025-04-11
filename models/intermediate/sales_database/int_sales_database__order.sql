with order_item_grouped_by_order as (

select order_id,
    user_id,
    order_status,
    order_created_at,
    order_approved_at,
    sum(total_order_item_amount) as total_order_amount,
    sum(item_quantity) as total_items,
    count(distinct product_id) as total_distinct_items
from {{ ref('int_sales_database__order_item') }}
group by order_id,
    user_id,
    order_status,
    order_created_at,
    order_approved_at

), feedback_grouped_by_order as (

select order_id,
    SUM(feedback_score) as sum_feedback_score,
    COUNT(*) as total_feedbacks
from {{ ref('stg_sales_database__feedback') }}
group by order_id

)

select oi.order_id,
    oi.user_id,
    oi.order_status,
    u.user_city,
    u.user_state,
    oi.order_created_at,
    oi.order_approved_at,
    coalesce(f.sum_feedback_score,0) as sum_feedback_score,
    coalesce(f.total_feedbacks,0) as total_feedbacks,
    coalesce(oi.total_order_amount,0) as total_order_amount,
    coalesce(oi.total_items,0) as total_items,
    coalesce(oi.total_distinct_items,0) as total_distinct_items
from order_item_grouped_by_order as oi 
left join feedback_grouped_by_order as f on f.order_id = oi.order_id
left join {{ ref('stg_sales_database__user' )}} as u on u.user_id = oi.user_id
