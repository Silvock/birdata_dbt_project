/*
with
    monthly_users_recap as (

        select
            date_trunc(order_date, month) as order_month,
            count(distinct user_name) as total_monthly_users
        from {{ source("sales_database", "order") }}
        group by 1
        order by total_monthly_users desc

    ),
    total_monthly_user_from_jawa_timur as (
        select
            date_trunc(order_date, month) as order_month,
            count(distinct user.user_name) as total_monthly_users_from_jawa_timur
        from {{ source("sales_database", "order") }} as orders
        left join
            {{ source("sales_database", "user") }} as user
            on user.user_name = orders.user_name
        where user.customer_state like '%JAWA%TIMUR%'
        group by order_month

    ),

    monthly_orders_recap as (

        select
            date_trunc(order_date, month) as order_month,
            count(order_id) as total_monthly_orders
        from {{ source("sales_database", "order") }}
        group by order_month

    )--,

    shipping_cost as (

        select shipping_cost from sales_database.order_item where price > 7000
    )

select
    u.order_month,
    coalesce(u.total_monthly_users, 0) as nb_users_monthly,
    coalesce(jt.total_monthly_users_from_jawa_timur, 0) as total_monthly_user,
    coalesce(o.total_monthly_orders, 0) as monthly_order_count
from monthly_users_recap as u
left join total_monthly_user_from_jawa_timur as jt on jt.order_month = u.order_month
left join monthly_orders_recap as o on o.order_month = u.order_month
order by order_month

*/

WITH
    total_monthly_user_from_jawa_timur as (
        select
            date_trunc(order_date, month) as order_month,
            count(distinct orders.user_id) as total_monthly_users_from_jawa_timur
        from {{ ref('int_sales_database__order') }} orders
        LEFT JOIN {{ ref('stg_google_sheets__account_manager_region_mapping') }} as mapping ON orders.user_state = mapping.state
        where mapping.state like '%JAWA%TIMUR%'
        group by order_month

    )

SELECT DATE_TRUNC(order_created_at, MONTH) AS reporting_date,
    mapping.account_manager,
    mapping.state,
    COUNT(DISTINCT order_id) AS total_orders,
    count(distinct orders.user_id) as total_monthly_users,
    total_monthly_users_from_jawa_timur
FROM {{ ref('int_sales_database__order') }} AS orders
LEFT JOIN {{ ref('stg_google_sheets__account_manager_region_mapping') }} as mapping ON orders.user_state = mapping.state
left join total_monthly_user_from_jawa_timur as jt on jt.order_month = u.order_month
GROUP BY reporting_date,
    account_manager,
    state