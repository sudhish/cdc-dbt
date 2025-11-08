-- models/marts/fct_sales.sql
-- Purpose: Sales fact table with enriched customer and product info

{{
    config(
        materialized='table'
    )
}}

with sales as (
    select * from {{ ref('stg_sales') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

products as (
    select * from {{ ref('stg_products') }}
),

final as (
    select
        -- Keys
        s.sale_id,
        s.customer_id,
        s.product_id,

        -- TODO: Add denormalized customer fields (customer_name, customer_city, customer_state)
        -- TODO: Add denormalized product fields (product_title, product_author, product_category)

        -- Metrics
        s.quantity,
        s.price_per_unit,
        -- TODO: Add total_amount from staging

        -- Date info
        s.sale_date

    from sales s
    left join customers c on s.customer_id = c.customer_id
    left join products p on s.product_id = p.product_id
)

select * from final
