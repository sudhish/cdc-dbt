-- models/marts/customer_summary.sql
-- Purpose: Customer-level summary statistics

{{
    config(
        materialized='table'
    )
}}

with sales as (
    select * from {{ ref('fct_sales') }}
),

final as (
    select
        customer_id,
        -- TODO: Add customer_name, customer_city, customer_state from fct_sales

        -- TODO: Add aggregated metrics:
        -- - count of distinct sale_id as total_purchases
        -- - sum of quantity as total_books_bought
        -- - sum of total_amount as lifetime_value
        -- - avg of total_amount as avg_purchase_amount
        -- - min of sale_date as first_purchase_date
        -- - max of sale_date as last_purchase_date

        -- TODO: Create customer segmentation based on lifetime_value:
        --   'High Value' if > 500
        --   'Medium Value' if > 200
        --   'Low Value' otherwise

    from sales
    group by
        customer_id
        -- TODO: Add other non-aggregated fields to group by
)

select * from final
