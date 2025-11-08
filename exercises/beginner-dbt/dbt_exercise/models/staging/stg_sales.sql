-- models/staging/stg_sales.sql
-- Purpose: Clean and standardize sales transaction data

{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from raw.sales
),

cleaned as (
    select
        sale_id,
        customer_id,
        product_id,
        quantity,
        price_per_unit,
        -- TODO: Calculate total_amount as quantity * price_per_unit
        sale_date,
        created_at
    from source
)

select * from cleaned
