-- models/staging/stg_products.sql
-- Purpose: Clean and standardize product data

{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from raw.products
),

cleaned as (
    select
        product_id,
        title,
        author,
        -- TODO: Add logic to ensure price is never negative
        price,
        category,
        created_at
    from source
)

select * from cleaned
