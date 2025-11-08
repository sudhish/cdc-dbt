-- models/staging/stg_customers.sql
-- Purpose: Clean and standardize customer data

{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from raw.customers
),

cleaned as (
    select
        customer_id,
        -- TODO: Standardize email to lowercase
        email,
        first_name,
        last_name,
        -- TODO: Create a full_name field by concatenating first_name and last_name
        city,
        state,
        created_at
    from source
)

select * from cleaned
