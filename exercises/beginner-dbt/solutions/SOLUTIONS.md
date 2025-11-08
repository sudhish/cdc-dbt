# Exercise Solutions

This file contains the completed solutions for the DBT exercise. Try to complete the exercise yourself first before looking at these!

## Solution: stg_products.sql

```sql
-- models/staging/stg_products.sql
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
        -- Ensure price is never negative
        case
            when price < 0 then 0
            else price
        end as price,
        category,
        created_at
    from source
)

select * from cleaned
```

## Solution: stg_customers.sql

```sql
-- models/staging/stg_customers.sql
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
        -- Standardize email to lowercase
        lower(email) as email,
        first_name,
        last_name,
        -- Create full_name field
        first_name || ' ' || last_name as full_name,
        city,
        state,
        created_at
    from source
)

select * from cleaned
```

## Solution: stg_sales.sql

```sql
-- models/staging/stg_sales.sql
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
        -- Calculate total_amount
        quantity * price_per_unit as total_amount,
        sale_date,
        created_at
    from source
)

select * from cleaned
```

## Solution: fct_sales.sql

```sql
-- models/marts/fct_sales.sql
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

        -- Denormalized customer fields
        c.full_name as customer_name,
        c.city as customer_city,
        c.state as customer_state,

        -- Denormalized product fields
        p.title as product_title,
        p.author as product_author,
        p.category as product_category,

        -- Metrics
        s.quantity,
        s.price_per_unit,
        s.total_amount,

        -- Date info
        s.sale_date

    from sales s
    left join customers c on s.customer_id = c.customer_id
    left join products p on s.product_id = p.product_id
)

select * from final
```

## Solution: customer_summary.sql

```sql
-- models/marts/customer_summary.sql
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
        customer_name,
        customer_city,
        customer_state,

        -- Aggregated metrics
        count(distinct sale_id) as total_purchases,
        sum(quantity) as total_books_bought,
        sum(total_amount) as lifetime_value,
        avg(total_amount) as avg_purchase_amount,
        min(sale_date) as first_purchase_date,
        max(sale_date) as last_purchase_date,

        -- Customer segmentation
        case
            when sum(total_amount) > 500 then 'High Value'
            when sum(total_amount) > 200 then 'Medium Value'
            else 'Low Value'
        end as customer_segment

    from sales
    group by
        customer_id,
        customer_name,
        customer_city,
        customer_state
)

select * from final
```

## Key Concepts Demonstrated

### 1. CTEs (Common Table Expressions)
```sql
with source as (
    select * from raw.products
),
cleaned as (
    -- transformation logic
)
```
CTEs make SQL more readable and modular.

### 2. DBT ref() Function
```sql
select * from {{ ref('stg_sales') }}
```
- Creates dependencies between models
- DBT runs models in correct order
- Changes propagate automatically

### 3. Denormalization
```sql
-- Instead of just storing customer_id, also store:
c.full_name as customer_name,
c.city as customer_city
```
Makes queries faster and easier to write.

### 4. Aggregations
```sql
count(distinct sale_id) as total_purchases,
sum(total_amount) as lifetime_value,
avg(total_amount) as avg_purchase_amount
```
Pre-calculate business metrics.

### 5. Case Statements
```sql
case
    when sum(total_amount) > 500 then 'High Value'
    when sum(total_amount) > 200 then 'Medium Value'
    else 'Low Value'
end as customer_segment
```
Add business logic and categorization.

## Verification

After implementing these solutions, run:

```bash
cd dbt_exercise
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..
python query_results.py
```

You should see:
- All 5 models built successfully
- All tests passing
- Query results showing your analytics data

## Next Challenges

1. **Add an author_summary model** that shows revenue by author
2. **Create a monthly_revenue model** showing trends over time
3. **Add incremental materialization** to fct_sales
4. **Create custom tests** for data quality
5. **Add macros** for reusable logic

Good work completing the exercise!
