# DBT Learning Exercise with DuckDB

Welcome! This exercise will teach you the fundamentals of DBT (Data Build Tool) using DuckDB as the database. By the end of this exercise, you'll understand how to build a data transformation pipeline using DBT.

## What You'll Learn

1. **DBT Basics**: Models, refs, and sources
2. **Staging Layer**: Clean and standardize raw data
3. **Marts Layer**: Create business-ready analytics tables
4. **Testing**: Validate your data transformations
5. **Documentation**: Document your data models

## Prerequisites

- Docker and Docker Compose (from the main project)
- Basic SQL knowledge
- Text editor

## Exercise Scenario

You work for an online bookstore. You have three raw data tables:
- **products**: Information about books (title, author, price)
- **customers**: Customer information
- **sales**: Transaction records

Your goal is to use DBT to transform this raw data into analytics-ready tables that answer business questions like:
- What are our top-selling books?
- Who are our best customers?
- What's our revenue by author?

## Project Structure

```
exercises/beginner-dbt/
├── README.md                    # This file
├── setup_exercise.py            # Creates sample data in DuckDB
├── exercise_db/                 # DuckDB database files
├── dbt_exercise/                # Your DBT project
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/             # Step 2: You'll create staging models here
│   │   │   ├── stg_products.sql
│   │   │   ├── stg_customers.sql
│   │   │   └── stg_sales.sql
│   │   ├── marts/               # Step 3: You'll create mart models here
│   │   │   ├── fct_sales.sql
│   │   │   └── customer_summary.sql
│   │   └── schema.yml           # Step 4: Tests and documentation
│   └── README.md
```

## Step-by-Step Guide

### Step 1: Setup the Exercise

First, create the sample data:

```bash
# From the repository root
cd exercises/beginner-dbt

# Install Python dependencies (if not using Docker)
pip install duckdb faker

# Run the setup script
python setup_exercise.py
```

This creates a DuckDB database at `exercise_db/bookstore.duckdb` with three tables:
- `raw.products` (20 books)
- `raw.customers` (50 customers)
- `raw.sales` (200 transactions)

You can verify the data:

```bash
python -c "
import duckdb
conn = duckdb.connect('exercise_db/bookstore.duckdb')
print('Products:', conn.execute('SELECT COUNT(*) FROM raw.products').fetchone()[0])
print('Customers:', conn.execute('SELECT COUNT(*) FROM raw.customers').fetchone()[0])
print('Sales:', conn.execute('SELECT COUNT(*) FROM raw.sales').fetchone()[0])
"
```

### Step 2: Understand DBT Project Structure

Navigate to the DBT project:

```bash
cd dbt_exercise
```

Key files:
- **dbt_project.yml**: Project configuration
- **profiles.yml**: Database connection settings
- **models/**: Where your SQL transformations live

### Step 3: Create Staging Models

Staging models clean and standardize raw data. They're typically views (not tables) because they're just a thin transformation layer.

#### 3a. Create `models/staging/stg_products.sql`

```sql
-- models/staging/stg_products.sql
-- Purpose: Clean and standardize product data

with source as (
    select * from raw.products
),

cleaned as (
    select
        product_id,
        -- Standardize title to proper case
        title,
        author,
        -- Ensure price is positive
        case
            when price < 0 then 0
            else price
        end as price,
        category,
        -- Add useful calculated fields
        length(title) as title_length,
        created_at
    from source
)

select * from cleaned
```

**Key DBT Concepts:**
- Use CTEs (with statements) for readability
- No CREATE TABLE needed - DBT handles that
- Add business logic and data quality rules

#### 3b. Create `models/staging/stg_customers.sql`

```sql
-- models/staging/stg_customers.sql
-- Purpose: Clean and standardize customer data

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
        -- Create full name field
        first_name || ' ' || last_name as full_name,
        city,
        state,
        created_at
    from source
)

select * from cleaned
```

#### 3c. Create `models/staging/stg_sales.sql`

```sql
-- models/staging/stg_sales.sql
-- Purpose: Clean and standardize sales transaction data

with source as (
    select * from raw.sales
),

cleaned as (
    select
        sale_id,
        customer_id,
        product_id,
        quantity,
        -- Ensure quantity is positive
        case
            when quantity < 0 then 0
            else quantity
        end as cleaned_quantity,
        price_per_unit,
        -- Calculate total
        quantity * price_per_unit as total_amount,
        sale_date,
        created_at
    from source
)

select * from cleaned
```

#### 3d. Run Your Staging Models

```bash
# Run all models
dbt run --profiles-dir .

# Or run just staging models
dbt run --profiles-dir . --select staging
```

You should see output like:
```
Running with dbt=1.7.0
Found 3 models, 0 tests, 0 snapshots...
Completed successfully
```

### Step 4: Create Mart Models

Marts are business-ready analytics tables. They join staging models and create aggregations.

#### 4a. Create `models/marts/fct_sales.sql`

Fact tables contain measurable business events (transactions).

```sql
-- models/marts/fct_sales.sql
-- Purpose: Sales fact table with enriched customer and product info

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
        -- Surrogate key
        s.sale_id,

        -- Foreign keys
        s.customer_id,
        s.product_id,

        -- Denormalized dimensions (for easy querying)
        c.full_name as customer_name,
        c.city as customer_city,
        c.state as customer_state,
        p.title as product_title,
        p.author as product_author,
        p.category as product_category,

        -- Metrics
        s.quantity,
        s.price_per_unit,
        s.total_amount,

        -- Date info
        s.sale_date,
        date_trunc('month', s.sale_date) as sale_month,
        date_trunc('year', s.sale_date) as sale_year

    from sales s
    left join customers c on s.customer_id = c.customer_id
    left join products p on s.product_id = p.product_id
)

select * from final
```

**Key DBT Concept: {{ ref() }}**
- `{{ ref('stg_sales') }}` creates a dependency on the `stg_sales` model
- DBT automatically determines the correct run order
- Changes propagate through the pipeline

#### 4b. Create `models/marts/customer_summary.sql`

Aggregation tables provide pre-calculated metrics.

```sql
-- models/marts/customer_summary.sql
-- Purpose: Customer-level summary statistics

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

        -- Calculated fields
        max(sale_date) - min(sale_date) as customer_lifetime_days,

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

#### 4c. Run All Models

```bash
# Run everything
dbt run --profiles-dir .

# DBT will run in order:
# 1. Staging models (no dependencies)
# 2. fct_sales (depends on staging)
# 3. customer_summary (depends on fct_sales)
```

### Step 5: Add Tests

DBT tests validate your data quality.

Create `models/schema.yml`:

```yaml
version: 2

models:
  - name: stg_products
    description: Cleaned and standardized product data
    columns:
      - name: product_id
        description: Unique product identifier
        tests:
          - unique
          - not_null
      - name: price
        description: Product price in USD
        tests:
          - not_null
          - positive_value

  - name: stg_customers
    description: Cleaned and standardized customer data
    columns:
      - name: customer_id
        description: Unique customer identifier
        tests:
          - unique
          - not_null
      - name: email
        description: Customer email address
        tests:
          - unique
          - not_null

  - name: stg_sales
    description: Cleaned and standardized sales transactions
    columns:
      - name: sale_id
        tests:
          - unique
          - not_null
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_customers')
              field: customer_id
      - name: product_id
        tests:
          - not_null
          - relationships:
              to: ref('stg_products')
              field: product_id
      - name: total_amount
        tests:
          - not_null

  - name: customer_summary
    description: Customer-level aggregated metrics
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: lifetime_value
        tests:
          - not_null
```

Run tests:

```bash
dbt test --profiles-dir .
```

### Step 6: Query Your Data

Now let's use the transformed data:

```python
import duckdb

conn = duckdb.connect('../exercise_db/bookstore.duckdb')

# Top customers by lifetime value
print("\n=== Top 5 Customers ===")
print(conn.execute("""
    SELECT
        customer_name,
        customer_state,
        total_purchases,
        lifetime_value,
        customer_segment
    FROM marts.customer_summary
    ORDER BY lifetime_value DESC
    LIMIT 5
""").df())

# Top selling books
print("\n=== Top 5 Books by Revenue ===")
print(conn.execute("""
    SELECT
        product_title,
        product_author,
        SUM(quantity) as total_sold,
        SUM(total_amount) as total_revenue
    FROM marts.fct_sales
    GROUP BY product_title, product_author
    ORDER BY total_revenue DESC
    LIMIT 5
""").df())

# Revenue by category
print("\n=== Revenue by Category ===")
print(conn.execute("""
    SELECT
        product_category,
        COUNT(DISTINCT sale_id) as transactions,
        SUM(total_amount) as revenue
    FROM marts.fct_sales
    GROUP BY product_category
    ORDER BY revenue DESC
""").df())
```

## DBT Commands Cheat Sheet

```bash
# Run all models
dbt run --profiles-dir .

# Run specific model
dbt run --profiles-dir . --select stg_products

# Run model and all downstream models
dbt run --profiles-dir . --select stg_products+

# Run all tests
dbt test --profiles-dir .

# Run tests for specific model
dbt test --profiles-dir . --select stg_customers

# Generate documentation
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .

# Show model lineage
dbt ls --profiles-dir . --select stg_products+

# Compile SQL without running
dbt compile --profiles-dir .
```

## Challenge Exercises

Once you've completed the basic exercise, try these challenges:

### Challenge 1: Author Summary
Create `models/marts/author_summary.sql` that shows:
- Author name
- Number of books
- Total revenue
- Average book price
- Best-selling book title

### Challenge 2: Monthly Revenue Trend
Create `models/marts/monthly_revenue.sql` that shows:
- Month
- Total sales
- Total revenue
- Number of unique customers
- Average order value

### Challenge 3: Add Custom Tests
Create a custom test in `tests/` to ensure:
- No sales have negative amounts
- All sales dates are not in the future
- Price per unit matches the product catalog

### Challenge 4: Incremental Models
Modify `fct_sales` to be an incremental model that only processes new sales.

Hint:
```sql
{{
    config(
        materialized='incremental',
        unique_key='sale_id'
    )
}}

-- Your model code...

{% if is_incremental() %}
where sale_date > (select max(sale_date) from {{ this }})
{% endif %}
```

## Key Concepts Learned

1. **Modularity**: Break transformations into small, reusable models
2. **Layering**: Staging → Marts creates clear separation of concerns
3. **Ref Function**: Creates dependencies and ensures correct run order
4. **Materialization**: Views (staging) vs Tables (marts)
5. **Testing**: Automated data quality checks
6. **Documentation**: Self-documenting data pipeline

## Common Issues

**Issue**: `Database Error in model stg_products`
**Solution**: Check your SQL syntax. Run the SQL directly in DuckDB to debug.

**Issue**: `Model depends on a node named 'stg_sales' which was not found`
**Solution**: Make sure file names match the ref() names.

**Issue**: `UNIQUE constraint failed`
**Solution**: Check for duplicate records in your raw data or keys.

## Next Steps

1. Explore the main CDC demo project to see production patterns
2. Read DBT documentation: https://docs.getdbt.com/
3. Learn about DBT macros for reusable code
4. Understand incremental models for large datasets
5. Explore DBT packages for common transformations

## Resources

- [DBT Documentation](https://docs.getdbt.com/)
- [DuckDB SQL Reference](https://duckdb.org/docs/sql/introduction)
- [DBT Best Practices](https://docs.getdbt.com/guides/best-practices)
- [Analytics Engineering Guide](https://www.getdbt.com/analytics-engineering/)

## Need Help?

- Check `target/compiled/` to see the actual SQL DBT generates
- Run with `--debug` flag for detailed logs
- Look at the main project's models for more examples

Happy learning!
