# DBT Exercise Project

This is your DBT project for the bookstore exercise!

## Quick Start

1. Make sure you've run the setup script from the parent directory:
   ```bash
   cd ..
   python setup_exercise.py
   ```

2. Install DBT (if not already installed):
   ```bash
   pip install dbt-duckdb
   ```

3. Run your first DBT model:
   ```bash
   dbt run --profiles-dir . --select stg_products
   ```

4. Complete all TODO items in the model files

5. Run all models:
   ```bash
   dbt run --profiles-dir .
   ```

6. Run tests:
   ```bash
   dbt test --profiles-dir .
   ```

## Project Structure

```
dbt_exercise/
├── dbt_project.yml           # DBT project configuration
├── profiles.yml              # Database connection settings
├── models/
│   ├── staging/              # Staging models (views)
│   │   ├── stg_products.sql
│   │   ├── stg_customers.sql
│   │   └── stg_sales.sql
│   ├── marts/                # Analytics models (tables)
│   │   ├── fct_sales.sql
│   │   └── customer_summary.sql
│   └── schema.yml            # Tests and documentation
└── README.md                 # This file
```

## Useful Commands

```bash
# Run all models
dbt run --profiles-dir .

# Run only staging models
dbt run --profiles-dir . --select staging

# Run only marts models
dbt run --profiles-dir . --select marts

# Run a specific model
dbt run --profiles-dir . --select stg_products

# Run a model and everything downstream
dbt run --profiles-dir . --select stg_sales+

# Run tests
dbt test --profiles-dir .

# Generate documentation
dbt docs generate --profiles-dir .

# View compiled SQL
cat target/compiled/bookstore_exercise/models/staging/stg_products.sql
```

## Completing the Exercise

Each model file has TODO comments indicating what you need to add. Follow these steps:

1. **Complete staging models** (stg_*.sql)
   - These are simple transformations of raw data
   - Add the logic specified in TODO comments

2. **Complete fact table** (fct_sales.sql)
   - Join staging models together
   - Add denormalized fields for easy querying

3. **Complete summary table** (customer_summary.sql)
   - Create aggregations
   - Add business logic for customer segmentation

4. **Run and test**
   - `dbt run --profiles-dir .`
   - `dbt test --profiles-dir .`

5. **Query your results** using the sample queries in the parent README

## Tips

- Check the parent README.md for detailed explanations and examples
- Look at the main project's models for inspiration
- Use `target/compiled/` to see the actual SQL that DBT generates
- Run with `--debug` flag if you encounter errors

Good luck!
