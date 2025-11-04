# Architecture Documentation

## System Overview

This DBT CDC Demo implements a modern ELT (Extract, Load, Transform) pipeline with Change Data Capture capabilities using a simplified, single-database architecture with DuckDB.

## Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                   SOURCE SCHEMA (DuckDB)                        │
├─────────────────────────────────────────────────────────────────┤
│  Schema: source                                                 │
│  Tables (simulating external source system):                    │
│  ├── cdc_customers (customer_id PK, email, name, address...)   │
│  ├── cdc_orders (order_id PK, customer_id FK, amount...)       │
│  ├── cdc_order_items (order_item_id PK, order_id FK...)        │
│  └── cdc_metadata (tracking table)                             │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ 1. Data Generator (Faker)
                 │    - Creates realistic sample data in source schema
                 │    - Simulates INSERT/UPDATE operations
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│              CDC EXTRACTOR (Python + DuckDB)                    │
├─────────────────────────────────────────────────────────────────┤
│  Process:                                                       │
│  1. Query new records: WHERE id > last_extracted_id            │
│     (from source.cdc_* tables)                                  │
│  2. Add CDC metadata:                                           │
│     - cdc_operation (INSERT/UPDATE/DELETE)                      │
│     - cdc_timestamp                                             │
│     - cdc_batch_id                                              │
│  3. Load to raw layer (*_cdc tables)                            │
│  4. Update high-water mark                                      │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ 2. CDC Extraction
                 │    - Incremental, high-water mark based
                 │    - Idempotent operations
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│              RAW LAYER (DuckDB)                                 │
├─────────────────────────────────────────────────────────────────┤
│  Schema: raw                                                    │
│  ├── customers_cdc (append-only CDC log)                        │
│  ├── orders_cdc (append-only CDC log)                           │
│  ├── order_items_cdc (append-only CDC log)                      │
│  └── cdc_metadata (extraction tracking)                         │
│                                                                 │
│  Format: Native DuckDB tables                                  │
│  Optional: Export to Parquet for Iceberg compatibility         │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ 3. DBT Transformations
                 │    - Staging: Deduplicate, latest state
                 │    - Marts: SCD Type 2, aggregations
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│              STAGING LAYER (DuckDB Views)                       │
├─────────────────────────────────────────────────────────────────┤
│  Schema: staging                                                │
│  ├── stg_customers (latest state per customer)                  │
│  ├── stg_orders (latest state per order)                        │
│  └── stg_order_items (latest state per item)                    │
│                                                                 │
│  Logic:                                                         │
│  - ROW_NUMBER() OVER (PARTITION BY id ORDER BY cdc_ts DESC)    │
│  - Filter: WHERE rn = 1 AND cdc_operation != 'DELETE'          │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 │ 4. Business Logic
                 │    - SCD Type 2 for dimensions
                 │    - Fact table joins
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────┐
│              MARTS LAYER (DuckDB Tables)                        │
├─────────────────────────────────────────────────────────────────┤
│  Schema: marts                                                  │
│                                                                 │
│  Dimensions:                                                    │
│  ├── dim_customers_scd2                                         │
│  │   ├── customer_sk (surrogate key)                            │
│  │   ├── customer_id (natural key)                              │
│  │   ├── attributes (email, name, address...)                   │
│  │   ├── valid_from / valid_to                                  │
│  │   └── is_current                                             │
│  │                                                              │
│  Facts:                                                         │
│  ├── fact_orders                                                │
│  │   ├── order_id                                               │
│  │   ├── customer_sk (FK to dim_customers_scd2)                 │
│  │   ├── metrics (total_amount, item_count...)                  │
│  │   └── dimensions (order_date, status...)                     │
│  │                                                              │
│  Aggregates:                                                    │
│  └── customer_order_summary                                     │
│      ├── customer_id                                            │
│      ├── total_orders                                           │
│      ├── total_revenue                                          │
│      └── avg_order_value                                        │
└─────────────────────────────────────────────────────────────────┘
```

## Component Architecture

### 1. Data Generator (`cdc_pipeline/data_generator.py`)

**Purpose**: Simulate a production source system with realistic data changes

**Responsibilities**:
- Generate realistic customer, order, and order item data using Faker
- Simulate business operations:
  - New customer registrations
  - Customer information updates (address changes, phone updates)
  - New orders and order items
  - Order status transitions
- Maintain referential integrity

**Key Methods**:
- `generate_customers()`: Create new customer records
- `update_customers()`: Modify existing customer attributes
- `generate_orders()`: Create orders for customers
- `update_order_status()`: Simulate order lifecycle

### 2. CDC Extractor (`cdc_pipeline/cdc_extractor.py`)

**Purpose**: Capture and land changes from DuckDB source schema to raw layer

**Pattern**: High-water mark CDC
- Tracks last extracted ID per table
- Queries: `SELECT * FROM source.cdc_table WHERE id > last_id`
- Simple, reliable, all within DuckDB

**Responsibilities**:
- Extract new/updated records from source schema tables
- Enrich with CDC metadata
- Load to DuckDB raw layer
- Update extraction metadata
- Optional: Export to Parquet

**Key Methods**:
- `extract_table_changes()`: Query source tables for new records
- `load_to_duckdb()`: Insert into CDC tables in raw layer
- `export_to_parquet()`: Export for external system compatibility

**Metadata Tracked**:
- `last_extracted_id`: High-water mark
- `last_extracted_at`: Timestamp of last extraction
- `total_records_extracted`: Cumulative count

### 3. DBT Models

#### Staging Layer

**Purpose**: Provide clean, deduplicated views of current state

**Pattern**: Latest state views
```sql
WITH latest AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY cdc_timestamp DESC
    ) as rn
  FROM raw.table_cdc
)
SELECT * FROM latest
WHERE rn = 1 AND cdc_operation != 'DELETE'
```

**Models**:
- `stg_customers.sql`
- `stg_orders.sql`
- `stg_order_items.sql`

#### Marts Layer

**Purpose**: Business-ready dimensional models

**dim_customers_scd2.sql** - SCD Type 2 Implementation:

```sql
-- On initial load:
1. Create version 1 of each customer
2. Set valid_from = created_at
3. Set valid_to = NULL, is_current = true

-- On incremental run:
1. Detect changes by comparing attributes
2. For changed records:
   a. Close old version: valid_to = now, is_current = false
   b. Create new version: valid_from = now, valid_to = NULL, is_current = true
3. Insert new customers
4. Keep unchanged current records as-is
```

**fact_orders.sql** - Fact Table:
- Joins orders with customer dimension
- Matches customer version valid at order date
- Calculates metrics from order items
- Maintains grain: one row per order

**customer_order_summary.sql** - Aggregate:
- Pre-aggregated metrics per customer
- Uses current customer version only
- Common BI queries optimized

### 4. Orchestrator (`cdc_pipeline/orchestrator.py`)

**Purpose**: Coordinate end-to-end pipeline execution

**Workflow**:
```python
1. generate_data()           # Simulate source changes in source schema
2. extract_cdc()             # Extract from source to raw layer
3. dbt deps                  # Install DBT packages (first time)
4. dbt run                   # Transform data
5. dbt test                  # Validate quality
6. show_results()            # Display metrics
```

**Modes**:
- **Once**: Single pipeline execution
- **Continuous**: Loop with configurable interval

## SCD Type 2 Deep Dive

### What is SCD Type 2?

Slowly Changing Dimension Type 2 preserves full history of dimension changes.

### Example Scenario

```
Day 1: John Doe registers
  customer_id: 1
  email: john@email.com
  city: New York

Day 30: John moves to Boston
  Same customer_id: 1
  Same email: john@email.com
  New city: Boston
```

### Traditional Approach (No History)

```sql
-- Day 1
customer_id | email          | city     | updated_at
1           | john@email.com | New York | 2024-01-01

-- Day 30 (overwrites)
customer_id | email          | city     | updated_at
1           | john@email.com | Boston   | 2024-01-30
```

❌ Lost history: We don't know John was ever in New York

### SCD Type 2 Approach (Full History)

```sql
-- Day 1
customer_sk | customer_id | email          | city     | valid_from | valid_to   | is_current
101         | 1           | john@email.com | New York | 2024-01-01 | NULL       | true

-- Day 30 (preserves history)
customer_sk | customer_id | email          | city     | valid_from | valid_to   | is_current
101         | 1           | john@email.com | New York | 2024-01-01 | 2024-01-30 | false
102         | 1           | john@email.com | Boston   | 2024-01-30 | NULL       | true
```

✅ Full history: We know John was in New York from Jan 1 to Jan 30

### Benefits

1. **Historical Analysis**: "What was the customer's address when they placed order X?"
2. **Trend Analysis**: "How many customers moved states this quarter?"
3. **Compliance**: Some regulations require historical data
4. **Data Quality**: Can detect and analyze changes
5. **Point-in-Time Reporting**: Reconstruct state at any date

### Querying SCD Type 2

```sql
-- Current state only
SELECT * FROM dim_customers_scd2
WHERE is_current = true

-- As of specific date
SELECT * FROM dim_customers_scd2
WHERE '2024-01-15' BETWEEN valid_from AND COALESCE(valid_to, '9999-12-31')

-- Join fact to dimension (point-in-time)
SELECT o.*, c.*
FROM fact_orders o
JOIN dim_customers_scd2 c
  ON o.customer_id = c.customer_id
  AND o.order_date BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
```

## Iceberg Compatibility

While DuckDB doesn't natively support Apache Iceberg, the architecture follows Iceberg-compatible patterns:

### 1. Parquet Storage Format
- CDC tables can be exported to Parquet
- Snappy compression
- Columnar format for efficient queries

### 2. Append-Only Raw Layer
- CDC tables are append-only (Iceberg's copy-on-write pattern)
- Never update/delete raw data
- History preserved

### 3. Schema Evolution
- Tables designed for backward compatibility
- New columns can be added
- Nullable fields for flexibility

### 4. Partition Pruning
- `cdc_batch_id` enables partition-like filtering
- Date-based partitioning possible
- Reduces data scanned

### 5. Time Travel
- SCD Type 2 provides time travel capability
- Query data as of any date
- Full audit trail

### Future Iceberg Integration

To use real Iceberg tables:

```python
# Use PyIceberg to create tables
from pyiceberg.catalog import load_catalog

catalog = load_catalog("my_catalog")
catalog.create_table(
    "raw.customers_cdc",
    schema=customers_schema,
    partition_spec=PartitionSpec(
        PartitionField(source_id=1, field_id=1, transform=DayTransform(), name="cdc_date")
    )
)
```

## Performance Optimization

### Indexing Strategy

**DuckDB**:
- DuckDB auto-indexes primary keys
- Zone maps provide automatic min/max filtering
- No explicit indexes needed for most queries
- Columnar storage provides efficient scans

### Partitioning Strategy

For large datasets:

```sql
-- Partition by date
CREATE TABLE raw.customers_cdc_partitioned AS
SELECT *,
  DATE_TRUNC('day', cdc_timestamp) as partition_date
FROM raw.customers_cdc;

-- Query specific partition
SELECT * FROM raw.customers_cdc_partitioned
WHERE partition_date = '2024-01-01';
```

### DBT Optimization

```yaml
# dbt_project.yml
models:
  cdc_demo:
    staging:
      +materialized: view        # Low overhead
    marts:
      +materialized: table       # Pre-compute
      +threads: 4                # Parallel execution
```

### Incremental Models

```sql
-- In dim_customers_scd2.sql
{{ config(materialized='incremental') }}

-- Only process new data
{% if is_incremental() %}
WHERE cdc_timestamp > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %}
```

## Testing Strategy

### DBT Tests

**Generic Tests**:
```yaml
columns:
  - name: customer_id
    tests:
      - unique
      - not_null
      - relationships:
          to: ref('stg_customers')
          field: customer_id
```

**Custom Tests**:
```sql
-- tests/assert_scd2_no_gaps.sql
-- Ensure no gaps in SCD2 history
SELECT customer_id
FROM {{ ref('dim_customers_scd2') }}
WHERE is_current = false
  AND valid_to IS NULL
```

### Data Quality Checks

```sql
-- No duplicate current records
SELECT customer_id, COUNT(*)
FROM dim_customers_scd2
WHERE is_current = true
GROUP BY customer_id
HAVING COUNT(*) > 1;

-- CDC completeness
SELECT COUNT(*) as missing_records
FROM raw.customers_cdc c
LEFT JOIN staging.stg_customers s
  ON c.customer_id = s.customer_id
WHERE s.customer_id IS NULL
  AND c.cdc_operation != 'DELETE';
```

## Deployment Architecture

### Development (Current Setup)
```
Local Docker Compose
├── DuckDB (single file database)
│   ├── source schema (simulated source)
│   ├── raw schema (CDC landing)
│   └── marts schema (DBT transforms)
└── Pipeline (Python container)
```

### Production Considerations

For production use with real source systems:

```
┌─────────────────┐
│   Source DB     │ (PostgreSQL, MySQL, etc.)
└────────┬────────┘
         │
         │ VPC/Private Network
         │
┌────────▼────────┐
│ CDC Extractor   │ (ECS, Kubernetes, Airflow)
│  (Container)    │ - Modified to connect to real source
└────────┬────────┘
         │
         │ S3/GCS/Azure Blob
         │
┌────────▼────────┐
│ DuckDB/Iceberg  │ (Data Lake)
│  Parquet Files  │ Or: Snowflake, BigQuery, etc.
└────────┬────────┘
         │
         │ DBT Cloud / Airflow
         │
┌────────▼────────┐
│   Marts Layer   │ (Analytics-ready tables)
└─────────────────┘
```

### Scaling Considerations

1. **Large Tables**:
   - Partition CDC tables by date
   - Parallel extraction workers
   - Batch processing

2. **High Frequency Changes**:
   - Reduce polling interval
   - Stream processing (Kafka, Kinesis)
   - Real-time CDC (Debezium)

3. **Many Tables**:
   - Dynamic pipeline generation
   - Metadata-driven extraction
   - Parallel DBT runs

## Monitoring & Observability

### Key Metrics

```python
# Extraction Metrics
- records_extracted_per_table
- extraction_duration_seconds
- extraction_lag_seconds (current_time - max_updated_at)

# DBT Metrics
- model_build_duration_seconds
- test_failures_count
- rows_affected_per_model

# Data Quality Metrics
- duplicate_records_count
- null_key_violations
- referential_integrity_failures
```

### Alerting

```yaml
# Example alert conditions
- extraction_lag > 300s          # More than 5 min behind
- test_failures > 0              # Any test failure
- records_extracted = 0 for 1hr  # No data flowing
```

## Security Considerations

1. **Credentials**: Use environment variables, never hardcode
2. **Network**: Isolate DB connections in private network
3. **Access Control**: Least privilege for service accounts
4. **Encryption**: TLS for DB connections, at-rest for data files
5. **Auditing**: Log all extractions, track data lineage

## Cost Optimization

1. **Storage**:
   - Parquet compression reduces size 70-90%
   - Partition pruning reduces query costs
   - Lifecycle policies for old data

2. **Compute**:
   - Batch processing over streaming where possible
   - Right-size container resources
   - Schedule non-critical jobs off-peak

3. **Data Transfer**:
   - Compress data in transit
   - Use regional endpoints
   - Minimize cross-region transfers
