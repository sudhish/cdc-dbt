#!/usr/bin/env python3
"""
Query the transformed data to see results of your DBT models
Run this after completing the exercise: dbt run --profiles-dir dbt_exercise
"""

import duckdb
from pathlib import Path

db_path = Path(__file__).parent / "exercise_db" / "bookstore.duckdb"

if not db_path.exists():
    print(f"Error: Database not found at {db_path}")
    print("Please run setup_exercise.py first!")
    exit(1)

conn = duckdb.connect(str(db_path))

print("\n" + "="*70)
print("BOOKSTORE DATA ANALYSIS - DBT Exercise Results")
print("="*70)

try:
    # Check if tables exist
    staging_exists = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'staging'
    """).fetchone()[0] > 0

    marts_exists = conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'marts'
    """).fetchone()[0] > 0

    if not (staging_exists or marts_exists):
        print("\n⚠️  No DBT models found!")
        print("Run the following commands first:")
        print("  cd dbt_exercise")
        print("  dbt run --profiles-dir .")
        exit(1)

    # Query 1: Top Customers
    print("\n📊 TOP 5 CUSTOMERS BY LIFETIME VALUE")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                customer_name,
                customer_state,
                total_purchases,
                printf('$%.2f', lifetime_value) as lifetime_value,
                customer_segment
            FROM marts.customer_summary
            ORDER BY lifetime_value DESC
            LIMIT 5
        """).df()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure customer_summary model is complete and has been run!")

    # Query 2: Top Books
    print("\n\n📚 TOP 5 BOOKS BY REVENUE")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                product_title,
                product_author,
                SUM(quantity) as copies_sold,
                printf('$%.2f', SUM(total_amount)) as total_revenue
            FROM marts.fct_sales
            GROUP BY product_title, product_author
            ORDER BY SUM(total_amount) DESC
            LIMIT 5
        """).df()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {e}")
        print("Make sure fct_sales model is complete and has been run!")

    # Query 3: Revenue by Category
    print("\n\n📖 REVENUE BY CATEGORY")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                product_category,
                COUNT(DISTINCT sale_id) as transactions,
                SUM(quantity) as books_sold,
                printf('$%.2f', SUM(total_amount)) as revenue
            FROM marts.fct_sales
            GROUP BY product_category
            ORDER BY SUM(total_amount) DESC
        """).df()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {e}")

    # Query 4: Customer Segmentation
    print("\n\n👥 CUSTOMER SEGMENTATION")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                customer_segment,
                COUNT(*) as num_customers,
                printf('$%.2f', AVG(lifetime_value)) as avg_lifetime_value,
                printf('$%.2f', SUM(lifetime_value)) as total_revenue
            FROM marts.customer_summary
            GROUP BY customer_segment
            ORDER BY AVG(lifetime_value) DESC
        """).df()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {e}")

    # Query 5: Monthly sales trend
    print("\n\n📈 SALES TREND (LAST 3 MONTHS)")
    print("-" * 70)
    try:
        result = conn.execute("""
            SELECT
                DATE_TRUNC('month', sale_date) as month,
                COUNT(DISTINCT sale_id) as transactions,
                SUM(quantity) as books_sold,
                printf('$%.2f', SUM(total_amount)) as revenue,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM marts.fct_sales
            GROUP BY DATE_TRUNC('month', sale_date)
            ORDER BY month DESC
            LIMIT 3
        """).df()
        print(result.to_string(index=False))
    except Exception as e:
        print(f"❌ Error: {e}")

    print("\n" + "="*70)
    print("✅ Analysis complete! Your DBT models are working!")
    print("="*70 + "\n")

except Exception as e:
    print(f"\n❌ Unexpected error: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure you ran setup_exercise.py")
    print("2. Complete the TODO items in the model files")
    print("3. Run: cd dbt_exercise && dbt run --profiles-dir .")
    print("4. Run: dbt test --profiles-dir .")

finally:
    conn.close()
