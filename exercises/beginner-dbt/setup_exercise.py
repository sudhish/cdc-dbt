#!/usr/bin/env python3
"""
Setup script for DBT Learning Exercise
Creates a DuckDB database with sample bookstore data
"""

import duckdb
from pathlib import Path
import random
from datetime import datetime, timedelta

def setup_database():
    """Create DuckDB database with sample data"""

    # Create directory for database
    db_dir = Path(__file__).parent / "exercise_db"
    db_dir.mkdir(exist_ok=True)

    db_path = db_dir / "bookstore.duckdb"

    print(f"Creating database at: {db_path}")

    # Connect to DuckDB
    conn = duckdb.connect(str(db_path))

    # Create schema
    conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

    # Create tables
    print("Creating tables...")

    # Products table
    conn.execute("""
        CREATE OR REPLACE TABLE raw.products (
            product_id INTEGER PRIMARY KEY,
            title VARCHAR,
            author VARCHAR,
            price DECIMAL(10,2),
            category VARCHAR,
            created_at TIMESTAMP
        )
    """)

    # Sample books data
    books = [
        ("The Great Gatsby", "F. Scott Fitzgerald", 12.99, "Fiction"),
        ("To Kill a Mockingbird", "Harper Lee", 14.99, "Fiction"),
        ("1984", "George Orwell", 13.99, "Fiction"),
        ("Pride and Prejudice", "Jane Austen", 11.99, "Romance"),
        ("The Hobbit", "J.R.R. Tolkien", 15.99, "Fantasy"),
        ("Harry Potter and the Sorcerer's Stone", "J.K. Rowling", 16.99, "Fantasy"),
        ("The Catcher in the Rye", "J.D. Salinger", 12.99, "Fiction"),
        ("The Lord of the Rings", "J.R.R. Tolkien", 24.99, "Fantasy"),
        ("Animal Farm", "George Orwell", 10.99, "Fiction"),
        ("Brave New World", "Aldous Huxley", 13.99, "Science Fiction"),
        ("The Chronicles of Narnia", "C.S. Lewis", 18.99, "Fantasy"),
        ("Jane Eyre", "Charlotte Bronte", 12.99, "Romance"),
        ("Wuthering Heights", "Emily Bronte", 11.99, "Romance"),
        ("Moby Dick", "Herman Melville", 14.99, "Adventure"),
        ("War and Peace", "Leo Tolstoy", 19.99, "Historical Fiction"),
        ("The Odyssey", "Homer", 13.99, "Classics"),
        ("Crime and Punishment", "Fyodor Dostoevsky", 15.99, "Classics"),
        ("The Divine Comedy", "Dante Alighieri", 16.99, "Classics"),
        ("Don Quixote", "Miguel de Cervantes", 17.99, "Classics"),
        ("Les Misérables", "Victor Hugo", 18.99, "Historical Fiction"),
    ]

    print("Inserting products...")
    for i, (title, author, price, category) in enumerate(books, 1):
        conn.execute("""
            INSERT INTO raw.products VALUES (?, ?, ?, ?, ?, ?)
        """, [i, title, author, price, category, datetime.now()])

    # Customers table
    conn.execute("""
        CREATE OR REPLACE TABLE raw.customers (
            customer_id INTEGER PRIMARY KEY,
            email VARCHAR,
            first_name VARCHAR,
            last_name VARCHAR,
            city VARCHAR,
            state VARCHAR,
            created_at TIMESTAMP
        )
    """)

    # Sample customer data
    first_names = ["Emma", "Liam", "Olivia", "Noah", "Ava", "Ethan", "Sophia", "Mason",
                   "Isabella", "William", "Mia", "James", "Charlotte", "Oliver", "Amelia",
                   "Benjamin", "Harper", "Elijah", "Evelyn", "Lucas", "Abigail", "Michael",
                   "Emily", "Alexander", "Elizabeth", "Daniel", "Sofia", "Matthew", "Avery",
                   "Henry", "Ella", "Jackson", "Scarlett", "Sebastian", "Grace", "Aiden",
                   "Chloe", "Jack", "Victoria", "Samuel", "Riley", "David", "Aria", "Joseph",
                   "Lily", "Carter", "Aubrey", "Owen", "Zoey", "Wyatt"]

    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez",
                  "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin",
                  "Lee", "Thompson", "White", "Harris", "Sanchez", "Clark", "Lewis",
                  "Robinson", "Walker", "Young", "Allen", "King", "Wright", "Scott",
                  "Torres", "Nguyen", "Hill", "Flores", "Green", "Adams", "Nelson",
                  "Baker", "Hall", "Rivera", "Campbell", "Mitchell", "Carter", "Roberts"]

    cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
              "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
              "Fort Worth", "Columbus", "Charlotte", "San Francisco", "Indianapolis",
              "Seattle", "Denver", "Boston"]

    states = ["NY", "CA", "IL", "TX", "AZ", "PA", "TX", "CA", "TX", "CA", "TX", "FL",
              "TX", "OH", "NC", "CA", "IN", "WA", "CO", "MA"]

    print("Inserting customers...")
    for i in range(1, 51):  # 50 customers
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        email = f"{first_name.lower()}.{last_name.lower()}{i}@email.com"
        city_idx = random.randint(0, len(cities) - 1)

        conn.execute("""
            INSERT INTO raw.customers VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [i, email, first_name, last_name, cities[city_idx], states[city_idx],
              datetime.now() - timedelta(days=random.randint(100, 365))])

    # Sales table
    conn.execute("""
        CREATE OR REPLACE TABLE raw.sales (
            sale_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price_per_unit DECIMAL(10,2),
            sale_date DATE,
            created_at TIMESTAMP
        )
    """)

    print("Inserting sales...")
    # Generate 200 sales transactions
    base_date = datetime.now() - timedelta(days=90)

    for i in range(1, 201):  # 200 sales
        customer_id = random.randint(1, 50)
        product_id = random.randint(1, 20)
        quantity = random.randint(1, 5)

        # Get product price
        price = conn.execute(
            "SELECT price FROM raw.products WHERE product_id = ?",
            [product_id]
        ).fetchone()[0]

        sale_date = base_date + timedelta(days=random.randint(0, 90))

        conn.execute("""
            INSERT INTO raw.sales VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [i, customer_id, product_id, quantity, price, sale_date.date(), datetime.now()])

    # Print summary
    print("\n" + "="*50)
    print("DATABASE SETUP COMPLETE!")
    print("="*50)

    print(f"\nDatabase location: {db_path}")
    print(f"Database size: {db_path.stat().st_size / 1024:.2f} KB")

    print("\nTables created:")
    print(f"  - raw.products: {conn.execute('SELECT COUNT(*) FROM raw.products').fetchone()[0]} rows")
    print(f"  - raw.customers: {conn.execute('SELECT COUNT(*) FROM raw.customers').fetchone()[0]} rows")
    print(f"  - raw.sales: {conn.execute('SELECT COUNT(*) FROM raw.sales').fetchone()[0]} rows")

    print("\nSample data preview:")
    print("\nTop 3 Products:")
    print(conn.execute("SELECT * FROM raw.products LIMIT 3").df().to_string())

    print("\n\nTop 3 Customers:")
    print(conn.execute("SELECT customer_id, email, first_name, last_name, city FROM raw.customers LIMIT 3").df().to_string())

    print("\n\nTop 3 Sales:")
    print(conn.execute("SELECT * FROM raw.sales LIMIT 3").df().to_string())

    print("\n" + "="*50)
    print("Next steps:")
    print("1. cd dbt_exercise")
    print("2. Follow the README instructions to create DBT models")
    print("3. Run: dbt run --profiles-dir .")
    print("="*50)

    conn.close()

if __name__ == "__main__":
    setup_database()
