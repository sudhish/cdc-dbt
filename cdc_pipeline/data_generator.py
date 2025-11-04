"""
Data generator to simulate CDC events in DuckDB source tables
"""
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import duckdb
from faker import Faker
from logging_config import get_logger

# Initialize logger
logger = get_logger(__name__)
fake = Faker()


class DataGenerator:
    """Generate realistic sample data for CDC simulation"""

    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        self.conn = None
        self.products = [
            "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
            "Webcam", "USB Cable", "HDMI Cable", "Desk Chair", "Standing Desk",
            "Phone", "Tablet", "Charger", "Case", "Screen Protector"
        ]
        self.order_statuses = ["pending", "processing", "completed", "cancelled", "refunded"]

    def connect(self):
        """Establish database connection"""
        logger.info(f"Connecting to DuckDB at {self.duckdb_path}")
        self.conn = duckdb.connect(self.duckdb_path)
        logger.info("Connected to DuckDB successfully")
        self._init_source_tables()

    def _init_source_tables(self):
        """Initialize source tables with cdc_ prefix to simulate source system"""
        logger.info("Initializing source tables...")

        # Create source schema
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS source")

        # Create cdc_customers table (simulated source system)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS source.cdc_customers (
                customer_id INTEGER PRIMARY KEY,
                email VARCHAR(255) UNIQUE NOT NULL,
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                phone VARCHAR(50),
                address TEXT,
                city VARCHAR(100),
                state VARCHAR(50),
                zip_code VARCHAR(20),
                country VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create cdc_orders table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS source.cdc_orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                order_status VARCHAR(50),
                total_amount DECIMAL(10, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create cdc_order_items table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS source.cdc_order_items (
                order_item_id INTEGER PRIMARY KEY,
                order_id INTEGER,
                product_name VARCHAR(255),
                quantity INTEGER,
                unit_price DECIMAL(10, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Create CDC tracking metadata table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS source.cdc_metadata (
                table_name VARCHAR(100) PRIMARY KEY,
                last_extracted_id INTEGER DEFAULT 0,
                last_extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        # Initialize metadata
        for table in ['customers', 'orders', 'order_items']:
            self.conn.execute(f"""
                INSERT INTO source.cdc_metadata (table_name, last_extracted_id)
                SELECT '{table}', 0
                WHERE NOT EXISTS (SELECT 1 FROM source.cdc_metadata WHERE table_name = '{table}')
            """)

        logger.info("Source tables initialized successfully")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def generate_customers(self, count: int = 10) -> List[int]:
        """Generate new customer records"""
        logger.info(f"Generating {count} new customers...")
        customer_ids = []
        errors = 0

        # Get the next customer_id
        result = self.conn.execute("SELECT COALESCE(MAX(customer_id), 0) + 1 FROM source.cdc_customers").fetchone()
        next_id = result[0]

        for i in range(count):
            try:
                customer_id = next_id + i
                self.conn.execute("""
                    INSERT INTO source.cdc_customers (customer_id, email, first_name, last_name, phone, address, city, state, zip_code, country)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    customer_id,
                    fake.unique.email(),
                    fake.first_name(),
                    fake.last_name(),
                    fake.phone_number(),
                    fake.street_address(),
                    fake.city(),
                    fake.state_abbr(),
                    fake.zipcode(),
                    'USA'
                ))
                customer_ids.append(customer_id)
                logger.debug(f"Created customer {customer_id}")
            except Exception as e:
                errors += 1
                logger.error(f"Error creating customer: {e}")

        logger.info(f"Generated {len(customer_ids)} customers (errors: {errors})")
        return customer_ids

    def update_customers(self, customer_ids: List[int], count: int = 5):
        """Update existing customer records to simulate changes"""
        if not customer_ids:
            logger.debug("No customers to update")
            return

        update_count = min(count, len(customer_ids))
        selected_ids = random.sample(customer_ids, update_count)

        logger.info(f"Updating {update_count} customers...")
        updated = 0
        errors = 0

        for customer_id in selected_ids:
            # Randomly choose what to update
            update_type = random.choice(['address', 'phone', 'email'])

            try:
                if update_type == 'address':
                    self.conn.execute("""
                        UPDATE source.cdc_customers
                        SET address = ?, city = ?, state = ?, zip_code = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE customer_id = ?
                    """, (
                        fake.street_address(),
                        fake.city(),
                        fake.state_abbr(),
                        fake.zipcode(),
                        customer_id
                    ))
                elif update_type == 'phone':
                    self.conn.execute("""
                        UPDATE source.cdc_customers
                        SET phone = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE customer_id = ?
                    """, (fake.phone_number(), customer_id))
                else:  # email
                    self.conn.execute("""
                        UPDATE source.cdc_customers
                        SET email = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE customer_id = ?
                    """, (fake.unique.email(), customer_id))

                logger.debug(f"Updated customer {customer_id} ({update_type})")
                updated += 1
            except Exception as e:
                errors += 1
                logger.error(f"Error updating customer {customer_id}: {e}")

        logger.info(f"Updated {updated} customers (errors: {errors})")

    def generate_orders(self, customer_ids: List[int], count: int = 20) -> List[int]:
        """Generate new order records"""
        if not customer_ids:
            logger.debug("No customer IDs available to generate orders")
            return []

        logger.info(f"Generating {count} new orders...")
        order_ids = []
        errors = 0

        # Get the next order_id
        result = self.conn.execute("SELECT COALESCE(MAX(order_id), 0) + 1 FROM source.cdc_orders").fetchone()
        next_id = result[0]

        for i in range(count):
            customer_id = random.choice(customer_ids)
            order_status = random.choice(self.order_statuses)
            total_amount = round(random.uniform(10.0, 1000.0), 2)
            order_date = datetime.now() - timedelta(days=random.randint(0, 90))

            try:
                order_id = next_id + i
                self.conn.execute("""
                    INSERT INTO source.cdc_orders (order_id, customer_id, order_date, order_status, total_amount)
                    VALUES (?, ?, ?, ?, ?)
                """, (order_id, customer_id, order_date, order_status, total_amount))
                order_ids.append(order_id)
                logger.debug(f"Created order {order_id} for customer {customer_id} (${total_amount})")
            except Exception as e:
                errors += 1
                logger.error(f"Error creating order: {e}")

        logger.info(f"Generated {len(order_ids)} orders (errors: {errors})")
        return order_ids

    def generate_order_items(self, order_ids: List[int]):
        """Generate order items for orders"""
        if not order_ids:
            logger.debug("No order IDs available to generate order items")
            return

        logger.info(f"Generating order items for {len(order_ids)} orders...")
        total_items = 0
        errors = 0

        # Get the next order_item_id
        result = self.conn.execute("SELECT COALESCE(MAX(order_item_id), 0) + 1 FROM source.cdc_order_items").fetchone()
        next_id = result[0]

        for order_id in order_ids:
            # Each order has 1-5 items
            num_items = random.randint(1, 5)

            for _ in range(num_items):
                product_name = random.choice(self.products)
                quantity = random.randint(1, 3)
                unit_price = round(random.uniform(10.0, 500.0), 2)

                try:
                    order_item_id = next_id + total_items
                    self.conn.execute("""
                        INSERT INTO source.cdc_order_items (order_item_id, order_id, product_name, quantity, unit_price)
                        VALUES (?, ?, ?, ?, ?)
                    """, (order_item_id, order_id, product_name, quantity, unit_price))
                    total_items += 1
                except Exception as e:
                    errors += 1
                    logger.error(f"Error creating order item: {e}")

        logger.info(f"Generated {total_items} order items (errors: {errors})")

    def update_order_status(self, order_ids: List[int], count: int = 10):
        """Update order statuses to simulate state changes"""
        if not order_ids:
            logger.debug("No orders to update")
            return

        update_count = min(count, len(order_ids))
        selected_ids = random.sample(order_ids, update_count)

        logger.info(f"Updating status for {update_count} orders...")
        updated = 0
        errors = 0

        for order_id in selected_ids:
            new_status = random.choice(self.order_statuses)

            try:
                self.conn.execute("""
                    UPDATE source.cdc_orders
                    SET order_status = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE order_id = ?
                """, (new_status, order_id))
                logger.debug(f"Updated order {order_id} status to {new_status}")
                updated += 1
            except Exception as e:
                errors += 1
                logger.error(f"Error updating order {order_id}: {e}")

        logger.info(f"Updated {updated} order statuses (errors: {errors})")

    def get_all_customer_ids(self) -> List[int]:
        """Get all customer IDs from database"""
        result = self.conn.execute("SELECT customer_id FROM source.cdc_customers").fetchall()
        customer_ids = [row[0] for row in result]
        return customer_ids

    def get_all_order_ids(self) -> List[int]:
        """Get all order IDs from database"""
        result = self.conn.execute("SELECT order_id FROM source.cdc_orders").fetchall()
        order_ids = [row[0] for row in result]
        return order_ids


def main():
    """Main function to run data generation"""
    import os

    duckdb_path = os.getenv('DUCKDB_PATH', '/data/warehouse.duckdb')

    generator = DataGenerator(duckdb_path)

    try:
        logger.info("Starting data generation process")
        generator.connect()

        # Initial data load
        logger.info("=== Generating initial data ===")
        customer_ids = generator.generate_customers(20)
        order_ids = generator.generate_orders(customer_ids, 50)
        generator.generate_order_items(order_ids)

        logger.info("=== Simulating CDC changes ===")
        # Simulate ongoing changes
        for i in range(3):
            logger.info(f"--- Change batch {i+1} ---")
            time.sleep(2)

            # Add new customers
            new_customers = generator.generate_customers(5)
            customer_ids.extend(new_customers)

            # Update existing customers
            generator.update_customers(customer_ids, 3)

            # Add new orders
            new_orders = generator.generate_orders(customer_ids, 10)
            order_ids.extend(new_orders)
            generator.generate_order_items(new_orders)

            # Update order statuses
            generator.update_order_status(order_ids, 5)

        logger.info("Data generation completed successfully!")

    except Exception as e:
        logger.critical(f"Data generation failed: {e}", exc_info=True)
    finally:
        generator.close()


if __name__ == "__main__":
    main()
