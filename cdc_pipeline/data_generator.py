"""
Data generator to simulate CDC events in Postgres
"""
import random
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any
import psycopg2
from faker import Faker
from logging_config import get_logger

# Initialize logger
logger = get_logger(__name__)
fake = Faker()


class DataGenerator:
    """Generate realistic sample data for CDC simulation"""

    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.conn = None
        self.products = [
            "Laptop", "Mouse", "Keyboard", "Monitor", "Headphones",
            "Webcam", "USB Cable", "HDMI Cable", "Desk Chair", "Standing Desk",
            "Phone", "Tablet", "Charger", "Case", "Screen Protector"
        ]
        self.order_statuses = ["pending", "processing", "completed", "cancelled", "refunded"]

    def connect(self):
        """Establish database connection"""
        logger.info(f"Connecting to Postgres at {self.db_config['host']}:{self.db_config['port']}")
        self.conn = psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            database=self.db_config['database'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )
        self.conn.autocommit = True
        logger.info("Connected to Postgres successfully")

    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()

    def generate_customers(self, count: int = 10) -> List[int]:
        """Generate new customer records"""
        logger.info(f"Generating {count} new customers...")
        customer_ids = []
        cursor = self.conn.cursor()
        errors = 0

        for _ in range(count):
            try:
                cursor.execute("""
                    INSERT INTO customers (email, first_name, last_name, phone, address, city, state, zip_code, country)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING customer_id
                """, (
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
                customer_id = cursor.fetchone()[0]
                customer_ids.append(customer_id)
                logger.debug(f"Created customer {customer_id}")
            except Exception as e:
                errors += 1
                logger.error(f"Error creating customer: {e}")

        cursor.close()
        logger.info(f"Generated {len(customer_ids)} customers (errors: {errors})")
        return customer_ids

    def update_customers(self, customer_ids: List[int], count: int = 5):
        """Update existing customer records to simulate changes"""
        if not customer_ids:
            logger.debug("No customers to update")
            return

        cursor = self.conn.cursor()
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
                    cursor.execute("""
                        UPDATE customers
                        SET address = %s, city = %s, state = %s, zip_code = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE customer_id = %s
                    """, (
                        fake.street_address(),
                        fake.city(),
                        fake.state_abbr(),
                        fake.zipcode(),
                        customer_id
                    ))
                elif update_type == 'phone':
                    cursor.execute("""
                        UPDATE customers
                        SET phone = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE customer_id = %s
                    """, (fake.phone_number(), customer_id))
                else:  # email
                    cursor.execute("""
                        UPDATE customers
                        SET email = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE customer_id = %s
                    """, (fake.unique.email(), customer_id))

                logger.debug(f"Updated customer {customer_id} ({update_type})")
                updated += 1
            except Exception as e:
                errors += 1
                logger.error(f"Error updating customer {customer_id}: {e}")

        cursor.close()
        logger.info(f"Updated {updated} customers (errors: {errors})")

    def generate_orders(self, customer_ids: List[int], count: int = 20) -> List[int]:
        """Generate new order records"""
        if not customer_ids:
            logger.debug("No customer IDs available to generate orders")
            return []

        logger.info(f"Generating {count} new orders...")
        order_ids = []
        cursor = self.conn.cursor()
        errors = 0

        for _ in range(count):
            customer_id = random.choice(customer_ids)
            order_status = random.choice(self.order_statuses)
            total_amount = round(random.uniform(10.0, 1000.0), 2)
            order_date = datetime.now() - timedelta(days=random.randint(0, 90))

            try:
                cursor.execute("""
                    INSERT INTO orders (customer_id, order_date, order_status, total_amount)
                    VALUES (%s, %s, %s, %s)
                    RETURNING order_id
                """, (customer_id, order_date, order_status, total_amount))
                order_id = cursor.fetchone()[0]
                order_ids.append(order_id)
                logger.debug(f"Created order {order_id} for customer {customer_id} (${total_amount})")
            except Exception as e:
                errors += 1
                logger.error(f"Error creating order: {e}")

        cursor.close()
        logger.info(f"Generated {len(order_ids)} orders (errors: {errors})")
        return order_ids

    def generate_order_items(self, order_ids: List[int]):
        """Generate order items for orders"""
        if not order_ids:
            logger.debug("No order IDs available to generate order items")
            return

        logger.info(f"Generating order items for {len(order_ids)} orders...")
        cursor = self.conn.cursor()
        total_items = 0
        errors = 0

        for order_id in order_ids:
            # Each order has 1-5 items
            num_items = random.randint(1, 5)

            for _ in range(num_items):
                product_name = random.choice(self.products)
                quantity = random.randint(1, 3)
                unit_price = round(random.uniform(10.0, 500.0), 2)

                try:
                    cursor.execute("""
                        INSERT INTO order_items (order_id, product_name, quantity, unit_price)
                        VALUES (%s, %s, %s, %s)
                    """, (order_id, product_name, quantity, unit_price))
                    total_items += 1
                except Exception as e:
                    errors += 1
                    logger.error(f"Error creating order item: {e}")

        cursor.close()
        logger.info(f"Generated {total_items} order items (errors: {errors})")

    def update_order_status(self, order_ids: List[int], count: int = 10):
        """Update order statuses to simulate state changes"""
        if not order_ids:
            logger.debug("No orders to update")
            return

        cursor = self.conn.cursor()
        update_count = min(count, len(order_ids))
        selected_ids = random.sample(order_ids, update_count)

        logger.info(f"Updating status for {update_count} orders...")
        updated = 0
        errors = 0

        for order_id in selected_ids:
            new_status = random.choice(self.order_statuses)

            try:
                cursor.execute("""
                    UPDATE orders
                    SET order_status = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE order_id = %s
                """, (new_status, order_id))
                logger.debug(f"Updated order {order_id} status to {new_status}")
                updated += 1
            except Exception as e:
                errors += 1
                logger.error(f"Error updating order {order_id}: {e}")

        cursor.close()
        logger.info(f"Updated {updated} order statuses (errors: {errors})")

    def get_all_customer_ids(self) -> List[int]:
        """Get all customer IDs from database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT customer_id FROM customers")
        customer_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return customer_ids

    def get_all_order_ids(self) -> List[int]:
        """Get all order IDs from database"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT order_id FROM orders")
        order_ids = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return order_ids


def main():
    """Main function to run data generation"""
    import os

    db_config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': os.getenv('POSTGRES_PORT', '5432'),
        'database': os.getenv('POSTGRES_DB', 'source_db'),
        'user': os.getenv('POSTGRES_USER', 'postgres'),
        'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
    }

    generator = DataGenerator(db_config)

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
