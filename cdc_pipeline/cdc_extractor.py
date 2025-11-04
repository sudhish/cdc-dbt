"""
CDC Extractor to capture changes from DuckDB source tables and land them in DuckDB raw layer
"""
import os
from datetime import datetime
from typing import Dict, List, Any
import duckdb
import pandas as pd
from logging_config import get_logger, LogContext

# Initialize logger
logger = get_logger(__name__)


class CDCExtractor:
    """Extract CDC changes from DuckDB source tables and load into DuckDB raw layer"""

    def __init__(self, duckdb_path: str):
        self.duckdb_path = duckdb_path
        self.conn = None

    def connect(self):
        """Establish connection to DuckDB"""
        logger.info(f"Connecting to DuckDB at {self.duckdb_path}")
        self.conn = duckdb.connect(self.duckdb_path)
        logger.info("Connected to DuckDB successfully")

        # Initialize DuckDB schemas and tables
        self._init_duckdb_schema()

    def _init_duckdb_schema(self):
        """Initialize DuckDB schema and CDC tables"""
        # Create schemas
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
        self.conn.execute("CREATE SCHEMA IF NOT EXISTS marts")

        # Create CDC tables with Iceberg-compatible structure
        # Note: DuckDB doesn't natively support Iceberg, but we'll structure data
        # in a way that's compatible with Iceberg table format (append-only with metadata)

        # Customers CDC table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.customers_cdc (
                customer_id INTEGER,
                email VARCHAR,
                first_name VARCHAR,
                last_name VARCHAR,
                phone VARCHAR,
                address VARCHAR,
                city VARCHAR,
                state VARCHAR,
                zip_code VARCHAR,
                country VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                cdc_operation VARCHAR,  -- INSERT, UPDATE, DELETE
                cdc_timestamp TIMESTAMP,
                cdc_batch_id VARCHAR
            )
        """)

        # Orders CDC table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.orders_cdc (
                order_id INTEGER,
                customer_id INTEGER,
                order_date TIMESTAMP,
                order_status VARCHAR,
                total_amount DECIMAL(10, 2),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                cdc_operation VARCHAR,
                cdc_timestamp TIMESTAMP,
                cdc_batch_id VARCHAR
            )
        """)

        # Order Items CDC table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.order_items_cdc (
                order_item_id INTEGER,
                order_id INTEGER,
                product_name VARCHAR,
                quantity INTEGER,
                unit_price DECIMAL(10, 2),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                cdc_operation VARCHAR,
                cdc_timestamp TIMESTAMP,
                cdc_batch_id VARCHAR
            )
        """)

        # CDC metadata table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.cdc_metadata (
                table_name VARCHAR PRIMARY KEY,
                last_extracted_id INTEGER,
                last_extracted_at TIMESTAMP,
                total_records_extracted BIGINT
            )
        """)

        # Initialize metadata if not exists
        for table in ['customers', 'orders', 'order_items']:
            self.conn.execute(f"""
                INSERT INTO raw.cdc_metadata (table_name, last_extracted_id, last_extracted_at, total_records_extracted)
                SELECT '{table}', 0, CURRENT_TIMESTAMP, 0
                WHERE NOT EXISTS (SELECT 1 FROM raw.cdc_metadata WHERE table_name = '{table}')
            """)

        logger.info("DuckDB schema and CDC tables initialized")

    def extract_table_changes(self, table_name: str, id_column: str) -> pd.DataFrame:
        """Extract changes from DuckDB source table using high-water mark"""
        # Get last extracted ID
        result = self.conn.execute(f"""
            SELECT last_extracted_id
            FROM raw.cdc_metadata
            WHERE table_name = '{table_name}'
        """).fetchone()

        last_id = result[0] if result else 0

        # Get total count in source table
        source_count = self.conn.execute(f"SELECT COUNT(*) FROM source.cdc_{table_name}").fetchone()[0]

        # Extract new/updated records from source tables
        source_table = f"source.cdc_{table_name}"
        result = self.conn.execute(f"""
            SELECT *
            FROM {source_table}
            WHERE {id_column} > ?
            ORDER BY {id_column}
        """, (last_id,)).fetchdf()

        if result.empty:
            logger.debug(f"No new records in source.cdc_{table_name} (last processed ID: {last_id}, source has {source_count} total)")
            return pd.DataFrame()

        df = result

        # Add CDC metadata
        df['cdc_operation'] = 'INSERT'  # Simplified: treating all as inserts
        df['cdc_timestamp'] = datetime.now()
        df['cdc_batch_id'] = datetime.now().strftime('%Y%m%d_%H%M%S')

        return df

    def load_to_duckdb(self, df: pd.DataFrame, table_name: str, id_column: str):
        """Load CDC data to DuckDB"""
        if df.empty:
            return

        # Insert into DuckDB CDC table
        target_table = f"raw.{table_name}_cdc"

        try:
            # Use DuckDB's DataFrame INSERT
            self.conn.execute(f"INSERT INTO {target_table} SELECT * FROM df")

            # Update metadata
            max_id = df[id_column].max()
            record_count = len(df)

            self.conn.execute(f"""
                UPDATE raw.cdc_metadata
                SET
                    last_extracted_id = {max_id},
                    last_extracted_at = CURRENT_TIMESTAMP,
                    total_records_extracted = total_records_extracted + {record_count}
                WHERE table_name = '{table_name}'
            """)

        except Exception as e:
            logger.error(f"Error loading data to {target_table}: {e}", exc_info=True)
            raise

    def export_to_parquet(self, table_name: str):
        """Export CDC data to Parquet format (Iceberg-compatible)"""
        output_dir = "/data/iceberg"
        os.makedirs(output_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        parquet_path = f"{output_dir}/{table_name}_cdc_{timestamp}.parquet"

        try:
            self.conn.execute(f"""
                COPY (SELECT * FROM raw.{table_name}_cdc)
                TO '{parquet_path}' (FORMAT PARQUET, COMPRESSION SNAPPY)
            """)

            logger.info(f"Exported {table_name} to {parquet_path}")

        except Exception as e:
            logger.error(f"Error exporting {table_name} to Parquet: {e}", exc_info=True)
            raise

    def run_extraction(self) -> Dict[str, int]:
        """Run full CDC extraction for all tables and return metrics"""
        tables = [
            ('customers', 'customer_id'),
            ('orders', 'order_id'),
            ('order_items', 'order_item_id')
        ]

        logger.info("Starting CDC extraction for all tables")

        metrics = {
            'total_records_extracted': 0,
            'tables_processed': 0,
            'tables_with_changes': 0,
            'errors': 0
        }

        for table_name, id_column in tables:
            try:
                with LogContext(logger, f"Extract and load {table_name}"):
                    # Get count before extraction
                    before_count = self.conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}_cdc").fetchone()[0]

                    df = self.extract_table_changes(table_name, id_column)
                    records_extracted = len(df) if not df.empty else 0

                    if not df.empty:
                        metrics['tables_with_changes'] += 1
                        metrics['total_records_extracted'] += records_extracted
                        metrics[f'{table_name}_records'] = records_extracted

                    self.load_to_duckdb(df, table_name, id_column)

                    # Get count after extraction
                    after_count = self.conn.execute(f"SELECT COUNT(*) FROM raw.{table_name}_cdc").fetchone()[0]

                    # Log the extraction result
                    if records_extracted > 0:
                        logger.info(f"✓ {table_name}: Extracted {records_extracted} new records → raw.{table_name}_cdc (now has {after_count} total)")
                    else:
                        logger.info(f"○ {table_name}: No new records (raw.{table_name}_cdc has {after_count} total)")

                metrics['tables_processed'] += 1

            except Exception as e:
                logger.error(f"Error extracting {table_name}: {e}", exc_info=True)
                metrics['errors'] += 1

        # Log summary
        if metrics['total_records_extracted'] > 0:
            logger.info(f"CDC Extraction Complete: {metrics['total_records_extracted']} total new records extracted from {metrics['tables_with_changes']} table(s)")
        else:
            logger.info("CDC Extraction Complete: No new changes detected")

        return metrics


    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()


def main():
    """Main function to run CDC extraction"""
    import os

    duckdb_path = os.getenv('DUCKDB_PATH', '/data/warehouse.duckdb')

    extractor = CDCExtractor(duckdb_path)

    try:
        logger.info("Starting CDC extraction process")
        extractor.connect()

        metrics = extractor.run_extraction()

        logger.info("CDC extraction completed successfully!")
        logger.info(f"Metrics: {metrics}")

    except Exception as e:
        logger.critical(f"CDC extraction failed: {e}", exc_info=True)
    finally:
        extractor.close()


if __name__ == "__main__":
    main()
