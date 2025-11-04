"""
Main orchestrator for the DBT CDC pipeline
Coordinates data generation, CDC extraction, and DBT transformations
"""
import os
import sys
import time
import subprocess
from datetime import datetime
from data_generator import DataGenerator
from cdc_extractor import CDCExtractor
from logging_config import get_logger, LogContext, MetricsLogger

# Initialize logger
logger = get_logger(__name__)


def parse_dbt_output(output: str, command: str):
    """Parse DBT output to extract useful information"""
    import re

    if command == "run":
        # Extract model execution info
        model_pattern = r'(\d+) of (\d+) (OK|ERROR) created (\w+) model (\w+\.)?(\w+)\s+\[(\w+) in ([\d.]+)s\]'
        models_run = []

        for line in output.split('\n'):
            match = re.search(model_pattern, line)
            if match:
                status = match.group(3)
                layer = match.group(5) if match.group(5) else ""
                model = match.group(6)
                result_type = match.group(7)
                duration = match.group(8)

                models_run.append({
                    'status': status,
                    'model': f"{layer}{model}",
                    'result': result_type,
                    'duration': duration
                })

        return models_run

    elif command == "test":
        # Extract test results
        test_pattern = r'Completed (\d+) tests?, (\d+) passed, (\d+) (failed|warned)'
        for line in output.split('\n'):
            match = re.search(test_pattern, line)
            if match:
                return {
                    'total': int(match.group(1)),
                    'passed': int(match.group(2)),
                    'failed': int(match.group(3))
                }

    return None


def run_dbt_command(command: str, project_dir: str = "/app/dbt_project"):
    """Run a DBT command"""
    with LogContext(logger, f"DBT {command}"):
        try:
            result = subprocess.run(
                f"cd {project_dir} && dbt {command} --profiles-dir .",
                shell=True,
                capture_output=True,
                text=True
            )

            # Parse and log DBT output
            if result.stdout:
                parsed = parse_dbt_output(result.stdout, command)

                if command == "run" and parsed:
                    logger.info(f"DBT executed {len(parsed)} model(s):")
                    for model in parsed:
                        status_symbol = "✓" if model['status'] == "OK" else "✗"
                        logger.info(f"  {status_symbol} {model['model']}: {model['result']} ({model['duration']}s)")

                elif command == "test" and parsed:
                    logger.info(f"DBT Tests: {parsed['passed']}/{parsed['total']} passed")
                    if parsed['failed'] > 0:
                        logger.warning(f"  {parsed['failed']} test(s) failed")

                # Log full output at debug level
                for line in result.stdout.split('\n'):
                    if line.strip():
                        logger.debug(f"DBT: {line}")

            if result.stderr:
                for line in result.stderr.split('\n'):
                    if line.strip():
                        logger.debug(f"DBT stderr: {line}")

            if result.returncode == 0:
                return True, result.stdout
            else:
                logger.error(f"DBT {command} failed with return code {result.returncode}")
                return False, result.stdout

        except Exception as e:
            logger.error(f"Error running DBT {command}: {e}", exc_info=True)
            return False, ""


def show_table_counts(duckdb_path, stage: str):
    """Show table counts at a specific stage"""
    import duckdb
    conn = duckdb.connect(duckdb_path, read_only=True)

    try:
        if stage == "source":
            logger.info("Source Tables:")
            tables = [
                ("source.cdc_customers", "customers"),
                ("source.cdc_orders", "orders"),
                ("source.cdc_order_items", "order_items")
            ]
        elif stage == "raw":
            logger.info("Raw Layer (CDC Tables):")
            tables = [
                ("raw.customers_cdc", "customers_cdc"),
                ("raw.orders_cdc", "orders_cdc"),
                ("raw.order_items_cdc", "order_items_cdc")
            ]
        elif stage == "marts":
            logger.info("Marts Layer (Analytics Tables):")
            tables = [
                ("main_marts.customer_order_summary", "customer_order_summary"),
                ("main_marts.dim_customers_scd2", "dim_customers_scd2"),
                ("main_marts.fact_orders", "fact_orders")
            ]
        else:
            return

        for full_name, short_name in tables:
            try:
                count = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()[0]
                logger.info(f"  {short_name}: {count:,} records")
            except Exception:
                logger.info(f"  {short_name}: <not created yet>")

    except Exception as e:
        logger.debug(f"Error showing {stage} counts: {e}")
    finally:
        conn.close()


def run_pipeline_once(duckdb_path):
    """Run one iteration of the pipeline"""
    logger.info("="*60)
    logger.info(f"Pipeline Iteration Started - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60)
    logger.info("")
    logger.info("Data Flow:")
    logger.info("  Step 1: Generate Data → source.cdc_* tables")
    logger.info("  Step 2: Extract CDC → raw.*_cdc tables")
    logger.info("  Step 3: DBT Transform → staging.* → marts.*")
    logger.info("  Step 4: DBT Tests")
    logger.info("")

    metrics = MetricsLogger(logger)
    pipeline_start = datetime.now()

    # Step 1: Generate/Update data in DuckDB source tables
    logger.info("[Step 1/4] Generating data in DuckDB source tables...")
    generator = DataGenerator(duckdb_path)
    try:
        with LogContext(logger, "Data generation"):
            generator.connect()

            # Get existing IDs
            customer_ids = generator.get_all_customer_ids()
            order_ids = generator.get_all_order_ids()

            if not customer_ids:
                # Initial load
                logger.info("Performing initial data load...")
                customer_ids = generator.generate_customers(20)
                order_ids = generator.generate_orders(customer_ids, 50)
                generator.generate_order_items(order_ids)
                metrics.record("data_load_type", "initial")
                metrics.record("new_customers", len(customer_ids))
                metrics.record("new_orders", len(order_ids))
            else:
                # Incremental updates
                logger.info(f"Performing incremental updates (existing: {len(customer_ids)} customers, {len(order_ids)} orders)...")
                initial_customer_count = len(customer_ids)
                initial_order_count = len(order_ids)

                new_customers = generator.generate_customers(5)
                customer_ids.extend(new_customers)

                generator.update_customers(customer_ids, 3)

                new_orders = generator.generate_orders(customer_ids, 10)
                order_ids.extend(new_orders)
                generator.generate_order_items(new_orders)

                generator.update_order_status(order_ids, 5)

                metrics.record("data_load_type", "incremental")
                metrics.record("new_customers", len(customer_ids) - initial_customer_count)
                metrics.record("new_orders", len(order_ids) - initial_order_count)
                metrics.record("customers_updated", 3)
                metrics.record("orders_updated", 5)

            logger.info("Data generation complete")

        # Show source table state
        show_table_counts(duckdb_path, "source")

    finally:
        generator.close()

    # Step 2: Extract CDC changes from source tables to raw layer
    logger.info("[Step 2/4] Extracting CDC changes from source tables to raw layer...")
    extractor = CDCExtractor(duckdb_path)
    try:
        with LogContext(logger, "CDC extraction"):
            extractor.connect()
            cdc_metrics = extractor.run_extraction()
            # Merge CDC metrics into pipeline metrics
            if cdc_metrics:
                for key, value in cdc_metrics.items():
                    metrics.record(f"cdc_{key}", value)
            logger.info("CDC extraction complete")
    finally:
        extractor.close()

    # Show raw layer state after CDC extraction
    show_table_counts(duckdb_path, "raw")

    # Step 3: Run DBT transformations
    logger.info("[Step 3/4] Running DBT transformations...")

    # Install DBT packages (first time only)
    if not os.path.exists("/app/dbt_project/dbt_packages"):
        logger.info("Installing DBT dependencies...")
        run_dbt_command("deps")

    # Run DBT models
    success, dbt_output = run_dbt_command("run")

    if success:
        logger.info("DBT transformations complete")
        # Show marts layer state after DBT
        show_table_counts(duckdb_path, "marts")
    else:
        logger.error("DBT transformations failed")
        return False

    # Step 4: Run DBT tests
    logger.info("[Step 4/4] Running DBT tests...")
    test_success, test_output = run_dbt_command("test")
    metrics.record("dbt_tests_passed", test_success)

    # Calculate total pipeline duration
    pipeline_duration = (datetime.now() - pipeline_start).total_seconds()
    metrics.record("total_pipeline_duration_seconds", pipeline_duration)

    # Log metrics summary
    metrics.log_summary()

    logger.info("="*60)
    logger.info("Pipeline iteration complete!")
    logger.info("="*60)

    return True


def run_continuous_pipeline(duckdb_path, interval_seconds=30):
    """Run pipeline continuously with specified interval"""
    logger.info("="*60)
    logger.info("Starting Continuous CDC Pipeline")
    logger.info("="*60)
    logger.info(f"Interval: {interval_seconds} seconds")
    logger.info("Press Ctrl+C to stop")
    logger.info("="*60)

    iteration = 0
    failures = 0
    try:
        while True:
            iteration += 1
            logger.info(f"\n\nIteration #{iteration}")

            success = run_pipeline_once(duckdb_path)

            if not success:
                failures += 1
                logger.warning(f"Pipeline iteration failed (total failures: {failures}), but continuing...")
            else:
                logger.info("Pipeline iteration succeeded")

            logger.info(f"Waiting {interval_seconds} seconds until next iteration...")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("\n\nPipeline stopped by user")
        logger.info(f"Total iterations completed: {iteration}")
        logger.info(f"Successful iterations: {iteration - failures}")
        logger.info(f"Failed iterations: {failures}")


def show_results(duckdb_path):
    """Show some results from the data warehouse"""
    import duckdb

    logger.info("="*60)
    logger.info("Sample Query Results")
    logger.info("="*60)

    try:
        conn = duckdb.connect(duckdb_path)

        # Show customer summary
        logger.info("\n--- Top 10 Customers by Revenue ---")
        result = conn.execute("""
            SELECT
                customer_id,
                email,
                first_name || ' ' || last_name as name,
                total_orders,
                total_revenue,
                avg_order_value
            FROM main_marts.customer_order_summary
            ORDER BY total_revenue DESC
            LIMIT 10
        """).fetchall()

        for row in result:
            logger.info(f"  {row[1]}: ${row[4]:.2f} ({row[3]} orders, avg ${row[5]:.2f})")

        # Show SCD2 example
        logger.info("\n--- Customers with History (SCD2) ---")
        result = conn.execute("""
            SELECT
                customer_id,
                email,
                city,
                state,
                valid_from,
                valid_to,
                is_current
            FROM main_marts.dim_customers_scd2
            WHERE customer_id IN (
                SELECT customer_id
                FROM main_marts.dim_customers_scd2
                GROUP BY customer_id
                HAVING COUNT(*) > 1
            )
            ORDER BY customer_id, valid_from
            LIMIT 10
        """).fetchall()

        if result:
            for row in result:
                current = "CURRENT" if row[6] else "EXPIRED"
                logger.info(f"  Customer {row[0]}: {row[2]}, {row[3]} [{current}]")
        else:
            logger.info("  No customer history changes found yet")

        conn.close()

    except Exception as e:
        logger.error(f"Error showing results: {e}", exc_info=True)


def main():
    """Main entry point"""
    # Configuration
    duckdb_path = os.getenv('DUCKDB_PATH', '/data/warehouse.duckdb')
    mode = os.getenv('PIPELINE_MODE', 'once')  # 'once' or 'continuous'
    interval = int(os.getenv('CDC_POLL_INTERVAL', '30'))

    logger.info("DBT CDC Pipeline Orchestrator (Simplified DuckDB-only Architecture)")
    logger.info(f"Mode: {mode}")
    logger.info(f"DuckDB Path: {duckdb_path}")
    logger.info(f"Log Level: {os.getenv('LOG_LEVEL', 'INFO')}")

    # Create data directory
    os.makedirs('/data', exist_ok=True)
    os.makedirs('/data/logs', exist_ok=True)

    try:
        if mode == 'continuous':
            run_continuous_pipeline(duckdb_path, interval)
        else:
            # Run once
            run_pipeline_once(duckdb_path)

            # Show results
            show_results(duckdb_path)

            logger.info("\nPipeline completed successfully!")

    except Exception as e:
        logger.critical(f"Pipeline failed with exception: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
