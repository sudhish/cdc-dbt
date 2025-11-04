.PHONY: help setup build up down logs clean run-once run-continuous dbt-shell query

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

setup: ## Setup project (copy .env file)
	@echo "Setting up project..."
	@mkdir -p data
	@chmod 777 data
	@cp -n .env.example .env || true
	@echo "✓ .env file created (edit if needed)"
	@echo "✓ data directory created"

build: ## Build Docker images
	@echo "Building Docker images..."
	docker-compose build

up: ## Start services (note: no background services needed with simplified architecture)
	@echo "No background services to start with DuckDB-only architecture"
	@echo "Run 'make run-once' to execute the pipeline"

down: ## Stop all services
	@echo "Stopping services..."
	docker-compose down

logs: ## Show logs from all services
	docker-compose logs -f

clean: ## Clean up data and volumes
	@echo "Cleaning up..."
	docker-compose down -v
	rm -rf data/
	@echo "✓ Cleaned up"

run-once: ## Run pipeline once (build, generate data, extract CDC, run DBT)
	@echo "Running pipeline once..."
	@mkdir -p data
	docker-compose run --rm pipeline python /app/cdc_pipeline/orchestrator.py

run-continuous: ## Run pipeline continuously
	@echo "Running continuous pipeline..."
	docker-compose up pipeline

dbt-run: ## Run DBT models only
	@echo "Running DBT models..."
	docker-compose run --rm pipeline sh -c "cd /app/dbt_project && dbt run --profiles-dir ."

dbt-test: ## Run DBT tests
	@echo "Running DBT tests..."
	docker-compose run --rm pipeline sh -c "cd /app/dbt_project && dbt test --profiles-dir ."

dbt-docs: ## Generate and serve DBT documentation
	@echo "Generating DBT documentation..."
	docker-compose run --rm pipeline sh -c "cd /app/dbt_project && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir ."

shell: ## Open shell in pipeline container
	docker-compose run --rm pipeline /bin/bash

dbt-shell: ## Open DBT shell (run SQL queries interactively)
	docker-compose run --rm pipeline sh -c "cd /app/dbt_project && dbt run --profiles-dir . && python -c 'import duckdb; conn = duckdb.connect(\"/data/warehouse.duckdb\"); conn.execute(\"SET schema=\\\"marts\\\"\"); import IPython; IPython.embed()'"

query: ## Run a quick query on the warehouse
	@echo "Running sample queries..."
	docker-compose run --rm pipeline python -c "import duckdb; conn = duckdb.connect('/data/warehouse.duckdb'); print('\n=== Top Customers ==='); print(conn.execute('SELECT customer_id, email, total_revenue FROM main_marts.customer_order_summary ORDER BY total_revenue DESC LIMIT 5').df()); conn.close()"

generate-data: ## Generate sample data in DuckDB source tables
	@echo "Generating sample data..."
	docker-compose run --rm pipeline python /app/cdc_pipeline/data_generator.py

extract-cdc: ## Extract CDC changes from source tables to raw layer
	@echo "Extracting CDC changes..."
	docker-compose run --rm pipeline python /app/cdc_pipeline/cdc_extractor.py

# Complete demo workflow
demo: setup build ## Run complete demo (setup, build, run pipeline)
	@echo "Running complete demo..."
	@make run-once
	@echo "\n✓ Demo completed!"
	@echo "\nTry these commands:"
	@echo "  make query         - Run sample queries"
	@echo "  make dbt-run       - Run DBT transformations"
	@echo "  make run-once      - Run another pipeline iteration"
	@echo "  make shell         - Open shell in container"
