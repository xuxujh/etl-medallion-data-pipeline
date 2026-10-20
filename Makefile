.PHONY: setup lint test ingest transform pipeline clean

PYTHON := python3

setup:
    $(PYTHON) -m pip install --upgrade pip
    $(PYTHON) -m pip install -e ".[dev]"

lint:
    $(PYTHON) -m ruff check src/ tests/

test:
    $(PYTHON) -m pytest --cov=src tests/

ingest:
    $(PYTHON) -m src.ingestion.extractor

transform:
    $(PYTHON) -m src.etl.processor

pipeline: ingest transform

clean:
    rm -rf data/bronze/*.parquet data/silver/*.parquet data/gold/*.parquet data/gold/*.duckdb
    rm -rf .pytest_cache .ruff_cache .coverage