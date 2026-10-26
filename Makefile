PYTHON ?= python

.PHONY: install setup lint format test test-unit test-integration generate sample-data ingest silver gold run full-refresh

install:
	$(PYTHON) -m pip install -e ".[dev]"

setup:
	$(PYTHON) -m icebergflow setup

lint:
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

format:
	$(PYTHON) -m ruff format .
	$(PYTHON) -m ruff check --fix .

test:
	$(PYTHON) -m pytest

test-unit:
	$(PYTHON) -m pytest tests/unit

test-integration:
	$(PYTHON) -m pytest tests/integration

generate:
	$(PYTHON) -m icebergflow generate --days 14 --schema-v2-from 2024-01-10

sample-data:
	$(PYTHON) -m icebergflow generate --output data/sample --days 3 --orders-per-day 40 		--customers 60 --products 24 --seed 42 --schema-v2-from 2024-01-03

ingest:
	$(PYTHON) -m icebergflow ingest

silver:
	$(PYTHON) -m icebergflow silver

gold:
	$(PYTHON) -m icebergflow gold

run:
	$(PYTHON) -m icebergflow run

full-refresh:
	$(PYTHON) -m icebergflow run --full-refresh
