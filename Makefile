PYTHON ?= python

.PHONY: install lint format test test-unit generate sample-data

install:
	$(PYTHON) -m pip install -e ".[dev]"

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

generate:
	$(PYTHON) -m icebergflow generate --days 14 --schema-v2-from 2024-01-10

sample-data:
	$(PYTHON) -m icebergflow generate --output data/sample --days 3 --orders-per-day 40 		--customers 60 --products 24 --seed 42 --schema-v2-from 2024-01-03
