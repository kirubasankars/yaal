PYTHON ?= python3
VENV ?= venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python

.PHONY: help venv install test test-unit test-integration test-all example \
	integration-up integration-down integration-ps clean

help:
	@echo "Targets:"
	@echo "  make install            Create venv and install dependencies"
	@echo "  make test               Run unit tests"
	@echo "  make test-integration   Start Docker DBs and run integration tests"
	@echo "  make test-all           Unit + integration tests"
	@echo "  make example            Run examples/run_user_get.py"
	@echo "  make integration-up     Start Postgres/MySQL (docker compose)"
	@echo "  make integration-down   Stop and remove compose containers/volumes"
	@echo "  make clean              Remove venv and caches"

venv:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv
	$(PIP) install -r requirements.txt

test: test-unit

test-unit:
	$(PY) -m unittest discover -s tests/unit -v

integration-up:
	docker compose up -d --wait

integration-down:
	docker compose down -v

integration-ps:
	docker compose ps

test-integration: integration-up
	YAAL_INTEGRATION=1 $(PY) -m unittest discover -s tests/integration -v

test-all: test-unit test-integration

example:
	$(PY) examples/run_user_get.py

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type f -name '*.py[co]' -delete
