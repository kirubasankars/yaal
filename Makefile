PYTHON ?= python3
VENV ?= venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
COMPOSE ?= docker compose

.PHONY: help venv install test test-unit test-integration test-all example yaal \
	integration-up integration-down integration-ps clean \
	test-csharp test-csharp-integration example-csharp

help:
	@echo "Targets:"
	@echo "  make install            Create venv and install dependencies"
	@echo "  make test               Run unit tests"
	@echo "  make test-integration   Start Docker DBs (Postgres/MySQL/ClickHouse) and run integration tests"
	@echo "  make test-all           Unit + integration tests"
	@echo "  make example            Demo: yaal_cli.py query user/get --arg id=1"
	@echo "  make yaal ARGS='...'    Run yaal_cli.py (query / explain / list)"
	@echo "  make test-csharp        Run .NET tests in sdk container (SQLite / unit)"
	@echo "  make test-csharp-integration  Compose DBs + .NET tests in sdk container"
	@echo "  make example-csharp     Run csharp example in sdk container"
	@echo "  make integration-up     Start Postgres/MySQL/ClickHouse (docker compose)"
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
	$(COMPOSE) up -d --wait postgres mysql clickhouse

integration-down:
	$(COMPOSE) --profile csharp down -v

integration-ps:
	$(COMPOSE) ps

test-integration: integration-up
	YAAL_INTEGRATION=1 $(PY) -m unittest discover -s tests/integration -v

test-all: test-unit test-integration

example:
	$(PY) yaal_cli.py query user/get --arg id=1

yaal:
	$(PY) yaal_cli.py $(ARGS)

# .NET tests always run in mcr.microsoft.com/dotnet/sdk:8.0 (no local SDK needed).
test-csharp:
	$(COMPOSE) --profile csharp run --rm --no-deps dotnet-test

test-csharp-integration:
	$(COMPOSE) --profile csharp run --rm -e YAAL_INTEGRATION=1 dotnet-test

example-csharp:
	$(COMPOSE) --profile csharp run --rm --no-deps dotnet-test \
		dotnet run --project csharp/examples/Yaal.Example/Yaal.Example.csproj

clean:
	rm -rf $(VENV) __pycache__ .pytest_cache
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type f -name '*.py[co]' -delete
	rm -rf csharp/**/bin csharp/**/obj
