PYTHON ?= python3
VENV ?= venv
PIP := $(VENV)/bin/pip
PY := $(VENV)/bin/python
COMPOSE ?= docker compose
EXP_DIR ?= experiment
EXP_API := $(EXP_DIR)/api
EXP_DB := $(EXP_DIR)/yaal.db
EXP_DB_URL = sqlite3:///$(abspath $(EXP_DB))
EXP_SCHEMA := docker/sqlite/schema.sql
EXP_FIXTURE_API := tests/fixtures/api

.PHONY: help venv install test test-unit test-integration test-all example yaal \
	experiment experiment-init experiment-reset experiment-clean \
	integration-up integration-down integration-ps clean \
	test-csharp test-csharp-integration example-csharp

help:
	@echo "Targets:"
	@echo "  make install            Create venv and pip install -e . (editable)"
	@echo "  make test               Run unit tests"
	@echo "  make test-integration   Start Docker DBs (Postgres/MySQL/ClickHouse) and run integration tests"
	@echo "  make test-all           Unit + integration tests"
	@echo "  make example            Demo: yaal query user/get --arg id=1"
	@echo "  make yaal ARGS='...'    Run yaal CLI (query / explain / list)"
	@echo "  make experiment         FS+SQLite sandbox (init if needed); ARGS defaults to query user/get"
	@echo "  make experiment-init    Create $(EXP_DIR)/api + seed $(EXP_DB)"
	@echo "  make experiment-reset   Reseed $(EXP_DB) only (keep API edits)"
	@echo "  make experiment-clean   Remove $(EXP_DIR)/"
	@echo "  make test-csharp        Run .NET tests in sdk container (SQLite / unit)"
	@echo "  make test-csharp-integration  Compose DBs + .NET tests in sdk container"
	@echo "  make example-csharp     Run csharp example in sdk container"
	@echo "  make integration-up     Start Postgres/MySQL/ClickHouse (docker compose)"
	@echo "  make integration-down   Stop and remove compose containers/volumes"
	@echo "  make clean              Remove venv, caches, and experiment sandbox"

venv:
	$(PYTHON) -m venv $(VENV)
	$(PIP) install --upgrade pip

install: venv
	$(PIP) install -e .

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
	$(PY) -m yaal_cli query user/get --arg id=1

yaal:
	$(PY) -m yaal_cli $(ARGS)

# Local FS descriptor tree + persistent SQLite for editing/experiments.
define seed_experiment_db
	$(PY) -c "import sqlite3; from pathlib import Path; \
p = Path('$(EXP_DB)'); p.parent.mkdir(parents=True, exist_ok=True); \
p.unlink(missing_ok=True); \
sqlite3.connect(p).executescript(Path('$(EXP_SCHEMA)').read_text())"
endef

experiment-init:
	mkdir -p $(EXP_DIR)
	rm -rf $(EXP_API)
	cp -R $(EXP_FIXTURE_API) $(EXP_API)
	$(seed_experiment_db)

experiment-reset:
	@test -d $(EXP_API) || (echo "missing $(EXP_API); run make experiment-init first" && exit 1)
	$(seed_experiment_db)

experiment-clean:
	rm -rf $(EXP_DIR)

experiment:
	@if [ ! -d "$(EXP_API)" ] || [ ! -f "$(EXP_DB)" ]; then $(MAKE) experiment-init; fi
	$(PY) -m yaal_cli --api $(EXP_API) --db '$(EXP_DB_URL)' --debug $(if $(ARGS),$(ARGS),query user/get --arg id=1)

# .NET tests always run in mcr.microsoft.com/dotnet/sdk:8.0 (no local SDK needed).
test-csharp:
	$(COMPOSE) --profile csharp run --rm --no-deps dotnet-test

test-csharp-integration:
	$(COMPOSE) --profile csharp run --rm -e YAAL_INTEGRATION=1 dotnet-test

example-csharp:
	$(COMPOSE) --profile csharp run --rm --no-deps dotnet-test \
		dotnet run --project csharp/examples/Yaal.Example/Yaal.Example.csproj

clean: experiment-clean
	rm -rf $(VENV) __pycache__ .pytest_cache
	find . -type d -name '__pycache__' -prune -exec rm -rf {} +
	find . -type f -name '*.py[co]' -delete
	find csharp -type d \( -name bin -o -name obj \) -prune -exec rm -rf {} +
