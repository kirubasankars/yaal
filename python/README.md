# Yaal for Python

Python implementation of the Yaal SQL→JSON library. Descriptor-driven queries, optional-filter SQL DSL, nested JSON shaping, and multi-engine providers.

Package: `yaal` `0.4.0` (MIT). Database drivers are extras — add the client your app uses. SQLite is stdlib.

```bash
pip install yaal
pip install 'yaal[postgres]'   # or [mysql] / [clickhouse]
```

From a clone: `make install` (`pip install -e .`). CLI: `yaal` (same as `python -m yaal_cli`).

Docs: [examples](../docs/examples.md) · [descriptors](../docs/descriptors.md) · [index](../docs/README.md).

Runnable tour (get / list / page / create + explain): `make example`.

## Requirements

- Python 3.9+

## Quick start

From the repo root:

```bash
make install
make example                     # full fixture tour
make test                        # unit tests (SQLite)
make test-integration            # also Postgres/MySQL/ClickHouse
```

### Programmatic usage

```python
from yaal import Yaal

y = Yaal("tests/fixtures/api", debug=True)
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
# y.setup_data_provider("db", MyContextManager())  # app-supplied provider

result = y.query("user/get", args={"id": 1})
raw = y.query_json("user/get", args={"id": 1})
page = y.query("user/page", args={"page": 1, "page_size": 10})

for twig in y.explain_sql("user/get", args={"id": 1}):
    print(twig["sql"])
```

Descriptors are shared with the .NET library under [`../tests/fixtures/api/`](../tests/fixtures/api/) (`user/get`, `user/nested`, `user/list`, `user/page`, `report/summary`, `user/combine`).

## Database URLs

| Engine | Example |
|---|---|
| SQLite (absolute) | `sqlite3:////tmp/app.db` |
| SQLite (relative) | `sqlite3://./data/app.db` |
| SQLite (memory) | `sqlite3:///` |
| Postgres | `postgresql://user:pass@127.0.0.1:5432/yaal` |
| MySQL | `mysql://user:pass@127.0.0.1:3306/yaal` |
| ClickHouse | `clickhouse://user:pass@127.0.0.1:9000/yaal` |

## Layout

```text
python/
  src/                 # library (flat modules: yaal, yaal_cli, …)
  tests/unit/
  tests/integration/
  examples/demo.py
```

Shared fixtures stay at repo-root [`tests/fixtures/`](../tests/fixtures/).

## Tests

```bash
make test                        # python/tests/unit
make test-integration            # Compose DBs + python/tests/integration
```
