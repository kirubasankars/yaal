<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Yaal for Python

Python implementation of the Yaal SQL→JSON library. Descriptor-driven queries, subtractive `optional()` / `optional_groups` SQL DSL, header array params (`integer[]` for `IN`), and nested JSON shaping.

Package: `yaal` `0.10.0` (MIT). The library does not open a connection. Pass a provider to `query`. Driver extras are for the `yaal` command. SQLite is stdlib.

```bash
pip install yaal
pip install 'yaal[postgres]'   # or [mysql] / [clickhouse]
```

From a clone: `make install` (`pip install -e .`). CLI: `yaal` (same as `python -m yaal_cli`).

Docs: [tutorial](../docs/tutorial/index.md) · [reference](../docs/reference/index.md) · [index](../docs/index.md). Preview with `make docs-serve`.

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

result = y.query(provider, "user/get", args={"id": 1})
raw = y.query_json(provider, "user/get", args={"id": 1})
page = y.query(provider, "user/page", args={"page": 1, "page_size": 10})

for twig in y.explain_sql(provider, "user/get", args={"id": 1}):
    print(twig["sql"])
```

### Precompiled descriptors

```bash
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
y = Yaal("tests/fixtures/api", precompiled="/tmp/yaal-precompiled")
```

`debug=True` forces live SQL/JSON and ignores `precompiled`. See [precompiled artifacts](../docs/reference/precompiled-artifacts.md).

Descriptors are shared with the .NET library under [`../tests/fixtures/api/`](../tests/fixtures/api/) (`user/get`, `user/nested`, `user/list`, `user/groups`, `user/page`, `report/summary`). Compile goldens for optional filters and groups: [`../tests/fixtures/sql_compile/`](../tests/fixtures/sql_compile/).

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
