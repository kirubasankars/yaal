# Python

Package `yaal` (MIT). SQLite is in the standard library. Other engines are extras.

```bash
pip install yaal
pip install 'yaal[postgres]'    # or [mysql], [clickhouse], [engines]
make install                    # from a clone: venv + pip install -e .
```

CLI: `yaal`. Library layout: `python/src`, tests, and `python/examples/demo.py`. See [python/README.md](https://github.com/kirubasankars/yaal/blob/main/python/README.md).

Descriptors are the shared tree under `tests/fixtures/api/`. This page is only the runtime.

## API

```python
from yaal import Yaal

y = Yaal("path/to/api", debug=True)  # or precompiled="path/to/precompiled"
y.setup_data_provider("db", "sqlite3:////tmp/app.db")

y.query("user/get", args={"id": 1})
y.query_json("user/get", args={"id": 1})
y.explain_sql("user/get", args={"id": 1})
y.query("user/get", args={"id": 1}, output_mapper="summary")
y.clear_cache()
```

`debug=True` reloads SQL and JSON and ignores `precompiled`. It is not a log level.

`query` returns nested objects. `query_json` returns a JSON string. `explain_sql` returns one dict per twig: `sql`, `parameters`, and `connection`.

Payload names (no `$args.` prefix) use `payload=`:

```python
y.query("user/create", payload={"id": 99, "name": "newbie"})
```

## Custom provider

`setup_data_provider` accepts a URL or an object with `get_context`, `begin`, `execute`, `end`, and `error`. Use that for logging, pools, or fakes. The second argument’s SQL is already compiled.

## URLs

[Database URLs](../reference/database-urls.md). Postgres default pool max 20 (`pool_size`, `minconn`, `maxconn`). MySQL default 10 (`pool_size`).
