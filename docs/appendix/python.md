<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Python

Package `yaal` (MIT). The library does not open a connection. Pass a provider to `query`. SQLite is in the standard library. Other engines are extras for the `yaal` command.

```bash
pip install yaal
pip install 'yaal[postgres]'    # or [mysql], [clickhouse], [engines]
make install                    # from a clone: venv + pip install -e .
```

CLI: `yaal`. Library layout: `python/src`, tests, and `python/examples/demo.py`. See [python/README.md](https://github.com/kirubasankars/yaal/blob/master/python/README.md).

Descriptors are the shared tree under `tests/fixtures/api/`. This page is only the runtime.

## API

```python
from yaal import Yaal

y = Yaal("path/to/api", debug=True)  # or precompiled="path/to/precompiled"

y.query(provider, "user/get", args={"id": 1})
y.query_json(provider, "user/get", args={"id": 1})
y.explain_sql(provider, "user/get", args={"id": 1})
y.query(provider, "user/get", args={"id": 1}, output_mapper="summary")
y.clear_cache()
```

`debug=True` reloads SQL and JSON and ignores `precompiled`. It is not a log level.

`query` returns nested objects. `query_json` returns a JSON string. `explain_sql` returns one dict per twig: `sql` and `parameters`. The placeholder comes from `provider.placeholder` (`?` or `%s`).

Payload names (no `$args.` prefix) use `payload=`:

```python
y.query(provider, "user/create", payload={"id": 99, "name": "newbie"})
```

## Application provider

The application opens the connection and passes a provider to `query`. Yaal compiles each twig, then calls `execute` with the SQL and bind values. It does not open, commit, roll back, or close that connection.

```python
class SqliteProvider:
    placeholder = "?"  # postgres, mysql, and clickhouse use "%s"

    def __init__(self, connection):
        self._connection = connection

    def begin(self):
        pass  # the connection is already open

    def execute(self, sql, parameters):
        cur = self._connection.cursor()
        try:
            cur.execute(sql, parameters)
            if cur.description is None:
                return [], cur.lastrowid
            names = [col[0] for col in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()], cur.lastrowid
        finally:
            cur.close()

    def end(self):
        pass  # the application commits and closes

    def error(self):
        pass  # the application rolls back and closes

connection = sqlite3.connect("/tmp/app.db")
provider = SqliteProvider(connection)
y.query(provider, "user/get", args={"id": 1})
```

`execute` returns `(rows, last_inserted_id)`. Rows are a list of dicts. `last_inserted_id` may be `None`. `placeholder` is `?` or `%s`. A second database is a second `query` call with a second provider.

`begin` and `end` are where an application may start a transaction or leave the connection alone. `error` is called when the query fails.

URL strings such as `sqlite3:////tmp/app.db` stay for the CLI, `python/examples/demo.py`, and tests. They call `yaal_drivers.open`. Those providers still open in `begin` and close in `end`. `yaal` does not import them.

## URLs

[Database URLs](../reference/database-urls.md). Postgres default pool max 20 (`pool_size`, `minconn`, `maxconn`). MySQL default 10 (`pool_size`).
