<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Database URLs

| Engine | Example |
|---|---|
| SQLite (absolute) | `sqlite3:////tmp/app.db` |
| SQLite (relative) | `sqlite3://./data/app.db` |
| SQLite (memory) | `sqlite3:///` |
| Postgres | `postgresql://user:pass@127.0.0.1:5432/yaal?pool_size=20` |
| MySQL | `mysql://user:pass@127.0.0.1:3306/yaal?pool_size=10` |
| ClickHouse | `clickhouse://user:pass@127.0.0.1:9000/yaal` |

These URLs are for the CLI, the example, and tests. Python opens them with `yaal_drivers.open`. C# opens them with `Yaal.Drivers.DriverRegistry.Open`. An application passes a provider to `query` and does not use a URL.

The same descriptors run on every engine above. Yaal does not translate SQL. `NULLS FIRST` / `LAST` from `dir()` is emitted as written; MySQL rejects it.

SQLite ships with Python. Other engines are extras for the `yaal` command: `yaal[postgres]`, `yaal[mysql]`, `yaal[clickhouse]`, or `yaal[engines]`. The .NET library does not reference a driver. See [Python](../appendix/python.md) and [C#](../appendix/csharp.md).

Pool query keys are noted in [Precompiled artifacts](precompiled-artifacts.md).
