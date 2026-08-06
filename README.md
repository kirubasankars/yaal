# Yaal

**Yaal is a subtractive SQL ORM.**

You author full SQL (plus YAML shapes). At bind time Yaal **subtracts** unused `optional(...)` / null-filter fragments, runs the remaining statements (optionally across named databases), and shapes flat rows into **nested JSON**. Aggregations, `WITH` / CTEs, and window functions stay ordinary SQL—not a query-builder escape hatch.

That is the opposite of additive ORMs that build SQL up from models. Yaal is not ActiveRecord: no entity tracking, migrations, or query-builder DSL. SQL files remain the source of truth.

Pipeline: *write SQL → subtract optionals → run (any named DB) → shape → JSON*.

License: [MIT](LICENSE). Version: `0.1.0` (Python package + NuGet metadata). Python and .NET 8 share the same descriptor files.

## Features

Learning path: [`docs/learn.md`](docs/learn.md). Full walkthroughs: [`docs/examples.md`](docs/examples.md) (feature index at the top).

### Subtractive filters

`optional(...)` / null groups are **removed** when params are null. [Full example →](docs/examples.md#optional-list--userlist)

```sql
--($args.active integer)--
select ... from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
```

| Args | Compiled predicate | Binds |
|---|---|---|
| *(omitted)* | predicate removed; `where 1 = 1` | `[]` |
| `active=1` | `and (u.active = ?)` | `[1]` |

```bash
yaal explain user/list
yaal explain user/list --arg active=1
```

### Dynamic ORDER BY

Allowlisted `sort()` / `dir()` splice author expressions only — never client SQL. Null `sort` elides `ORDER BY` unless the header sets a default (`string = id`). Supports multi-column sort (`sort=name,id` + `dir=desc,asc`), `NULLS FIRST/LAST` (`dir=desc_nulls_last`), and mixing with a static tiebreaker column. [Details →](docs/descriptors.md#dynamic-order-by--sort--dir)

```sql
--($args.sort string = id, $args.dir string = asc)--
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}}),
  u.user_id asc
```

```bash
yaal query user/list --arg sort=name --arg dir=desc
yaal query user/list --arg sort=name,id --arg dir=desc,asc
```

### JSON out

Every `query` returns nested JSON. [Full example →](docs/examples.md#nested-get--userget)

```bash
yaal query user/get --arg id=1
# {"id":1,"name":"admin","roles":[{"id":1,"name":"Administrator"},...]}
```

### Output shaping

`mapped`, `partition_by`, `parent_rows`, or child SQL (`$.roles.sql`). [get](docs/examples.md#nested-get--userget) · [nested](docs/examples.md#nested-child-sql--usernested)

```yaml
roles:
  type: array
  partition_by: role_id
  parent_rows: true
  properties:
    id: { mapped: role_id }
    name: { mapped: role_name }
```

### Multi-query + data passing

`--sql--` twigs share binds; `$mode=params` copies columns onto `$params` for later twigs. [Full example →](docs/examples.md#paginated-nest--userpage)

```sql
SELECT 'params' AS "$mode", COUNT(*) AS total_count FROM users WHERE active = 1
--sql--
SELECT {{$args.page}} AS page, {{$args.page_size}} AS page_size,
       {{$params.total_count}} AS total_count
```

### API pagination

Sibling `$.paging.sql` + `$.data.sql` → `{ paging, data }` via `$mode=params`. [Full example →](docs/examples.md#paginated-nest--userpage)

```bash
yaal query user/page --arg page=1 --arg page_size=10
# {"paging":{"page":1,"page_size":10,"total_count":2},"data":[...]}
```

### Multi-database

Named providers + `--sql(name)--` twigs in one operation. [Full example →](docs/examples.md#multi-database--usercombine)

```python
y.setup_data_provider("db", "sqlite3:///" + app_db)
y.setup_data_provider("flags", "sqlite3:///" + flags_db)
y.query("user/combine", args={"id": 1})
# {"app":{"id":1,"name":"admin"},"flags":{"user_id":1,"vip":1}}
```

```sql
--sql(flags)--
SELECT f.user_id, f.vip FROM external_flags f WHERE f.user_id = {{$args.id}}
```

### Real SQL (`WITH` / aggregations)

CTEs and aggregates stay ordinary SQL. [Full example →](docs/examples.md#real-sql--reportsummary)

```sql
WITH role_counts AS (
  SELECT user_id, COUNT(*) AS role_count FROM user_roles GROUP BY user_id
)
SELECT COUNT(*) AS user_count, ... FROM users u LEFT JOIN role_counts rc ...
```

```bash
yaal query report/summary
# {"user_count":2,"active_count":2,"assignment_count":3}
```

### Dual runtime

Python and .NET 8 share [`tests/fixtures/api/`](tests/fixtures/api/). [Full example →](docs/examples.md#dual-runtime-python--c)

```python
y.query("user/get", args={"id": 1})
```

```csharp
y.Query("user/get", args: new { id = 1 });
```

### Ahead-of-time compile

`yaal compile` / `precompiled=...`; elision still runs per request. [Full example →](docs/examples.md#precompiled-descriptors)

```bash
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  query user/get --arg id=1
```

## Install

```bash
make install          # venv + pip install -e .
# or: pip install -e .
```

CLI entry point after install: `yaal` (same as `python yaal_cli.py`).

C# / .NET 8: add a project reference to [`csharp/src/Yaal/Yaal.csproj`](csharp/src/Yaal/Yaal.csproj) (packable as `Yaal` `0.1.0`; not published to NuGet yet).

## Quick start

```bash
make install
make example                          # examples/demo.py — all fixtures + explain
make example-csharp                   # same tour in .NET (Docker SDK)
make yaal ARGS='list'
make yaal ARGS='query user/get --arg id=1'

# editable FS tree + persistent SQLite under experiment/
make experiment-init
make experiment
make experiment ARGS='query user/page --arg page=1 --arg page_size=10'
make experiment-reset                 # reseed DB only

# same API sandbox against Compose ClickHouse
make experiment-clickhouse-init
make experiment-clickhouse
make experiment-clickhouse-reset      # truncate+reseed CH (keep API edits)
```

`make example` runs [`examples/demo.py`](examples/demo.py): temp SQLite from `docker/sqlite/schema.sql` (+ flags DB), then get / nested / list / page / `report/summary` / `user/combine` plus `explain` elision (read-only). CLI commands with `--db` omitted also seed a temp SQLite DB.

`make experiment` uses a local sandbox at `experiment/` (gitignored): a copy of `tests/fixtures/api` plus `yaal.db`. Edit `experiment/api/` and re-run; `make experiment-reset` reseeds the DB without wiping API edits.

`make experiment-clickhouse` shares `experiment/api/` and points `--db` at Compose ClickHouse (`clickhouse://yaal:yaal@127.0.0.1:9000/yaal`). It starts the `clickhouse` service if needed; `experiment-clickhouse-reset` reloads rows via [`docker/clickhouse/experiment_seed.sql`](docker/clickhouse/experiment_seed.sql). (`user/combine` still needs a second SQLite flags DB — use the SQLite experiment for that.)

### CLI

```bash
# zero-config demo (temp SQLite + tests/fixtures/api)
yaal query user/get --arg id=1
yaal explain user/get --arg id=1
yaal list

# your own descriptors / database
yaal query orders/list --api ./my-api --db 'sqlite3:////tmp/app.db' --args '{"status":"open"}'
```

### Programmatic usage

```python
from yaal import Yaal

y = Yaal("tests/fixtures/api", debug=True)
y.setup_data_provider("db", "sqlite3:////tmp/app.db")

result = y.query("user/get", args={"id": 1})
# {'id': 1, 'name': 'admin', 'roles': [{'id': 1, 'name': 'Administrator'}, ...]}
```

Preview compiled SQL (after null-filter elision):

```python
for twig in y.explain_sql("user/get", args={"id": 1}):
    print(twig["sql"], twig["parameters"])
```

## Documentation

Operations are folders of `*.sql` (+ `$.output.yaml`), discovered filesystem-first and called by path (`y.query("user/get", ...)`). Full reference (parameters, output shaping, multi-twig / `$mode`, pagination, precompile, database URLs, errors, public API) lives in [`docs/descriptors.md`](docs/descriptors.md) — not duplicated here.

- [`docs/learn.md`](docs/learn.md) — step-by-step learning guide
- [`docs/examples.md`](docs/examples.md) — end-to-end walkthroughs (SQL, YAML, sample JSON, CLI/Python/C#)
- [`docs/descriptors.md`](docs/descriptors.md) — the full reference above
- [`docs/why-sql-first.md`](docs/why-sql-first.md) — why SQL-first fits ClickHouse-like engines and complex reporting apps
- [`docs/README.md`](docs/README.md) — full docs index (also covers the .NET port and runnable demos)

## Make targets

| Target | Purpose |
|---|---|
| `make install` | Create `venv` and `pip install -e .` |
| `make test` | Unit tests |
| `make test-integration` | Start Docker Postgres/MySQL/ClickHouse and run integration tests |
| `make test-all` | Unit + integration |
| `make example` | Run `examples/demo.py` (all fixture ops + explain) |
| `make example-csharp` | Same tour in .NET SDK container |
| `make yaal ARGS='...'` | Pass-through to CLI (`query` / `explain` / `list` / `compile`) |
| `make experiment` | FS+SQLite sandbox under `experiment/` (init if needed) |
| `make experiment-clickhouse` | Same API sandbox against Compose ClickHouse |
| `make experiment-init` / `experiment-reset` / `experiment-clean` | Create, reseed SQLite, or remove sandbox |
| `make experiment-clickhouse-init` / `experiment-clickhouse-reset` | Start/seed or reseed ClickHouse (keep API edits) |
| `make integration-up` / `integration-down` | Manage compose DBs |

SQLite-only usage does **not** need Docker. Compose is only for Postgres/MySQL/ClickHouse integration tests.

## Tests

```bash
make test                 # tests/unit
YAAL_INTEGRATION=1 make test-integration
make test-csharp          # .NET unit tests (sdk container)
```

CI (GitHub Actions) runs Python unit tests and .NET tests on every PR. Shared SQL compile goldens live under [`tests/fixtures/sql_compile/`](tests/fixtures/sql_compile/). Descriptor fixtures live under [`tests/fixtures/api/`](tests/fixtures/api/).

## C# (.NET 8)

A full-parity .NET port lives under [`csharp/`](csharp/). See [`csharp/README.md`](csharp/README.md). Tests run in a .NET SDK container (no local `dotnet` required):

```bash
make test-csharp
make test-csharp-integration
make example-csharp
```
