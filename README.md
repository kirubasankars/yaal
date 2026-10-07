# Yaal

**Yaal is a subtractive SQL ORM.**

You author full SQL (plus JSON shapes). At bind time Yaal **subtracts** unused `optional(...)` / null-filter fragments, runs the remaining statements (optionally across named databases), and shapes flat rows into **nested JSON**. Aggregations, `WITH` / CTEs, and window functions stay ordinary SQL—not a query-builder escape hatch.

That is the opposite of additive ORMs that build SQL up from models. Yaal is not ActiveRecord: no entity tracking, migrations, or query-builder DSL. SQL files remain the source of truth.

Pipeline: *write SQL → subtract optionals → run (any named DB) → shape → JSON*.

License: [MIT](LICENSE). Version: `0.8.0` (Python package + NuGet metadata). Python and .NET 8 share the same descriptor files.

## Features

Learning path: [`docs/tutorial/index.md`](docs/tutorial/index.md). Task guides: [`docs/guides/index.md`](docs/guides/index.md). Preview the site with `make docs-serve`.

### Subtractive filters

`optional(...)` / null groups are **removed** when params are null, omitted, or (for list params) `[]`. Empty `WHERE`, `PREWHERE`, and `HAVING` clauses are dropped after elision—no bare `HAVING` or leftover `1 = 1`. [Full example →](docs/guides/optional-filters.md)

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

**Header array types** (`integer[]`, …): one `{{param}}` expands to `?, ?, ?` for `IN` lists. Inside `optional(...)`, use `optional(id in ({{$args.id}}))` with `--arg 'id=[1,2]'`. Multi-param `optional(...)` requires all listed params or none—partial args are a compile error. See [optional semantics](docs/concepts/optional-semantics.md).

**Optional groups** repeat an AND-shaped filter per row of a `blob` arg (JSON array of objects), OR-joined; omit/`[]` elides the whole block. [Example →](docs/guides/optional-groups.md)

```bash
yaal explain user/list
yaal explain user/list --arg active=1
yaal explain user/groups --arg 'pairs=[{"id":1}]'
```

### Dynamic ORDER BY

Allowlisted `sort()` / `dir()` splice author expressions only — never client SQL. Null `sort` elides `ORDER BY` unless the header sets a default (`string = id`). Supports multi-column sort (`sort=name,id` + `dir=desc,asc`), `NULLS FIRST/LAST` (`dir=desc_nulls_last`), and mixing with a static tiebreaker column. [Details →](docs/reference/sort-and-dir.md)

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

Every `query` returns nested JSON. [Full example →](docs/tutorial/03-nested-get.md)

```bash
yaal query user/get --arg id=1
# {"id":1,"name":"admin","roles":[{"id":1,"name":"Administrator"},...]}
```

### Output shaping

`mapped`, `partition_by`, `parent_rows`, or child SQL (`$.roles.sql`). [get](docs/tutorial/03-nested-get.md) · [nested](docs/guides/child-sql-and-siblings.md)

```json
{
  "roles": {
    "type": "array",
    "partition_by": "role_id",
    "parent_rows": true,
    "properties": {
      "id": { "mapped": "role_id" },
      "name": { "mapped": "role_name" }
    }
  }
}
```

### Multi-query + data passing

`--sql--` twigs share binds; `$mode=params` copies columns onto `$params` for later twigs. [Full example →](docs/guides/pagination-and-mode-params.md)

```sql
SELECT 'params' AS "$mode", COUNT(*) AS total_count FROM users WHERE active = 1
--sql--
SELECT {{$args.page}} AS page, {{$args.page_size}} AS page_size,
       {{$params.total_count}} AS total_count
```

### API pagination

Sibling `$.paging.sql` + `$.data.sql` → `{ paging, data }` via `$mode=params`. [Full example →](docs/guides/pagination-and-mode-params.md)

```bash
yaal query user/page --arg page=1 --arg page_size=10
# {"paging":{"page":1,"page_size":10,"total_count":2},"data":[...]}
```

### Multi-database

Named providers + `--sql(name)--` twigs in one operation. [Full example →](docs/guides/multi-database.md)

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

CTEs and aggregates stay ordinary SQL. [Full example →](docs/tutorial/08-real-sql.md)

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

Python and .NET 8 share [`tests/fixtures/api/`](tests/fixtures/api/). [Full example →](docs/appendix/python.md)

```python
y.query("user/get", args={"id": 1})
```

```csharp
y.Query("user/get", args: new { id = 1 });
```

### Ahead-of-time compile

`yaal compile` / `precompiled=...`; elision still runs per request. [Full example →](docs/guides/precompile.md)

**Python CLI** — JSON artifacts:

```bash
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  query user/get --arg id=1
```

**C# JSON load** — pass `precompiled:` to the constructor (same artifact layout).

**C# source precompile** — zero-parse startup via generated `Branch` types and `RegisterDescriptor`:

```bash
dotnet run --project csharp/src/Yaal.Cli -- \
  compile --api tests/fixtures/api --format cs --out Generated/YaalDescriptors
```

```csharp
var y = new Yaal.Yaal("tests/fixtures/api");
foreach (var (path, branch) in Yaal.Generated.YaalDescriptorRegistry.All)
    y.RegisterDescriptor(path, branch);
```

Load order when `debug=false`: registered → cache → precompiled JSON → live SQL/JSON. See [precompiled artifacts](docs/reference/precompiled-artifacts.md).

## Install

```bash
make install          # venv + pip install -e .
# or: pip install yaal
pip install 'yaal[postgres]'   # or [mysql] / [clickhouse] — SQLite is stdlib
```

CLI entry point after install: `yaal` (same as `python -m yaal_cli`).

C# / .NET 8: `dotnet add package Yaal` (`0.8.0`), then add the client your app uses:

```bash
dotnet add package Microsoft.Data.Sqlite   # or Npgsql / MySqlConnector / ClickHouse.Client
```

Or add a project reference to [`csharp/src/Yaal/Yaal.csproj`](csharp/src/Yaal/Yaal.csproj).

## Quick start

```bash
make install
make example                          # python/examples/demo.py — all fixtures + explain
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

`make example` runs [`python/examples/demo.py`](python/examples/demo.py): temp SQLite from `docker/sqlite/schema.sql` (+ flags DB), then get / nested / list / page / `report/summary` / `user/combine` plus `explain` elision (read-only). CLI commands with `--db` omitted also seed a temp SQLite DB.

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

```csharp
var y = new Yaal.Yaal("tests/fixtures/api", debug: true);
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");
var user = y.Query("user/get", args: new { id = 1 });
```

Preview compiled SQL (after null-filter elision):

```python
for twig in y.explain_sql("user/get", args={"id": 1}):
    print(twig["sql"], twig["parameters"])
```

## Documentation

Operations are folders of `*.sql` (+ `$.output.json`), discovered filesystem-first and called by path (`y.query("user/get", ...)`). The docs site splits that material into a tutorial, task guides, concept notes, and a reference.

```bash
make docs-install
make docs-serve
```

- [`docs/index.md`](docs/index.md) — site home
- [`docs/tutorial/index.md`](docs/tutorial/index.md) — step-by-step learning path
- [`docs/guides/index.md`](docs/guides/index.md) — task guides (filters, paging, precompile, explain)
- [`docs/concepts/index.md`](docs/concepts/index.md) — elision, optional semantics, shaping
- [`docs/reference/index.md`](docs/reference/index.md) — header, DSL, CLI, URLs
- [`docs/essays/why-sql-first.md`](docs/essays/why-sql-first.md) — why SQL-first fits reporting and ClickHouse-like engines
- [`docs/README.md`](docs/README.md) — short index for readers browsing the repo

## Make targets

| Target | Purpose |
|---|---|
| `make install` | Create `venv` and `pip install -e .` |
| `make test` | Unit tests |
| `make test-integration` | Start Docker Postgres/MySQL/ClickHouse and run integration tests |
| `make test-all` | Unit + integration |
| `make example` | Run `python/examples/demo.py` (all fixture ops + explain) |
| `make example-csharp` | Same tour in .NET SDK container |
| `make yaal ARGS='...'` | Pass-through to CLI (`query` / `explain` / `list` / `compile`) |
| `make experiment` | FS+SQLite sandbox under `experiment/` (init if needed) |
| `make experiment-clickhouse` | Same API sandbox against Compose ClickHouse |
| `make experiment-init` / `experiment-reset` / `experiment-clean` | Create, reseed SQLite, or remove sandbox |
| `make experiment-clickhouse-init` / `experiment-clickhouse-reset` | Start/seed or reseed ClickHouse (keep API edits) |
| `make test-csharp` | .NET unit tests (SDK container) |
| `make test-csharp-integration` | Compose DBs + .NET integration tests |
| `make benchmark-csharp` | Descriptor load benchmarks (live SQL vs JSON vs `RegisterDescriptor`) |
| `make docs-install` / `docs-serve` / `docs-build` | MkDocs site (preview or strict build) |
| `make integration-up` / `integration-down` | Manage compose DBs |

SQLite-only usage does **not** need Docker. Compose is only for Postgres/MySQL/ClickHouse integration tests.

## Tests

```bash
make test                 # python/tests/unit
YAAL_INTEGRATION=1 make test-integration
make test-csharp          # .NET unit tests (sdk container)
```

CI (GitHub Actions) runs Python unit tests and .NET tests on every PR. Shared SQL compile goldens live under [`tests/fixtures/sql_compile/`](tests/fixtures/sql_compile/) (optional filters, `integer[]` expansion, `optional_groups_or` / `optional_groups_and`, `HAVING` cleanup). Descriptor fixtures live under [`tests/fixtures/api/`](tests/fixtures/api/) (including [`user/groups`](tests/fixtures/api/user/groups/) for blob + optional groups).

## Python

Library, tests, and demo live under [`python/`](python/). See [`python/README.md`](python/README.md).

## C# (.NET 8)

A full-parity .NET port lives under [`csharp/`](csharp/). Consume from nuget.org: [`Yaal`](https://www.nuget.org/packages/Yaal) — see [`csharp/README.md`](csharp/README.md) (the package listing). Includes JSON or C# source precompile, `RegisterDescriptor`, and the `yaal` CLI (`compile --format json|cs`). Tests run in a .NET SDK container (no local `dotnet` required):

```bash
make test-csharp
make test-csharp-integration
make example-csharp
make benchmark-csharp
```
