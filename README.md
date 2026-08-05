# Yaal

Yaal is a SQL→JSON framework. You author operations as SQL + YAML descriptor files; Yaal binds parameters, runs queries, and reshapes flat rows into nested JSON.

License: [MIT](LICENSE). Version: `0.1.0` (Python package + NuGet metadata).

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
```

`make example` runs [`examples/demo.py`](examples/demo.py): temp SQLite from `docker/sqlite/schema.sql`, then get / list / page / create plus `explain` elision. CLI commands with `--db` omitted also seed a temp SQLite DB.

`make experiment` uses a local sandbox at `experiment/` (gitignored): a copy of `tests/fixtures/api` plus `yaal.db`. Edit `experiment/api/` and re-run; `make experiment-reset` reseeds the DB without wiping API edits.

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

## Docs and examples

| Resource | Purpose |
|---|---|
| [`docs/examples.md`](docs/examples.md) | End-to-end walkthroughs (SQL, YAML, sample JSON, CLI/Python/C#) |
| [`docs/descriptors.md`](docs/descriptors.md) | Trunk/branch/twig reference, shaping, `$action`, errors |
| [`docs/README.md`](docs/README.md) | Docs index |
| [`examples/demo.py`](examples/demo.py) | Runnable Python tour (`make example`) |
| [`csharp/examples/Yaal.Example`](csharp/examples/Yaal.Example/) | Runnable .NET tour (`make example-csharp`) |
| [`tests/fixtures/api/`](tests/fixtures/api/) | Shared descriptors: get, list, page, create |

## Make targets

| Target | Purpose |
|---|---|
| `make install` | Create `venv` and `pip install -e .` |
| `make test` | Unit tests |
| `make test-integration` | Start Docker Postgres/MySQL/ClickHouse and run integration tests |
| `make test-all` | Unit + integration |
| `make example` | Run `examples/demo.py` (all fixture ops + explain) |
| `make example-csharp` | Same tour in .NET SDK container |
| `make yaal ARGS='...'` | Pass-through to CLI (`query` / `explain` / `list`) |
| `make experiment` | FS+SQLite sandbox under `experiment/` (init if needed) |
| `make experiment-init` / `experiment-reset` / `experiment-clean` | Create, reseed DB, or remove sandbox |
| `make integration-up` / `integration-down` | Manage compose DBs |

SQLite-only usage does **not** need Docker. Compose is only for Postgres/MySQL/ClickHouse integration tests.

## Descriptor layout

```text
api/
  user/
    get/                 # operation folder (name is yours; not an HTTP verb)
      $.sql              # trunk query
      $.input.yaml       # input model (args / payload)
      $.output.yaml      # output shape (mapped / partition_by)
```

Call by descriptor path: `y.query("user/get", args={"id": 1})`.

### Parameters

Declare types at the top of the SQL file, then bind with `{{...}}`. Use `$args` for operation keys and the payload root for body fields:

```sql
--($args.id integer)--

select *
from users u
where 1 = 1
  and optional(u.user_id = {{$args.id}})
```

When a parameter is null, Yaal elides optional groups. Long form still works:

```sql
({{param}} is null or col = {{param}})
```

including a preceding `AND`/`OR`. If that group is the only predicate, it becomes `1 = 1`.

Shorter sugar (param once):

```sql
optional(col = {{param}})
```

### Output shaping

```yaml
type: object
partition_by: user_id
properties:
  id:
    mapped: user_id
  name:
    mapped: user_name
  roles:
    type: array
    partition_by: role_id
    parent_rows: true
    properties:
      id:
        mapped: role_id
      name:
        mapped: role_name
```

- `mapped` — column → JSON field  
- `partition_by` — collapse join fan-out into nested objects/arrays  
- `parent_rows: true` — nest from parent rows without a child SQL file  

## Database URLs

| Engine | Example |
|---|---|
| SQLite (absolute) | `sqlite3:////tmp/app.db` |
| SQLite (relative) | `sqlite3://./rel/path.db` |
| SQLite (memory) | `sqlite3:///` |
| Postgres | `postgresql://user:pass@127.0.0.1:5432/yaal` |
| MySQL | `mysql://user:pass@127.0.0.1:3306/yaal` |
| ClickHouse | `clickhouse://user:pass@127.0.0.1:9000/yaal` |

Register with:

```python
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
```

The default connection name used by SQL twigs is `"db"`.

### Input validation dialect

Python validates `$.input.yaml` with **JSON Schema Draft-4** (`jsonschema.Draft4Validator`). C# uses Json.Schema’s default **2020-12** dialect. Keep models to the common subset (`type` / `properties` / `required`) so both ports accept the same fixtures. See [`docs/descriptors.md`](docs/descriptors.md).

Invalid args/payload return `{"errors": [...]}` (soft); missing descriptors and bad URLs raise typed exceptions.

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
