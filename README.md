# Yaal

Yaal is a SQL→JSON framework. You author operations as SQL + YAML descriptor files; Yaal binds parameters, runs queries, and reshapes flat rows into nested JSON.

## Quick start

```bash
make install
make example                          # demo: user/get id=1
make yaal ARGS='explain user/get --arg id=1'
make yaal ARGS='list'
```

`make example` (and `yaal_cli.py` when `--db` is omitted) builds a temp SQLite DB from `docker/sqlite/schema.sql`, runs the fixture `user/get` with `args={"id": 1}`, and prints nested JSON.

### CLI

```bash
# zero-config demo (temp SQLite + tests/fixtures/api)
python yaal_cli.py query user/get --arg id=1
python yaal_cli.py explain user/get --arg id=1
python yaal_cli.py list

# your own descriptors / database
python yaal_cli.py query orders/list --api ./my-api --db 'sqlite3:////tmp/app.db' --args '{"status":"open"}'
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

## Make targets

| Target | Purpose |
|---|---|
| `make install` | Create `venv` and install dependencies |
| `make test` | Unit tests |
| `make test-integration` | Start Docker Postgres/MySQL/ClickHouse and run integration tests |
| `make test-all` | Unit + integration |
| `make example` | Demo via `yaal_cli.py query user/get --arg id=1` |
| `make yaal ARGS='...'` | Pass-through to `yaal_cli.py` (`query` / `explain` / `list`) |
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
| SQLite (relative) | `sqlite3://./data/app.db` |
| SQLite (memory) | `sqlite3:///` |
| Postgres | `postgresql://user:pass@127.0.0.1:5432/yaal` |
| MySQL | `mysql://user:pass@127.0.0.1:3306/yaal` |
| ClickHouse | `clickhouse://user:pass@127.0.0.1:9000/yaal` |

Register with:

```python
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
```

The default connection name used by SQL twigs is `"db"`.

## Tests

```bash
make test                 # tests/unit
YAAL_INTEGRATION=1 make test-integration
```

Integration fixtures live under [`tests/fixtures/api/`](tests/fixtures/api/).

## C# (.NET 8)

A full-parity .NET port lives under [`csharp/`](csharp/). See [`csharp/README.md`](csharp/README.md). Tests run in a .NET SDK container (no local `dotnet` required):

```bash
make test-csharp
make test-csharp-integration
make example-csharp
```
