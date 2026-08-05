# Descriptor reference

Operations are folders of SQL + YAML. You call them by path (`user/get`); Yaal binds parameters, runs queries, and reshapes flat rows into nested JSON.

Worked fixtures with sample JSON: [examples.md](examples.md).

```mermaid
flowchart TD
  op["api/user/get/"] --> trunk["trunk $.sql"]
  op --> input["$.input.yaml"]
  op --> output["$.output.yaml"]
  op --> branches["$.paging.sql / $.data.sql"]
  trunk --> twigs["--sql-- twigs"]
  twigs --> exec["providers execute + bind"]
  exec --> shape["partition_by / mapped / parent_rows"]
  shape --> json["nested JSON"]
```

## Trunk, branch, twig

| Concept | Meaning |
|---|---|
| **Operation** | Folder under the API root, e.g. `user/get/` |
| **Trunk** | Root SQL `$` → `$.sql`, or a trunk that only has child SQL files |
| **Branch** | Nested SQL `$.paging.sql`, `$.data.sql` → methods `$.paging`, `$.data` |
| **Twig** | One statement inside a SQL file, split by `--sql--` or `--sql(connection)--` |

```text
api/user/get/
  $.sql
  $.input.yaml
  $.output.yaml
  $.output.summary.yaml     # optional alternate shape (output_mapper="summary")

api/user/page/
  $.paging.sql
  $.data.sql
  $.input.yaml
  $.output.yaml
```

Call path = folder path: `y.query("user/page", args={"page": 1, "page_size": 10})`.

## Parameters

Declare types at the top of a SQL file (first significant token; leading blank lines/spaces are fine); bind with `{{...}}`:

```sql
--($args.id integer, name string)--

select *
from users u
where optional(u.user_id = {{$args.id}})
  and u.user_name = {{name}}
```

Allowed types: `integer`, `string`, `float`, `bool`, `blob`. Each name needs a type; duplicates and unknown types are errors.

Plain SQL `--` line comments are allowed in query text. Yaal directives are only `--(name type, ...)--` and `--sql--` / `--sql(connection)--`.

| Prefix | Source |
|---|---|
| `$args.*` | `query(..., args=...)` |
| bare name | `query(..., payload=...)` |
| `$params.*` | Run bag: `$run_id`, `$last_inserted_id`, `$action=params` values |
| `$parent.*` | Parent branch payload |

### Optional filters

```sql
and optional(u.user_id = {{$args.id}})
```

| Call | Result |
|---|---|
| value present | `and (u.user_id = ?)` with a bind |
| value null / omitted | clause removed |

If the elided filter was the only predicate, the empty `WHERE` / ClickHouse `PREWHERE` is dropped — including before `)` or other clause starts (no leftover bare clause or `1 = 1`). Leading author `WHERE`/`PREWHERE 1 = 1 AND|OR …` is cleaned the same way when the rest remains. When both clauses appear, each is cleaned independently.

Long form still works and must be parenthesized: `({{param}} is null or col = {{param}})` (case and surrounding whitespace are flexible).

```bash
yaal explain user/list --arg active=1
yaal explain user/list
```

## Output shaping

Root `type` is `object` (one result) or `array` (list). Fields are a flat map under `properties`. Named nested branches use their own `type` + `properties`.

```yaml
type: array
partition_by: id
properties:
  id:
    mapped: id
  details:
    type: object
    parent_rows: true
    properties:
      name:
        mapped: name
```

```yaml
type: object
partition_by: user_id
properties:
  id:
    mapped: user_id
  roles:
    type: array
    partition_by: role_id
    parent_rows: true
    properties:
      id:
        mapped: role_id
```

Invalid: bare `type: object` / `type: array` under `properties` (including a nested item wrapper). Root `type` already sets array/object. A JSON field named `type` uses `type: { mapped: col }`.

| Key | Role |
|---|---|
| `mapped` | SQL column → JSON field |
| `partition_by` | Collapse join fan-out |
| `parent_rows` | Nest from parent rows (parent must set `partition_by`) |
| root / branch `type` | `object` → one object; `array` → list |

## Multi-twig writes

```sql
--(id integer, name string)--

INSERT INTO users (user_id, user_name, active) VALUES ({{id}}, {{name}}, 1)

--sql--

INSERT INTO user_roles (user_id, role_id) VALUES ({{id}}, 2)

--sql--

SELECT ... WHERE u.user_id = {{id}}
```

After each twig, providers set `$params.$last_inserted_id` (engine-specific). Prefer an explicit payload/args id when you need a stable key across twigs.

Named connection: `--sql(other)--` uses provider `"other"` (default `"db"`).

Fixture: [`user/create`](../tests/fixtures/api/user/create/).

## Multi-file branches

Files `$.paging.sql` + `$.data.sql` become output properties `paging` and `data`. Branch names in `$.output.yaml` must match file suffixes.

When using `LIMIT`/`OFFSET` with join fan-out + `parent_rows`, page the parent entity in a subquery first — otherwise the limit truncates join rows and nests incomplete children. See [`user/page`](../tests/fixtures/api/user/page/).

## `$action` rows

If a result row includes `$action`, Yaal treats it specially:

| `$action` | Effect |
|---|---|
| `params` | Copy columns onto `$params`; continue |
| `error` | Stop; return `{"errors": [...]}` |
| `break` | Return these rows (minus `$action`) as the branch result |
| `json` | Parse/return the `json` column as the branch result |

```sql
SELECT 'params' AS "$action", COUNT(*) AS total_count FROM users
```

```sql
SELECT
    'error' AS "$action",
    1 AS code,
    'page out of range' AS message
WHERE {{$args.page}} < 1 OR {{$args.page}} > {{$params.total_pages}}
```

## `output_mapper`

Alternate shapes use `$.output.<name>.yaml`:

```python
y.query("user/get", args={"id": 1}, output_mapper="summary")
# loads $.output.summary.yaml instead of $.output.yaml
```

There is no process-wide or cross-query result cache. `clear_cache()` only clears cached descriptors (reload SQL/YAML).

## Performance notes

- Providers drain cursors with `fetchmany` into a per-branch row list; nesting (`partition_by`) still buffers that branch in memory.
- Compiled SQL (after optional-filter elision) is cached per twig + null-set + placeholder for the duration of a trunk execution.
- Postgres / MySQL URL query knobs: `pool_size` (and Postgres `minconn` / `maxconn`). Defaults: Postgres max 20, MySQL 10.
- C# uses driver connection pooling (Npgsql / MySqlConnector); pass `pooling`, `pool_size` / `maximum pool size` in the URL query string.

## Errors

**Raised** (config / I/O):

| Type | When |
|---|---|
| `DescriptorNotFoundError` | No SQL / cannot build |
| `UnsupportedDatabaseUrlError` | Bad URL scheme |
| `PathEscapeError` | Path escapes API root |
| `YaalError` | Base class |

**Soft** (not raised): invalid args/payload → `{"errors": [{"message": "..."}]}`. Also used for `$action=error`. Check for an `errors` key.

## Dual-port JSON Schema subset

Python: Draft-4. C#: Json.Schema 2020-12. Keep models on the common subset:

- `type`, `properties`, `required`
- Scalars: `string`, `integer`, `number`, `boolean`, `object`, `array`

Avoid dialect-only keywords if both ports must accept the same fixtures.

## Public API

### Python

```python
from yaal import Yaal

y = Yaal("path/to/api", debug=True)
y.setup_data_provider("db", "sqlite3:////tmp/app.db")

y.query("user/get", args={"id": 1})
y.query_json("user/get", args={"id": 1})
y.explain_sql("user/get", args={"id": 1})
y.clear_cache()
```

`debug=True` disables descriptor file caching (reload each call). Not a log level.

### C#

```csharp
var y = new Yaal.Yaal("path/to/api", debug: true);
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");

y.Query("user/get", args: new { id = 1 });
y.QueryJson("user/get", args: new { id = 1 });
y.ExplainSql("user/get", args: new { id = 1 });
```

### Database URLs

| Engine | Example |
|---|---|
| SQLite (absolute) | `sqlite3:////tmp/app.db` |
| SQLite (relative) | `sqlite3://./data/app.db` |
| SQLite (memory) | `sqlite3:///` |
| Postgres | `postgresql://user:pass@127.0.0.1:5432/yaal?pool_size=20` |
| MySQL | `mysql://user:pass@127.0.0.1:3306/yaal?pool_size=10` |
| ClickHouse | `clickhouse://user:pass@127.0.0.1:9000/yaal` |
