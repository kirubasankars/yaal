# Descriptor reference

Operations are folders of SQL + YAML. You call them by path (`user/get`); Yaal binds parameters, runs queries, and reshapes flat rows into nested JSON.

Worked fixtures with sample JSON: [examples.md](examples.md).

```mermaid
flowchart TD
  op["operation folder"] --> sqlFiles["list *.sql on disk"]
  op --> output["$.output.yaml"]
  sqlFiles --> tree["branch tree under dollar"]
  output --> shapeSlots["object/array property slots"]
  tree --> load["load each method.sql if present"]
  shapeSlots --> load
  load --> exec["providers execute + bind"]
  exec --> shape["mapped / partition_by / parent_rows"]
  shape --> json["nested JSON"]
```

## Trunk, branch, twig

| Concept | Meaning |
|---|---|
| **Operation** | Folder under the API root, e.g. `user/get/` |
| **Trunk** | Root method `$` — file `$.sql` when present; may be empty if only sibling/child SQL files exist |
| **Branch** | Nested method under `$`, e.g. `$.paging` / `$.roles` → files `$.paging.sql` / `$.roles.sql` |
| **Twig** | One statement inside a SQL file, split by `--sql--` or `--sql(connection)--` |

### How SQL files and `$.output.yaml` relate

Discovery is **filesystem-first**, then shaped by output YAML:

1. **List** every `*.sql` in the operation folder. At least one is required or the descriptor is not found.
2. **Strip** `.sql` → names like `$`, `$.paging`, `$.roles` (deeper dots allowed: `$.data.items`).
3. **Build** a branch tree under `$`. `$.sql` is the trunk file when present; it is **not** required — sibling-only ops use only `$.paging.sql`, `$.data.sql`, etc.
4. **Load** `$.output.yaml` (or `$.output.<mapper>.yaml`). For each branch, the matching nested object/array property is that branch’s output model (`mapped`, `partition_by`, `parent_rows`, …).
5. **Object/array properties** in output also open child branch slots (needed for `parent_rows` with no child SQL file). File-derived children that are missing from that map are merged in.
6. Nested child SQL is looked up as `$.{property}.sql` for property `property`. Output does **not** invent SQL filenames; it shapes whatever SQL (or parent rows) that branch has.

| Pattern | Files | Output role |
|---|---|---|
| Trunk + shape | `$.sql` + `$.output.yaml` | Root `type` / `properties` shape the trunk result |
| Nested child SQL | `$.sql` + `$.roles.sql` + `$.output.yaml` | Property `roles` must match the file suffix; its schema shapes the child |
| `parent_rows` only | `$.sql` + `$.output.yaml` (no `$.roles.sql`) | Property `roles` with `parent_rows: true` nests from parent rows |
| Sibling branches | `$.paging.sql` + `$.data.sql` + `$.output.yaml` (no `$.sql`) | Properties `paging` / `data` match suffixes |

```text
api/user/get/
  $.sql
  $.output.yaml
  $.output.summary.yaml     # optional alternate shape (output_mapper="summary")

api/user/nested/
  $.sql                     # parent rows
  $.roles.sql               # child SQL → output property "roles"
  $.output.yaml

api/user/page/
  $.paging.sql              # sibling branch (no trunk $.sql)
  $.data.sql
  $.output.yaml
```

Call path = folder path: `y.query("user/page", args={"page": 1, "page_size": 10})`.

## Parameters

The SQL parameter header is the **sole input model**. Declare types at the top of a SQL file (first significant token; leading blank lines/spaces are fine); bind with `{{...}}`. Yaal derives args/payload JSON Schema from these headers (union across files in the operation).

```sql
--($args.id integer, name! string)--

select *
from users u
where optional(u.user_id = {{$args.id}})
  and u.user_name = {{name}}
```

Allowed types: `integer`, `string`, `float`, `bool`, `blob`. Each name needs a type; duplicates and unknown types are errors. Trailing `!` on a name marks it **required** (`--($args.id! integer)--`). Optional `= <literal>` sets a JSON Schema **default** used when the caller omits the value:

```sql
--($args.sort string = id, $args.dir string = asc, $args.page integer = 1)--
```

| Type | Literal |
|---|---|
| `integer` | `-?\d+` |
| `float` | `-?\d+` or `-?\d+.\d+` |
| `bool` | `true` / `false` |
| `string` | `'...'` or bare `\w+` (e.g. `= id`) |
| `blob` | not allowed |

`!` and `=` cannot be combined. Conflicting type/required/default declarations across files raise at descriptor build time. A defaulted param is **not** null: `optional(...)` keeps the filter, and `sort()`/`dir()` resolve using the default (they do not elide).

Plain SQL `--` line comments are allowed in query text. Yaal directives are only `--(name type, ...)--` and `--sql--` / `--sql(connection)--`.

| Prefix | Source |
|---|---|
| `$args.*` | `query(..., args=...)` |
| bare name | `query(..., payload=...)` |
| `$params.*` | Run bag: `$run_id`, `$last_inserted_id`, `$mode=params` values |
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

### Dynamic ORDER BY — `sort()` / `dir()`

Identifiers cannot be bound as values. Use allowlisted sugar so only author-written expressions are spliced:

```sql
--($args.sort string, $args.dir string)--

order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}})
```

| Construct | Behavior |
|---|---|
| `sort({{param}}, key = expr, …)` | Resolve `param` to a declared key (case-insensitive). Splice the matching **author** `expr`. Unknown non-null key → soft `{"errors":[…]}` (no execute). |
| `dir({{param}})` | Allow only `asc` / `desc` (case-insensitive). Null/omitted → `ASC`. Unknown → soft error. |
| Null / omitted `sort` (no header default) | **Elide** the entire `ORDER BY` (dir is ignored). |
| Header default on `sort` / `dir` | Omitted args use that default (e.g. `string = id`) instead of eliding. |

**v1 rules:** the `ORDER BY` clause must be exactly `sort(...)` and optional `dir(...)` — no extra comma terms. Keys must match `\w+`. `sort`/`dir` outside `ORDER BY` parse but are the author's responsibility. Empty-string / whitespace-only sort keys are soft errors (not treated as null).

Security: only expressions written in the descriptor are ever spliced; client keys never become SQL.

```bash
yaal explain user/list --arg sort=name --arg dir=desc
yaal query user/list --arg sort=id --arg dir=asc
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

## Multi-twig queries

Split one SQL file into ordered twigs with `--sql--`. Args/payload binds are shared; cross-twig values flow through `$params`.

```sql
--($args.page integer, $args.page_size integer, $params.total_count integer)--

SELECT
    'params' AS "$mode",
    COUNT(*) AS total_count
FROM users
WHERE active = 1

--sql--

SELECT
    {{$args.page}} AS page,
    {{$args.page_size}} AS page_size,
    {{$params.total_count}} AS total_count
```

`$mode=params` copies columns onto `$params` for later twigs. After each twig, providers also set `$params.$last_inserted_id` (engine-specific; useful when a twig writes). Prefer an explicit args/payload id when you need a stable key.

Named connection: `--sql(other)--` uses provider `"other"` (default `"db"`).

Readonly fixture: [`user/page`](../tests/fixtures/api/user/page/) (`$.paging.sql`).

## Multi-file branches

Branch map seed = **SQL files on disk** + **object/array properties in `$.output.yaml`** (see [How SQL files and `$.output.yaml` relate](#how-sql-files-and-outputyaml-relate)).

- File `$.{name}.sql` → branch method `$.{name}` → JSON property `name` (must appear under `properties` in the parent output model when you want it shaped).
- Output property with `type: object|array` and no matching SQL file → branch slot for `parent_rows` (or an empty child until a file is added).

**Sibling branches** (no trunk `$.sql`): `$.paging.sql` + `$.data.sql` → properties `paging` and `data`. See [`user/page`](../tests/fixtures/api/user/page/).

**Nested child SQL** under a trunk: `$.sql` + `$.roles.sql` → child property `roles`. Parent and child both return the join key; `partition_by` on the parent stitches child rows onto matching parents (no `parent_rows`). See [`user/nested`](../tests/fixtures/api/user/nested/).

When using `LIMIT`/`OFFSET` with join fan-out + `parent_rows`, page the parent entity in a subquery first — otherwise the limit truncates join rows and nests incomplete children.

## `$mode` rows

`$mode` is an optional **result-column control key**. When the first row of a twig includes `$mode`, Yaal does not treat that result as ordinary data for output shaping. It reads the mode value and steers the twig/branch:

| `$mode` | Effect |
|---|---|
| `params` | Copy the row’s columns onto `$params` for later twigs; continue the twig list |
| `error` | Stop; return soft `{"errors": [...]}` (same shape as invalid args/payload) |
| `break` | Return these rows as the branch result immediately (column `$mode` stripped) |
| `json` | Treat the `json` column as the branch result (string → parse; otherwise pass through) |

Ordinary SELECT twigs omit `$mode` entirely — rows go through normal `$.output.yaml` shaping.

Use `$mode` for **in-SQL orchestration** across multi-twig files (stash values, soft business errors, early exit, engine JSON) without a second orchestration language in the host.

### `params` — stash for later twigs

Copies every column from the mode row onto `$params` (including `$mode` itself). Later twigs bind with `{{$params.*}}`. Fixture: [`user/page`](../tests/fixtures/api/user/page/) · [examples](examples.md#paginated-nest--userpage).

```sql
--($args.page integer, $args.page_size integer, $params.total_count integer)--

SELECT
    'params' AS "$mode",
    COUNT(*) AS total_count
FROM users
WHERE active = 1

--sql--

SELECT
    {{$args.page}} AS page,
    {{$args.page_size}} AS page_size,
    {{$params.total_count}} AS total_count
```

### `error` — soft business errors from SQL

Stops the branch and returns `{"errors": [ ...rows... ]}`. Not raised as an exception. Useful for checks that belong next to the SQL (range validation, precondition failures).

```sql
SELECT
    'error' AS "$mode",
    1 AS code,
    'page out of range' AS message
WHERE {{$args.page}} < 1 OR {{$args.page}} > {{$params.total_pages}}
```

### `break` — early branch result

Returns the twig’s rows as the branch result and skips remaining twigs / normal shaping for that branch. `$mode` is removed from each row before return.

```sql
SELECT
    'break' AS "$mode",
    u.user_id AS id,
    u.user_name AS name
FROM users u
WHERE u.user_id = {{$args.id}}
```

### `json` — engine-produced JSON

Uses the `json` column as the branch result. If the value is a string, it is parsed as JSON; otherwise it is passed through. Handy when the database already builds JSON (e.g. `json_group_array` / `jsonb_agg`). Bypasses `$.output.yaml` shaping for that branch.

```sql
SELECT
    'json' AS "$mode",
    json_group_array(json_object('id', user_id, 'name', user_name)) AS json
FROM users
WHERE active = 1
```

## `output_mapper`

Alternate shapes use `$.output.<name>.yaml`:

```python
y.query("user/get", args={"id": 1}, output_mapper="summary")
# loads $.output.summary.yaml instead of $.output.yaml
```

There is no process-wide or cross-query result cache. `clear_cache()` only clears cached descriptors (reload SQL/YAML).

## Precompiled descriptors

Compile SQL/YAML once to JSON (token twigs preserved), then load at runtime without re-lexing sources:

```bash
yaal --api path/to/api compile --out path/to/precompiled
```

```python
y = Yaal("path/to/api", precompiled="path/to/precompiled")
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
y.query("user/get", args={"id": 1})
```

`debug=True` forces live SQL/YAML and ignores `precompiled`. Artifacts are one JSON file per path (`user/get.json`; alternate mappers as `user/get#summary.json`). Optional-filter SQL elision still runs per request.

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

**Soft** (not raised): invalid args/payload → `{"errors": [{"message": "..."}]}`. Also used for `$mode=error`. Check for an `errors` key.

## Input validation

Args/payload schemas are derived from SQL headers (`float`→`number`, `bool`→`boolean`). Soft validation uses JSON Schema Draft-4 (Python) / 2020-12 (C#) on that derived model. Invalid args/payload return `{"errors": [...]}`.

## Public API

### Python

```python
from yaal import Yaal

y = Yaal("path/to/api", debug=True)  # or precompiled="path/to/precompiled"
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
