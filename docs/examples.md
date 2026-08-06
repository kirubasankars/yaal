# Examples

Every example below lives under [`tests/fixtures/api/`](../tests/fixtures/api/). App seed: two users (`admin`, `guest`), two roles, join table — [`docker/sqlite/schema.sql`](../docker/sqlite/schema.sql). Multi-DB flags seed: [`docker/sqlite/flags_schema.sql`](../docker/sqlite/flags_schema.sql).

```bash
make example              # full tour (Python)
make example-csharp       # full tour (.NET)
make yaal ARGS='list'
```

## Feature index

| Feature | Section | Fixture |
|---|---|---|
| JSON out / nested shape | [Nested get](#nested-get--userget) | `user/get` |
| Output shaping (child SQL) | [Nested child SQL](#nested-child-sql--usernested) | `user/nested` |
| Subtractive filters | [Optional list](#optional-list--userlist) | `user/list` |
| Dynamic ORDER BY (`sort`/`dir`) | [Optional list](#optional-list--userlist) | `user/list` |
| API pagination / `$mode=params` | [Paginated nest](#paginated-nest--userpage) | `user/page` |
| Multi-query + data passing | [Paginated nest](#paginated-nest--userpage) | `user/page` (`--sql--` + `$params`) |
| `$mode` (`params` / `error` / `break` / `json`) | [descriptors — `$mode` rows](descriptors.md#mode-rows) | `user/page` (+ unit tests) |
| Real SQL (`WITH` / agg) | [Report summary](#real-sql--reportsummary) | `report/summary` |
| Multi-database | [Multi-database](#multi-database--usercombine) | `user/combine` |
| Ahead-of-time compile | [Precompiled descriptors](#precompiled-descriptors) | *(any)* |
| Dual runtime | [Dual runtime](#dual-runtime-python--c) | shared fixtures |

Examples are **read-only** (SELECT / shape). They do not insert or update seed data.

---

## Nested get — `user/get`

Join rows become one user object with a nested `roles` array.

### Descriptor

```text
user/get/
  $.sql
  $.output.yaml
```

**`$.sql`**

```sql
--($args.id integer)--

select
    u.user_id,
    u.user_name,
    r.role_id,
    r.role_name
from users u
inner join user_roles ur on ur.user_id = u.user_id
inner join roles r on r.role_id = ur.role_id
where u.active = 1
  and r.active = 1
  and optional(u.user_id = {{$args.id}})
order by u.user_id, r.role_id
```

**`$.output.yaml`**

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

### Commands

```bash
yaal query user/get --arg id=1
yaal explain user/get --arg id=1
yaal explain user/get          # optional(...) removed; binds []
```

```python
y.query("user/get", args={"id": 1})
```

```csharp
y.Query("user/get", args: new { id = 1 });
```

### Sample JSON

```json
{
  "id": 1,
  "name": "admin",
  "roles": [
    { "id": 1, "name": "Administrator" },
    { "id": 2, "name": "User" }
  ]
}
```

How shaping works on the flat result set:

| user_id | user_name | role_id | role_name |
|---:|---|---:|---|
| 1 | admin | 1 | Administrator |
| 1 | admin | 2 | User |

`partition_by: user_id` → one object. `roles` + `parent_rows: true` nests from those rows (no child SQL file).

---

## Nested child SQL — `user/nested`

Same JSON shape as `user/get`, but roles come from a **child SQL file** (`$.roles.sql`) instead of a join + `parent_rows`. Files are discovered on disk; the `roles` property in `$.output.yaml` supplies that branch’s output schema (not the other way around). See [descriptors](descriptors.md#how-sql-files-and-outputyaml-relate).

### Descriptor

```text
user/nested/
  $.sql
  $.roles.sql             # → branch $.roles → JSON property "roles"
  $.output.yaml
```

**`$.sql`** — parent users only:

```sql
--($args.id integer)--

select
    u.user_id,
    u.user_name
from users u
where u.active = 1
  and optional(u.user_id = {{$args.id}})
order by u.user_id
```

**`$.roles.sql`** — child query; include the parent join key (`user_id`):

```sql
--($args.id integer)--

select
    ur.user_id,
    r.role_id,
    r.role_name
from user_roles ur
inner join roles r on r.role_id = ur.role_id
where r.active = 1
  and optional(ur.user_id = {{$args.id}})
order by ur.user_id, r.role_id
```

**`$.output.yaml`** — no `parent_rows`; the `roles` property matches the file suffix:

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
    properties:
      id:
        mapped: role_id
      name:
        mapped: role_name
```

Parent `partition_by: user_id` groups each user’s role rows from `$.roles.sql` onto that user.

### Commands

```bash
yaal query user/nested --arg id=1
```

```python
y.query("user/nested", args={"id": 1})
```

```csharp
y.Query("user/nested", args: new { id = 1 });
```

### Sample JSON

```json
{
  "id": 1,
  "name": "admin",
  "roles": [
    { "id": 1, "name": "Administrator" },
    { "id": 2, "name": "User" }
  ]
}
```

| Approach | Files | Nesting |
|---|---|---|
| `user/get` | one join `$.sql` | `parent_rows: true` |
| `user/nested` | `$.sql` + `$.roles.sql` | `partition_by` join key in both result sets |

---

## Optional list — `user/list`

Root `type: array`. Omit `active` to return everyone; pass it to filter. Optional `sort` / `dir` use allowlisted `sort()` / `dir()` sugar (see [descriptors](descriptors.md#dynamic-order-by--sortdir)).

### Descriptor

**`$.sql`**

```sql
--($args.active integer, $args.sort string = id, $args.dir string = asc)--

select
    u.user_id,
    u.user_name,
    u.active
from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}}),
  u.user_id asc
```

The trailing `u.user_id asc` is a static tiebreaker — it stays even when `sort`/`dir` are omitted (only the dynamic term elides). `sort`/`dir` also each accept a comma-separated list for multi-column sort (`sort=name,id` + `dir=desc,asc`), and `dir` accepts `*_nulls_first`/`*_nulls_last` suffixes.

**`$.output.yaml`**

```yaml
type: array
partition_by: user_id
properties:
  id:
    mapped: user_id
  name:
    mapped: user_name
  active:
    mapped: active
```

### Commands

```bash
yaal query user/list
yaal query user/list --arg active=1
yaal query user/list --arg sort=name --arg dir=desc
yaal query user/list --arg sort=name,id --arg dir=desc,asc
yaal explain user/list
yaal explain user/list --arg active=1 --arg sort=id
```

### Explain (elision + defaults)

**active omitted** — filter removed; header defaults keep the dynamic term (`u.user_id ASC`) plus the static tiebreaker:

```sql
select
    u.user_id,
    u.user_name,
    u.active
from users u
order by
  u.user_id ASC, u.user_id asc
-- binds: []
```

**active=1, sort=name, dir=desc**

```sql
select
    u.user_id,
    u.user_name,
    u.active
from users u
where (u.active = ?)
order by
  u.user_name DESC, u.user_id asc
-- binds: [1]
```

**sort=name,id, dir=desc,asc** (multi-column):

```sql
order by
  u.user_name DESC, u.user_id ASC, u.user_id asc
```

### Sample JSON (`active=1`, `sort=name`)

```json
[
  { "id": 1, "name": "admin", "active": 1 },
  { "id": 2, "name": "guest", "active": 1 }
]
```

---

## Paginated nest — `user/page`

No trunk `$.sql` (allowed: at least one `*.sql` is enough). Sibling files `$.paging.sql` and `$.data.sql` become branches `paging` and `data`; `$.output.yaml` shapes those properties (and nested `data.roles` via `parent_rows`).

### Layout

```text
user/page/
  $.paging.sql            # sibling branch (no $.sql)
  $.data.sql
  $.output.yaml
```

**`$.paging.sql`** — `$mode=params` stores `total_count` on `$params`, then returns paging fields (see [descriptors — `$mode`](descriptors.md#mode-rows)):

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

**`$.data.sql`** — page **users** in a subquery, then join roles (so `LIMIT` does not cut role rows):

```sql
--($args.page integer, $args.page_size integer)--

SELECT
    u.user_id,
    u.user_name,
    r.role_id,
    r.role_name
FROM (
    SELECT user_id, user_name
    FROM users
    WHERE active = 1
    ORDER BY user_id
    LIMIT {{$args.page_size}} OFFSET ({{$args.page}} - 1) * {{$args.page_size}}
) u
INNER JOIN user_roles ur ON ur.user_id = u.user_id
INNER JOIN roles r ON r.role_id = ur.role_id
ORDER BY u.user_id, r.role_id
```

**`$.output.yaml`**

```yaml
type: object
properties:
  paging:
    type: object
    properties:
      page:
        mapped: page
      page_size:
        mapped: page_size
      total_count:
        mapped: total_count
  data:
    type: array
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

### Commands

```bash
yaal query user/page --arg page=1 --arg page_size=10
yaal query user/page --arg page=1 --arg page_size=1
yaal query user/page --arg page=2 --arg page_size=10
```

```python
y.query("user/page", args={"page": 1, "page_size": 1})
```

```csharp
y.Query("user/page", args: new { page = 1, page_size = 1 });
```

### Sample JSON (`page=1`, `page_size=1`)

```json
{
  "paging": {
    "page": 1,
    "page_size": 1,
    "total_count": 2
  },
  "data": [
    {
      "id": 1,
      "name": "admin",
      "roles": [
        { "id": 1, "name": "Administrator" },
        { "id": 2, "name": "User" }
      ]
    }
  ]
}
```

---

## Real SQL — `report/summary`

Aggregations and `WITH` / CTEs are ordinary SQL in the descriptor—no query-builder escape hatch.

### Descriptor

```text
report/summary/
  $.sql
  $.output.yaml
```

**`$.sql`**

```sql
WITH role_counts AS (
    SELECT
        ur.user_id,
        COUNT(*) AS role_count
    FROM user_roles ur
    GROUP BY ur.user_id
)
SELECT
    COUNT(*) AS user_count,
    SUM(CASE WHEN u.active = 1 THEN 1 ELSE 0 END) AS active_count,
    COALESCE(SUM(rc.role_count), 0) AS assignment_count
FROM users u
LEFT JOIN role_counts rc ON rc.user_id = u.user_id
```

**`$.output.yaml`**

```yaml
type: object
properties:
  user_count:
    mapped: user_count
  active_count:
    mapped: active_count
  assignment_count:
    mapped: assignment_count
```

### Commands

```bash
yaal query report/summary
```

```python
y.query("report/summary")
```

```csharp
y.Query("report/summary");
```

### Sample JSON

```json
{
  "user_count": 2,
  "active_count": 2,
  "assignment_count": 3
}
```

---

## Multi-database — `user/combine`

One operation, two named providers. Sibling branches: `app` reads the default `"db"` connection; `flags` switches with `--sql(flags)--`.

### Layout

```text
user/combine/
  $.app.sql
  $.flags.sql
  $.output.yaml
```

**`$.app.sql`** (connection `"db"`)

```sql
--($args.id! integer)--

SELECT
    u.user_id,
    u.user_name
FROM users u
WHERE u.user_id = {{$args.id}}
```

**`$.flags.sql`** (connection `"flags"`)

```sql
--($args.id! integer)--

--sql(flags)--

SELECT
    f.user_id,
    f.vip
FROM external_flags f
WHERE f.user_id = {{$args.id}}
```

**`$.output.yaml`**

```yaml
type: object
properties:
  app:
    type: object
    properties:
      id:
        mapped: user_id
      name:
        mapped: user_name
  flags:
    type: object
    properties:
      user_id:
        mapped: user_id
      vip:
        mapped: vip
```

### Setup

Seed app DB from [`schema.sql`](../docker/sqlite/schema.sql) and flags DB from [`flags_schema.sql`](../docker/sqlite/flags_schema.sql):

```python
y.setup_data_provider("db", "sqlite3:///" + app_db)
y.setup_data_provider("flags", "sqlite3:///" + flags_db)
y.query("user/combine", args={"id": 1})
```

```csharp
y.SetupDataProvider("db", "sqlite3:///" + appDb);
y.SetupDataProvider("flags", "sqlite3:///" + flagsDb);
y.Query("user/combine", args: new { id = 1 });
```

### Sample JSON (`id=1`)

```json
{
  "app": { "id": 1, "name": "admin" },
  "flags": { "user_id": 1, "vip": 1 }
}
```

---

## Precompiled descriptors

Compile SQL/YAML ahead of time (no database required). Elision still runs per request.

```bash
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled query user/get --arg id=1
```

```python
from yaal import Yaal

y = Yaal("tests/fixtures/api", precompiled="/tmp/yaal-precompiled")
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
y.query("user/get", args={"id": 1})
```

```csharp
var y = new Yaal.Yaal("tests/fixtures/api", precompiled: "/tmp/yaal-precompiled");
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");
y.Query("user/get", args: new { id = 1 });
```

`debug=True` forces live SQL/YAML and ignores `precompiled`. Details: [descriptors.md](descriptors.md#precompiled-descriptors).

---

## Dual runtime (Python / C#)

Same descriptors, same JSON. Pick either runtime:

```python
from yaal import Yaal

y = Yaal("tests/fixtures/api", debug=True)
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
print(y.query("user/get", args={"id": 1}))
```

```csharp
var y = new Yaal.Yaal("tests/fixtures/api", debug: true);
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");
Console.WriteLine(y.QueryJson("user/get", args: new { id = 1 }));
```

```bash
make example            # Python tour
make example-csharp     # .NET tour (Docker SDK)
```

---

## Experiment sandbox

Persistent local copy of the fixtures for editing:

```bash
make experiment-init
make experiment ARGS='query user/page --arg page=1 --arg page_size=10'
# edit experiment/api/... then re-run
make experiment-reset
make experiment-clean

# same experiment/api against Docker Compose ClickHouse
make experiment-clickhouse-init
make experiment-clickhouse
make experiment-clickhouse ARGS='query user/list --arg sort=name'
make experiment-clickhouse-reset
```

---

## Your own API tree

```text
my-api/
  orders/
    list/
      $.sql
      $.output.yaml
```

```bash
yaal query orders/list \
  --api ./my-api \
  --db 'sqlite3:////tmp/app.db' \
  --args '{"status":"open"}'
```

```python
from yaal import Yaal

y = Yaal("./my-api", debug=True)
y.setup_data_provider("db", "postgresql://user:pass@127.0.0.1:5432/app")
y.query("orders/list", args={"status": "open"})
```

```csharp
var y = new Yaal.Yaal("./my-api", debug: true);
y.SetupDataProvider("db", "postgresql://user:pass@127.0.0.1:5432/app");
y.Query("orders/list", args: new { status = "open" });
```

---

## Full demo scripts

| Runtime | Entry |
|---|---|
| Python | [`examples/demo.py`](../examples/demo.py) · `make example` |
| C# | [`csharp/examples/Yaal.Example`](../csharp/examples/Yaal.Example/) · `make example-csharp` |

Both print get / nested / list / page / `report/summary` / `user/combine` and show `explain` elision for `user/list`.

See also: [descriptors.md](descriptors.md) · [README.md](README.md)
