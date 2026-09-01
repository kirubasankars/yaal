# Learn Yaal — step by step

Yaal is a **subtractive SQL ORM**: you write SQL + YAML descriptors; Yaal binds parameters, removes unused `optional(...)` filters, runs queries (optionally across databases), and shapes flat rows into **nested JSON**.

This guide walks the shared fixtures under [`tests/fixtures/api/`](../tests/fixtures/api/). Seed data: [`docker/sqlite/schema.sql`](../docker/sqlite/schema.sql). Examples are **read-only** (no inserts/updates).

| Step | You learn | Fixture / command |
|---|---|---|
| 0 | Install and run something | `make example` |
| 1 | Mental model | pipeline below |
| 2 | One get → nested JSON | `user/get` |
| 3 | Subtractive filters | `user/list` + `explain` |
| 4 | Output shaping | `mapped` / `partition_by` / `parent_rows` |
| 5 | Child SQL nesting | `user/nested` |
| 6 | `$mode` + pagination | `user/page` |
| 7 | Real SQL (`WITH` / agg) | `report/summary` |
| 8 | Multi-database | `user/combine` |
| 9 | Your own API + sandbox | `experiment/` |
| 10 | Precompile + dual runtime | CLI / C# |

Deep dives: [examples.md](examples.md) · [descriptors.md](descriptors.md).

---

## Step 0 — Install and run the tour

```bash
make install
make example                 # Python: all fixtures + explain
make yaal ARGS='list'
make yaal ARGS='query user/get --arg id=1'
```

Optional .NET (Docker SDK, no local `dotnet` required):

```bash
make example-csharp
```

When `--db` is omitted, the CLI seeds a temp SQLite DB from `docker/sqlite/schema.sql` and uses `tests/fixtures/api`.

---

## Step 1 — Mental model

```text
descriptor folder  →  SQL (+ optional child/sibling .sql)  →  bind {{params}}
                   →  subtract optional(...) when null
                   →  execute twigs (maybe named DBs)
                   →  shape with $.output.yaml  →  nested JSON
```

Key ideas:

- **Descriptors are folders**, not HTTP routes. Call path = folder path (`user/get`).
- **SQL is the source of truth.** No query builder; aggregations and `WITH` are normal SQL.
- **Subtractive, not additive:** unused filters are removed, not generated from models.
- **`$` files:** trunk `$.sql` when present; branches like `$.roles.sql` / `$.paging.sql`; shape in `$.output.yaml`.
- Discovery is **filesystem-first** (list `*.sql`), then output YAML shapes each branch. See [descriptors — SQL files and output](descriptors.md#how-sql-files-and-outputyaml-relate).

---

## Step 2 — First query: nested get

**Goal:** one user object with a nested `roles` array from a join.

```bash
yaal query user/get --arg id=1
yaal explain user/get --arg id=1
```

Open:

- [`tests/fixtures/api/user/get/$.sql`](../tests/fixtures/api/user/get/$.sql)
- [`tests/fixtures/api/user/get/$.output.yaml`](../tests/fixtures/api/user/get/$.output.yaml)

Notice:

1. Parameter header `--($args.id integer)--` is the input model (no `$.input.yaml`).
2. Bind with `{{$args.id}}`.
3. Output `partition_by` + `parent_rows` collapses join fan-out into nested `roles`.

Full walkthrough: [examples — Nested get](examples.md#nested-get--userget).

**Try in code:**

```python
from yaal import Yaal

y = Yaal("tests/fixtures/api", debug=True)
y.setup_data_provider("db", "sqlite3:////tmp/app.db")  # seed schema first
print(y.query("user/get", args={"id": 1}))
```

---

## Step 3 — Subtractive filters

**Goal:** see optional predicates disappear when args are null.

```bash
yaal explain user/list
yaal explain user/list --arg active=1
yaal query user/list --arg active=1
```

In [`user/list/$.sql`](../tests/fixtures/api/user/list/$.sql):

```sql
--($args.active integer, $args.sort string = id, $args.dir string = asc)--
...
and optional(u.active = {{$args.active}})
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}})
```

| Call | What happens |
|---|---|
| no `active` | filter clause removed; binds `[]` |
| `active=1` | `and (u.active = ?)` with bind `[1]` |
| no `sort` / `dir` | header defaults → `ORDER BY u.user_id ASC` |
| `sort=name` / `dir=desc` | splices `u.user_name DESC` (allowlisted only) |

That is the core of “subtractive.” Details: [examples — Optional list](examples.md#optional-list--userlist) and [descriptors — Dynamic ORDER BY](descriptors.md#dynamic-order-by--sort--dir).

---

## Step 4 — Output shaping vocabulary

In `$.output.yaml`:

| Key | Meaning |
|---|---|
| `mapped` | SQL column → JSON field |
| `partition_by` | Collapse duplicate parent keys from joins |
| `parent_rows: true` | Nest children from the **parent** result set (no child SQL file) |
| `type: object` / `array` | One object vs list at that branch |

Root `type` sets object vs array. Named nested branches each have their own `type` + `properties`. Bare anonymous `type: object|array` under `properties` is invalid (flat field maps only).

Reference: [descriptors — Output shaping](descriptors.md#output-shaping).

---

## Step 5 — Child SQL instead of `parent_rows`

**Goal:** same JSON as `user/get`, but roles from `$.roles.sql`.

```bash
yaal query user/nested --arg id=1
```

Compare:

| Approach | Files | Nesting |
|---|---|---|
| `user/get` | one join `$.sql` | `parent_rows: true` |
| `user/nested` | `$.sql` + `$.roles.sql` | `partition_by` join key in both result sets |

Parent and child must both return the join key (`user_id`). Walkthrough: [examples — Nested child SQL](examples.md#nested-child-sql--usernested).

---

## Step 6 — `$mode` and pagination

**Goal:** `{ paging, data }` for a list API.

```bash
yaal query user/page --arg page=1 --arg page_size=1
```

Concepts:

1. **Sibling branches** (no trunk `$.sql`): `$.paging.sql` + `$.data.sql` → JSON keys `paging` and `data`.
2. **`$mode=params`:** first twig runs `COUNT(*)`, copies `total_count` onto `$params`; second twig returns page meta using `{{$params.total_count}}`.
3. **Page parents, then join:** `LIMIT`/`OFFSET` on users in a subquery, then join roles so page size is users—not join rows.

Other `$mode` values (`error`, `break`, `json`): [descriptors — `$mode` rows](descriptors.md#mode-rows).  
Pagination walkthrough: [examples — Paginated nest](examples.md#paginated-nest--userpage).

---

## Step 7 — Real SQL: `WITH` and aggregations

**Goal:** CTEs and aggregates stay ordinary SQL.

```bash
yaal query report/summary
```

Open [`report/summary/$.sql`](../tests/fixtures/api/report/summary/$.sql). No query-builder escape hatch—write the SQL you want, map columns in `$.output.yaml`.

Walkthrough: [examples — Real SQL](examples.md#real-sql--reportsummary).

---

## Step 8 — Multi-database

**Goal:** one operation, two named providers.

Demo wiring (see [`python/examples/demo.py`](../python/examples/demo.py)):

```python
y.setup_data_provider("db", "sqlite3:///" + app_db)
y.setup_data_provider("flags", "sqlite3:///" + flags_db)
y.query("user/combine", args={"id": 1})
```

- `$.app.sql` → default connection `"db"`
- `$.flags.sql` starts with `--sql(flags)--` → provider `"flags"`
- Flags seed: [`docker/sqlite/flags_schema.sql`](../docker/sqlite/flags_schema.sql)

Walkthrough: [examples — Multi-database](examples.md#multi-database--usercombine).

---

## Step 9 — Build your own (experiment sandbox)

```bash
make experiment-init
make experiment
make experiment ARGS='query user/page --arg page=1 --arg page_size=10'
# edit experiment/api/... then re-run
make experiment-reset    # reseed SQLite, keep API edits
make experiment-clickhouse   # same API against Compose ClickHouse
make experiment-clean
```

Checklist for a new operation folder:

1. Create `api/<area>/<op>/`
2. Add at least one `*.sql` (`$.sql` and/or siblings / children)
3. Add parameter headers and `{{...}}` binds
4. Add `$.output.yaml` with `mapped` / nesting
5. `yaal query <area>/<op> --api ... --db ...`

More: [examples — Your own API tree](examples.md#your-own-api-tree) · [examples — Experiment sandbox](examples.md#experiment-sandbox).

---

## Step 10 — Precompile and dual runtime

**Precompile** (no DB required for compile; elision still runs per request):

```bash
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
yaal --api tests/fixtures/api --precompiled /tmp/yaal-precompiled \
  query user/get --arg id=1
```

Details: [examples — Precompiled](examples.md#precompiled-descriptors) · [descriptors](descriptors.md#precompiled-descriptors).

**Same descriptors, two languages:**

```python
y.query("user/get", args={"id": 1})
```

```csharp
y.Query("user/get", args: new { id = 1 });
```

```bash
make example
make example-csharp
```

---

## Practice path (suggested order)

1. Run `make example` and read the printed JSON.  
2. Change `user/list` optional filter; re-run `explain` with/without args.  
3. Add a field to `user/get` output (`mapped` a new column).  
4. Copy `user/get` to a new folder; switch roles to child SQL like `user/nested`.  
5. Sketch a paginated list using `$mode=params` like `user/page`.  
6. Point `make experiment` at your edits until the shape looks right.

---

## Where to go next

| Need | Doc |
|---|---|
| Full SQL/YAML/JSON samples | [examples.md](examples.md) |
| Trunk/branch/twig, `$mode`, errors, API | [descriptors.md](descriptors.md) |
| Install / features overview | [README.md](../README.md) |
| .NET port | [csharp/README.md](../csharp/README.md) |
