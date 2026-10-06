# Descriptor reference

Operations are folders of SQL + JSON. You call them by path (`user/get`); Yaal binds parameters, runs queries, and reshapes flat rows into nested JSON.

Worked fixtures with sample JSON: [examples.md](examples.md).

```mermaid
flowchart TD
  op["operation folder"] --> sqlFiles["list *.sql on disk"]
  op --> output["$.output.json"]
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

### How SQL files and `$.output.json` relate

Discovery is **filesystem-first**, then shaped by output JSON:

1. **List** every `*.sql` in the operation folder. At least one is required or the descriptor is not found.
2. **Strip** `.sql` → names like `$`, `$.paging`, `$.roles` (deeper dots allowed: `$.data.items`).
3. **Build** a branch tree under `$`. `$.sql` is the trunk file when present; it is **not** required — sibling-only ops use only `$.paging.sql`, `$.data.sql`, etc.
4. **Load** `$.output.json` (or `$.output.<mapper>.json`). For each branch, the matching nested object/array property is that branch’s output model (`mapped`, `partition_by`, `parent_rows`, …).
5. **Object/array properties** in output also open child branch slots (needed for `parent_rows` with no child SQL file). File-derived children that are missing from that map are merged in.
6. Nested child SQL is looked up as `$.{property}.sql` for property `property`. Output does **not** invent SQL filenames; it shapes whatever SQL (or parent rows) that branch has.

| Pattern | Files | Output role |
|---|---|---|
| Trunk + shape | `$.sql` + `$.output.json` | Root `type` / `properties` shape the trunk result |
| Nested child SQL | `$.sql` + `$.roles.sql` + `$.output.json` | Property `roles` must match the file suffix; its schema shapes the child |
| `parent_rows` only | `$.sql` + `$.output.json` (no `$.roles.sql`) | Property `roles` with `parent_rows: true` nests from parent rows |
| Sibling branches | `$.paging.sql` + `$.data.sql` + `$.output.json` (no `$.sql`) | Properties `paging` / `data` match suffixes |

```text
api/user/get/
  $.sql
  $.output.json
  $.output.summary.json     # optional alternate shape (output_mapper="summary")

api/user/nested/
  $.sql                     # parent rows
  $.roles.sql               # child SQL → output property "roles"
  $.output.json

api/user/page/
  $.paging.sql              # sibling branch (no trunk $.sql)
  $.data.sql
  $.output.json
```

Call path = folder path: `y.query("user/page", args={"page": 1, "page_size": 10})`.

## Parameters

The SQL parameter header is the **sole input model**. Declare types at the top of a SQL file (first significant token; leading blank lines/spaces are fine); bind with `{{...}}`. Yaal derives args/payload input schemas from these headers (union across files in the operation).

```sql
--($args.id integer, name! string)--

select *
from users u
where optional(u.user_id = {{$args.id}})
  and u.user_name = {{name}}
```

Allowed scalar types: `integer`, `string`, `float`, `bool`, `blob`. Array types use the same names with `[]` (e.g. `integer[]`, `string[]`): a single `{{param}}` in SQL expands at compile time to one bound placeholder per list element—typical pattern `col in ({{$args.ids}})` → `col in (?, ?, ?)`. An empty list `[]` is a compile error for a required filter; inside `optional(...)`, `null` and `[]` both count as omitted. Array types do not support header defaults.

Each name needs a type; duplicates and unknown types are errors. Trailing `!` on a name marks it **required** (`--($args.id! integer)--`). Optional `= <literal>` sets a **default** used when the caller omits the value (scalar types only):

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

`optional(...)` may reference **multiple** `{{params}}` in one block—including array params (e.g. `optional(col1 = {{a}} and col2 in ({{ids}}) and col3 = {{b}})` with `ids integer[]`). The block is **kept** only when **every** listed param is provided (non-empty for arrays); it is **removed** when **all** are null, omitted, or `[]`. If only **some** are provided, compile fails with an error (partial parameters are not allowed).

Optional **`IN`** with a single array arg (typical list filter):

```sql
--($args.id integer[])--
select * from (select 1 as id union select 2) t
where optional(id in ({{$args.id}}))
```

| CLI | Result |
|---|---|
| no `id` / `[]` | `WHERE` elided → both rows |
| `--args '{"id": [1, 2]}'` or `--arg 'id=[1,2]'` | `where (id in (?, ?))` |

Do not nest the args object inside `--arg id=…` (e.g. `--arg id='{"id":[1,2]}'` validates as an object, not an array).

If the elided filter was the only predicate, the empty `WHERE`, ClickHouse `PREWHERE`, or `HAVING` is dropped — including before `)` or other clause starts (no leftover bare clause or `1 = 1`). Leading author `WHERE`/`PREWHERE`/`HAVING 1 = 1 AND|OR …` is cleaned the same way when the rest remains. When multiple filter clauses appear, each is cleaned independently. For why this SQL-first, engine-agnostic elision fits ClickHouse-style engines (and reporting-shaped queries generally) better than an ORM-owned dialect layer, see [Why SQL-first fits](why-sql-first.md).

Long form still works and must be parenthesized: `({{param}} is null or col = {{param}})` (case and surrounding whitespace are flexible).

### Conditional filters (`optional_when`)

`optional(...)` decides from the params **inside** the block. `optional_when({{cond}}, body)` adds a separate **condition** param that gates the block from outside:

```sql
--($args.apply bool, $args.id integer)--
and optional_when({{$args.apply}}, u.user_id = {{$args.id}})
```

The condition is removed from the emitted SQL and is never bound — it only decides whether the block survives. The body keeps the ordinary `optional(...)` all-or-nothing rules on top.

| `$args.apply` | `$args.id` | Result |
|---|---|---|
| omitted / null | given | clause removed |
| `true` or `false` | given | `and (u.user_id = ?)` |
| given | omitted | clause removed |

Only **absence** disables the block, so a `bool` condition of `false` still keeps it — pass `null` (or omit the arg) to drop the filter. An absent condition is checked first, so it also suppresses the body's partial-parameter error.

The condition must be declared in the parameter header like any other arg, and the body needs at least one `{{param}}` of its own (or an `optional_groups_or(...)`). Nesting one `optional_when(...)` directly inside another is rejected.

Inside an `optional_groups_or(...)` body the condition must be a header-declared `$args.*` name — a bare name there would be a blob row field, which is always present and could never gate the block. With a condition in place the body may use **only** row fields, since the condition does the gating:

```sql
-- elides per branch when $args.flag is omitted; cv/cv2 come from each row
optional_groups_or({{$args.pairs}}, c1 = {{cv}} and optional_when({{$args.flag}}, c2 = {{cv2}}))
```

Fixture: `user/when_optional` under `tests/fixtures/api/`.

### Optional groups (`optional_groups_or`, `optional_groups_and`)

Repeat the same predicate for each row of a **blob** parameter (JSON array of objects). `optional_groups_or(...)` joins the copies with **OR**, `optional_groups_and(...)` with **AND**; omit/null/`[]` on the blob removes the whole clause (like `optional`). The two keywords are otherwise identical — same body rules, same metadata, same elision — so everything below applies to both.

```sql
--($args.pairs blob)--
select * from t
where optional_groups_or(
  {{$args.pairs}},
  col2 in ({{cv}})
  and col1 = {{cv1}}
)
```

Runtime `$args.pairs`:

```json
[
  {"cv": [1, 2, 3], "cv1": 10},
  {"cv": [4], "cv1": 20}
]
```

Compiled shape:

```sql
where ((col2 in (?, ?, ?) and col1 = ?) or (col2 in (?) and col1 = ?))
```

Each row becomes its own parenthesized predicate, and when there are two or more rows the whole OR-join is wrapped once more. That outer pair is what keeps a neighbouring `AND` from binding tighter than the group, so two groups side by side compile as separate units:

```sql
where optional_groups_or({{$args.pairs}}, id = {{id}})
  and optional_groups_or({{$args.pairs1}}, id = {{id}})
-- 2 rows in pairs, 1 row in pairs1:
where ((id = ?) or (id = ?)) and (id = ?)
```

A single-row group is already one predicate in parens, so it is not wrapped again.

#### Choosing the joiner

Swap the keyword to change only how the rows combine. `optional_groups_or(...)` reads as "match any of these rows" — the usual shape for a list of filter alternatives. `optional_groups_and(...)` reads as "match all of these rows", which is what you want when each row narrows the result further, such as a tag filter that must match every requested tag.

```sql
-- any of the requested pairs matches
where optional_groups_or({{$args.pairs}}, id = {{id}})
-- 2 rows: where ((id = ?) or (id = ?))

-- every requested pair must match
where optional_groups_and({{$args.pairs}}, id = {{id}})
-- 2 rows: where ((id = ?) and (id = ?))
```

Both keywords are plain boolean units, so they compose freely and may sit side by side with each other or with the surrounding predicate.

There is no bare `optional_groups(...)` keyword. Using it is a compile error that points at the two replacements, so an old statement fails loudly rather than silently picking a joiner.

| Blob field value | Placeholders |
|---|---|
| scalar (number, string, bool, bytes) | one `?` |
| non-empty JSON array of scalars | `?, ?, …` (per-row `IN` arity) |
| `[]` | compile error (empty `IN`) |
| nested object or array | compile error (not bindable) |
| missing key or `null` | compile error |

Body placeholders (`cv`, `cv1`, …) are written **bare** and are **keys** in each row object (case-insensitive at bind time). A bare name must **not** be declared bare in the SQL parameter header. The first argument must be a single `{{blob}}` declared `blob`. This is separate from multi-param `optional(...)` and from header `integer[]` (one list param for one `IN`).

#### `$args` in the body template

A group body has two namespaces, and the `$args.` prefix is what tells them apart: **bare** names are blob row fields, `$args.` names are runtime args that must be **declared in the parameter header**.

```sql
--($args.pairs blob, $args.flag integer)--
select * from t
where optional_groups_or(
  {{$args.pairs}},
  col1 = {{cv1}}
  and col2 = {{$args.flag}}
)
```

| Body placeholder | Binds from |
|---|---|
| `{{cv1}}` | row key `cv1` in each blob row |
| `{{$args.flag}}`, declared in the header | the runtime arg, repeated in **every** OR branch |
| `{{$args.nope}}`, not declared | compile error (`type missing`) |

With two rows and `flag = 9`, the example above compiles to `((col1 = ? and col2 = ?) or (col1 = ? and col2 = ?))` and binds `[cv1_row1, 9, cv1_row2, 9]`.

Because the two namespaces are distinct, a row field `cv1` may coexist with a declared `$args.cv1` used elsewhere in the statement. These are compile errors: using the blob itself in its own body (`{{$args.pairs}}` above), referencing a header **array** parameter such as `integer[]` from a group body (put the list in each row instead, which is how per-row `IN` works), a `{{$args.x}}` in the body that is not declared in the header, and a body whose placeholders are all `$args.` names (no row field left to iterate).

#### Nesting with `optional`

A group may sit **inside** an `optional(...)` block, so one switch controls a scalar filter and a per-row group together:

```sql
--($args.pairs blob, $args.active integer)--
select * from users u
where u.user_id > 0
  and optional(u.active = {{$args.active}}
               and optional_groups_or({{$args.pairs}}, u.user_id in ({{ids}})))
```

A nested group is **independent of the optional's all-or-nothing set**: the blob parameter does not gate the block, and neither do the row fields (`ids` above). The group elides itself when its blob has no rows, and the surrounding `optional(...)` is still decided only by its own `{{$args.*}}` params — `$args.active` here.

| Args | Result |
|---|---|
| `pairs` and `active` both given | `and (u.active = ? and ((u.user_id in (?)) or (u.user_id in (?))))` |
| only `active` given | `and (u.active = ?)` — the group drops out, the block stays |
| `active` omitted | whole `optional(...)` block removed, group included |

The group keeps its own parentheses inside the block, so the OR-join still reads as one unit next to the optional's `AND`.

An `optional(...)` may also wrap nothing but a group. It then has no params of its own, so it lives and dies with the group — when the blob has no rows the wrapper parentheses disappear too, rather than compiling to an empty `()`.

```sql
-- $args.pairs absent compiles to `select * from t where z = 1`
select * from t where z = 1 and optional(optional_groups_or({{$args.pairs}}, id = {{id}}))
```

The reverse nesting also works: an `optional(...)` **inside** a group body elides per branch, but it must reference at least one header-declared `$args.*` param. One keyed only on row fields is a compile error, since row fields are always required.

```sql
-- ok: elides in every branch when $args.flag is omitted
optional_groups_or({{$args.pairs}}, col1 = {{cv1}} and optional(col2 = {{$args.flag}}))
-- error: cv1 is a row field, so this optional could never be dropped
optional_groups_or({{$args.pairs}}, optional(col1 = {{cv1}}))
```

Fixture: `user/groups_in_optional` under `tests/fixtures/api/`.

The blob argument itself may arrive as a **list of row objects**, a **JSON string**, or **UTF-8 JSON bytes** — all three compile and bind identically. Anything that is not a JSON array of objects (a bare object, a scalar, malformed JSON) is rejected. `null`, `[]`, `""`, and `"[]"` all count as **no rows** and elide the clause.

`blob` args model as `{"type": "array", "items": {"type": "object"}}`, so list values are not coerced to strings on the way in.

Minimal end-to-end fixture: `user/groups` under `tests/fixtures/api/` (and `experiment/api/`). CLI: `--arg 'pairs=[{"id":1}]'` or `--args '{"pairs":[{"id":1}]}'`.

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
| `sort({{param}}, key = expr, …)` | Resolve `param` to one or more declared keys (case-insensitive, comma-separated — see multi-column below). Splice the matching **author** `expr`(s). Unknown non-null key → soft `{"errors":[…]}` (no execute). |
| `dir({{param}})` | Positional list matched to `sort`'s keys (comma-separated). Allowed values: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last` (case-insensitive). Missing trailing entries default to `asc`. Too many entries or an unknown value → soft error. |
| Null / omitted `sort` (no header default) | **Elide** the dynamic `sort()`/`dir()` term (see mixing below; dir is ignored). |
| Header default on `sort` / `dir` | Omitted args use that default (e.g. `string = id`) instead of eliding. |

#### Multi-column dynamic sort

`sort` and `dir` are each a comma-separated list, matched **by position**. A single key/value (`sort=name`, `dir=desc`) is just the length-1 case:

```bash
yaal query user/list --arg sort=name,id --arg dir=desc,asc
```

resolves to `ORDER BY u.user_name DESC, u.user_id ASC`. Rules: keys must be declared in `sort(...)`'s choices and not repeated; `dir` may have fewer entries than `sort` (missing ones default to `asc`) but not more (soft error).

#### NULLS FIRST/LAST

Client-controlled via the `dir` vocabulary above, one entry per sort key:

```bash
yaal query user/list --arg sort=name --arg dir=desc_nulls_last
```

resolves to `ORDER BY u.user_name DESC NULLS LAST`. **MySQL has no `NULLS FIRST/LAST` syntax** — a `*_nulls_first`/`*_nulls_last` value against a MySQL connection raises a real SQL error from the engine, not a Yaal soft error.

#### Mixing with a static tiebreaker

At most one dynamic term (`sort()` plus its immediately-adjacent optional `dir()`) is allowed per `ORDER BY`, but ordinary static terms may sit before and/or after it, comma-separated:

```sql
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}}),
  u.user_id asc
```

When `sort` is null (no header default), only the dynamic term elides — the static tiebreaker(s) remain (`ORDER BY u.user_id asc`). The whole `ORDER BY` only elides when the dynamic term is the sole term.

#### Multiple `sort()`/`dir()` pairs in one statement

A statement may contain more than one `ORDER BY` — e.g. a subquery's `ORDER BY` plus the outer query's — and each may have its own dynamic `sort()`/`dir()` pair (still at most one dynamic term per individual `ORDER BY`). Each pair **must use a distinct `{{param}}`**: resolution is keyed by param name for the whole statement, so reusing the same param across two `sort()` calls (each with its own `key = expr` choices) is rejected at parse time, since only one of the two would ever get spliced.

```sql
--($args.s1 string, $args.d1 string, $args.s2 string, $args.d2 string)--
select * from (
  select * from t1
  order by sort({{$args.s1}}, a = x, b = y) dir({{$args.d1}})
) sub
order by sort({{$args.s2}}, c = z, d = w) dir({{$args.d2}})
```

**v2 rules:** the dynamic term is `sort(...)` and optional immediately-following `dir(...)` — nothing else in that same comma-separated term. Keys must match `\w+`. Each `sort()` in a statement must use a distinct param. `sort`/`dir` outside `ORDER BY` parse but are the author's responsibility. Empty-string / whitespace-only sort keys are soft errors (not treated as null).

Security: only expressions written in the descriptor are ever spliced; client keys never become SQL — the NULLS vocabulary above is a fixed keyword allowlist, not client-supplied SQL.

```bash
yaal explain user/list --arg sort=name --arg dir=desc
yaal query user/list --arg sort=id --arg dir=asc
```

## Output shaping

Root `type` is `object` (one result) or `array` (list). Fields are a flat map under `properties`. Named nested branches use their own `type` + `properties`.

```json
{
  "type": "array",
  "partition_by": "id",
  "properties": {
    "id": { "mapped": "id" },
    "details": {
      "type": "object",
      "parent_rows": true,
      "properties": {
        "name": { "mapped": "name" }
      }
    }
  }
}
```

```json
{
  "type": "object",
  "partition_by": "user_id",
  "properties": {
    "id": { "mapped": "user_id" },
    "roles": {
      "type": "array",
      "partition_by": "role_id",
      "parent_rows": true,
      "properties": {
        "id": { "mapped": "role_id" }
      }
    }
  }
}
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

Branch map seed = **SQL files on disk** + **object/array properties in `$.output.json`** (see [How SQL files and `$.output.json` relate](#how-sql-files-and-outputjson-relate)).

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

Ordinary SELECT twigs omit `$mode` entirely — rows go through normal `$.output.json` shaping.

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

Uses the `json` column as the branch result. If the value is a string, it is parsed as JSON; otherwise it is passed through. Handy when the database already builds JSON (e.g. `json_group_array` / `jsonb_agg`). Bypasses `$.output.json` shaping for that branch.

```sql
SELECT
    'json' AS "$mode",
    json_group_array(json_object('id', user_id, 'name', user_name)) AS json
FROM users
WHERE active = 1
```

## `output_mapper`

Alternate shapes use `$.output.<name>.json`:

```python
y.query("user/get", args={"id": 1}, output_mapper="summary")
# loads $.output.summary.json instead of $.output.json
```

There is no process-wide or cross-query result cache. `clear_cache()` only clears cached descriptors (reload SQL/JSON).

## Precompiled descriptors

Compile SQL and `$.output.json` once (twig token arrays compacted: adjacent whitespace and static SQL merged; structural tokens such as parameters, braces, and `sort()`/`dir()` preserved), then load at runtime without re-lexing sources:

```bash
yaal --api path/to/api compile --out path/to/precompiled
```

```python
y = Yaal("path/to/api", precompiled="path/to/precompiled")
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
y.query("user/get", args={"id": 1})
```

`debug=True` forces live SQL/JSON and ignores `precompiled`. Artifacts are one JSON file per path (`user/get.json`; alternate mappers as `user/get#summary.json`). Optional-filter SQL elision still runs per request.

**C# typed load:** precompiled JSON is deserialized into `Branch` / `Twig` via `System.Text.Json` (snake_case). No manual token-by-token parsing at load time.

**C# source precompile (zero-parse startup):**

```bash
dotnet run --project csharp/src/Yaal.Cli -- compile --api path/to/api --format cs --out Generated/YaalDescriptors
```

Generated `.g.cs` files expose static `Branch` properties plus `YaalDescriptorRegistry.All`. Register at startup:

```csharp
var y = new Yaal.Yaal("path/to/api");
foreach (var (path, branch) in Yaal.Generated.YaalDescriptorRegistry.All)
    y.RegisterDescriptor(path, branch);
```

**In-memory registration (C#):** pass a built or generated `Branch` without any artifact file:

```csharp
y.RegisterDescriptor("user/get", myBranch);
y.UnregisterDescriptor("user/get");
```

Load order when `debug=false`: registered descriptor → memory cache → precompiled JSON directory → live SQL/JSON.

## Performance notes

- Providers drain cursors with `fetchmany` into a per-branch row list; nesting (`partition_by`) still buffers that branch in memory.
- Compiled SQL (after optional-filter elision) is cached per twig + null-set + placeholder for the duration of a trunk execution.
- Twigs without `sort()`/`dir()` skip runtime sort/dir resolution (`has_sort_dir: false` on precompiled artifacts).
- Precompiled twig tokens are compacted at parse time (smaller JSON, faster load); compile/explain semantics are unchanged.
- Nested row stitching uses shallow copies where safe (`use_parent_rows`, `partition_by`, single-parent branches).
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

Args/payload schemas are derived from SQL headers (`float`→`number`, `bool`→`boolean`). Soft validation checks types and `required` on that model. Invalid args/payload return `{"errors": [...]}`.

## Public API

### Python

```python
from yaal import Yaal

y = Yaal("path/to/api", debug=True)  # or precompiled="path/to/precompiled"
y.setup_data_provider("db", "sqlite3:////tmp/app.db")
# or an app-supplied manager (get_context / begin / execute / end / error):
# y.setup_data_provider("db", MyContextManager())

y.query("user/get", args={"id": 1})
y.query_json("user/get", args={"id": 1})
y.explain_sql("user/get", args={"id": 1})
y.clear_cache()
```

`debug=True` disables descriptor file caching (reload each call). Not a log level.

### C#

```csharp
var y = new Yaal.Yaal("path/to/api", debug: true);
// or precompiled="path/to/precompiled" for JSON artifacts
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");
// or an app IDataProviderContextManager:
// y.SetupDataProvider("db", new MyContextManager());

y.RegisterDescriptor("user/get", myBranch);  // optional in-memory descriptor
y.Query("user/get", args: new { id = 1 });
y.QueryJson("user/get", args: new { id = 1 });
y.ExplainSql("user/get", args: new { id = 1 });
y.UnregisterDescriptor("user/get");
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

Same descriptors, no ORM-side dialect layer, across every engine above — see [Why SQL-first fits](why-sql-first.md) for why that matters for ClickHouse-like engines and complex reporting apps.
