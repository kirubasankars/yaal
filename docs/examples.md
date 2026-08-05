# Yaal examples

All examples use the shared fixtures under [`tests/fixtures/api/`](../tests/fixtures/api/) and the users/roles seed in [`docker/sqlite/schema.sql`](../docker/sqlite/schema.sql).

```bash
make install
make yaal ARGS='list'
# user/create
# user/get
# user/list
# user/page
```

Zero-config CLI runs seed a temp SQLite DB automatically when `--db` is omitted.

---

## 1. Nested get — `user/get`

Join fan-out collapsed into a user object with a `roles` array via `parent_rows`.

### Files

[`tests/fixtures/api/user/get/`](../tests/fixtures/api/user/get/)

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

**`$.input.yaml`**

```yaml
args:
  type: object
  properties:
    id:
      type: integer
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

### Run

```bash
yaal query user/get --arg id=1
yaal explain user/get --arg id=1
# omit id → optional(...) clause removed; binds []
yaal explain user/get
```

```python
from yaal import Yaal

y = Yaal("tests/fixtures/api", debug=True)
y.setup_data_provider("db", "sqlite3:////tmp/app.db")

y.query("user/get", args={"id": 1})
```

```csharp
var y = new Yaal.Yaal("tests/fixtures/api", debug: true);
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");
var result = y.Query("user/get", args: new { id = 1 });
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

Flat SQL rows → nested JSON:

| user_id | user_name | role_id | role_name |
|---:|---|---:|---|
| 1 | admin | 1 | Administrator |
| 1 | admin | 2 | User |

`partition_by: user_id` keeps one object; `roles` with `parent_rows: true` nests from those same rows.

---

## 2. List with optional filter — `user/list`

Root `type: array`. Pass `active` to filter, or omit it.

### Files

[`tests/fixtures/api/user/list/`](../tests/fixtures/api/user/list/)

**`$.sql`**

```sql
--($args.active integer)--

select
    u.user_id,
    u.user_name,
    u.active
from users u
where 1 = 1
  and optional(u.active = {{$args.active}})
order by u.user_id
```

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

### Run

```bash
yaal query user/list
yaal query user/list --arg active=1
yaal explain user/list --arg active=1
yaal explain user/list
```

```python
y.query("user/list")
y.query("user/list", args={"active": 1})
```

### Sample JSON (`active=1`)

```json
[
  { "id": 1, "name": "admin", "active": 1 },
  { "id": 2, "name": "guest", "active": 1 }
]
```

---

## 3. Paginated nest — `user/page`

Multi-file branches: paging metadata + a page of users with roles. No trunk `$.sql`.

### Layout

```text
user/page/
  $.paging.sql
  $.data.sql
  $.input.yaml
  $.output.yaml
```

**`$.paging.sql`** — stash `total_count` on `$params`, then return paging fields:

```sql
--($args.page integer, $args.page_size integer, $params.total_count integer)--

SELECT
    'params' AS "$action",
    COUNT(*) AS total_count
FROM users
WHERE active = 1

--sql--

SELECT
    {{$args.page}} AS page,
    {{$args.page_size}} AS page_size,
    {{$params.total_count}} AS total_count
```

**`$.data.sql`** — page **users** in a subquery, then join roles (so `LIMIT` does not truncate role rows):

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

**`$.output.yaml`** (branch names match file suffixes):

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

### Run

```bash
yaal query user/page --arg page=1 --arg page_size=10
yaal query user/page --arg page=1 --arg page_size=1
yaal query user/page --arg page=2 --arg page_size=10
```

```python
y.query("user/page", args={"page": 1, "page_size": 10})
```

```csharp
y.Query("user/page", args: new { page = 1, page_size = 10 });
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

## 4. Multi-twig write — `user/create`

One SQL file, three twigs: insert user → assign default role → select shaped result.

### Files

[`tests/fixtures/api/user/create/`](../tests/fixtures/api/user/create/)

**`$.input.yaml`** (payload, not args):

```yaml
payload:
  type: object
  properties:
    id:
      type: integer
    name:
      type: string
  required:
    - id
    - name
```

**`$.sql`**

```sql
--(id integer, name string)--

INSERT INTO users (user_id, user_name, active) VALUES ({{id}}, {{name}}, 1)

--sql--

INSERT INTO user_roles (user_id, role_id) VALUES ({{id}}, 2)

--sql--

SELECT
    u.user_id,
    u.user_name,
    r.role_id,
    r.role_name
FROM users u
INNER JOIN user_roles ur ON ur.user_id = u.user_id
INNER JOIN roles r ON r.role_id = ur.role_id
WHERE u.user_id = {{id}}
ORDER BY r.role_id
```

Output shape matches `user/get` (object + nested roles).

### Run

```bash
yaal query user/create --payload '{"id":3,"name":"newbie"}'
```

```python
y.query("user/create", payload={"id": 3, "name": "newbie"})
# then:
y.query("user/get", args={"id": 3})
```

```csharp
y.Query("user/create", payload: new { id = 3, name = "newbie" });
```

### Sample JSON

```json
{
  "id": 3,
  "name": "newbie",
  "roles": [
    { "id": 2, "name": "User" }
  ]
}
```

Missing required fields return soft errors (not raised):

```bash
yaal query user/create --payload '{"name":"x"}'
# {"errors": [{"message": "..."}]}
```

---

## 5. Alternate output mapper — `user/get` + `cached`

Same SQL as `user/get`, different output file: `$.output.cached.yaml` sets `cache: true`.

```bash
# default mapper → $.output.yaml
yaal query user/get --arg id=1
```

```python
y.query("user/get", args={"id": 1}, output_mapper="cached")
# loads $.output.cached.yaml
```

```csharp
y.Query("user/get", args: new { id = 1 }, outputMapper: "cached");
```

JSON shape matches the default get; the mapper mainly demonstrates alternate outputs and request-scoped row caching.

---

## 6. Experiment sandbox

Edit descriptors against a persistent local DB:

```bash
make experiment-init
make experiment ARGS='query user/page --arg page=1 --arg page_size=10'
# edit experiment/api/... then re-run
make experiment-reset    # reseed DB only
make experiment-clean
```

---

## 7. Programmatic patterns

### Python

```python
from yaal import Yaal

y = Yaal("tests/fixtures/api", debug=True)
y.setup_data_provider("db", "sqlite3:////tmp/app.db")

# nested get
user = y.query("user/get", args={"id": 1})

# optional filter list
active_users = y.query("user/list", args={"active": 1})

# branches
page = y.query("user/page", args={"page": 1, "page_size": 10})

# write
created = y.query("user/create", payload={"id": 3, "name": "newbie"})

# inspect compiled SQL + binds
for twig in y.explain_sql("user/list", args={"active": None}):
    print(twig["sql"])
    print(twig["parameters"])

# JSON string
print(y.query_json("user/get", args={"id": 2}))
```

### C#

```csharp
var y = new Yaal.Yaal("tests/fixtures/api", debug: true);
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");

var user = y.Query("user/get", args: new { id = 1 });
var page = y.Query("user/page", args: new { page = 1, page_size = 10 });
var created = y.Query("user/create", payload: new { id = 3, name = "newbie" });
string json = y.QueryJson("user/list", args: new { active = 1 });

foreach (var twig in y.ExplainSql("user/get", args: new { id = 1 }))
    Console.WriteLine(twig["sql"]);
```

### Your own API tree

```text
my-api/
  orders/
    list/
      $.sql
      $.input.yaml
      $.output.yaml
```

```bash
yaal query orders/list --api ./my-api --db 'sqlite3:////tmp/app.db' --args '{"status":"open"}'
```

```python
y = Yaal("./my-api")
y.setup_data_provider("db", "postgresql://user:pass@127.0.0.1:5432/app")
y.query("orders/list", args={"status": "open"})
```

---

## See also

- Descriptor reference: [descriptors.md](descriptors.md)
- Root quick start: [../README.md](../README.md)
- C# port: [../csharp/README.md](../csharp/README.md)
