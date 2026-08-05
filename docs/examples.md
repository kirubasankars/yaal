# Examples

Every example below lives under [`tests/fixtures/api/`](../tests/fixtures/api/). Seed data: two users (`admin`, `guest`), two roles, join table — see [`docker/sqlite/schema.sql`](../docker/sqlite/schema.sql).

```bash
make example              # full tour (Python)
make example-csharp       # full tour (.NET)
make yaal ARGS='list'
```

---

## Nested get — `user/get`

Join rows become one user object with a nested `roles` array.

### Descriptor

```text
user/get/
  $.sql
  $.input.yaml
  $.output.yaml
  $.output.cached.yaml    # used when output_mapper="cached"
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

### Commands

```bash
yaal query user/get --arg id=1
yaal explain user/get --arg id=1
yaal explain user/get          # optional(...) removed; binds []
```

```python
y.query("user/get", args={"id": 1})
y.query("user/get", args={"id": 1}, output_mapper="cached")
```

```csharp
y.Query("user/get", args: new { id = 1 });
y.Query("user/get", args: new { id = 1 }, outputMapper: "cached");
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

## Optional list — `user/list`

Root `type: array`. Omit `active` to return everyone; pass it to filter.

### Descriptor

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

### Commands

```bash
yaal query user/list
yaal query user/list --arg active=1
yaal explain user/list
yaal explain user/list --arg active=1
```

### Explain (elision)

**active omitted**

```sql
select
    u.user_id,
    u.user_name,
    u.active
from users u
where 1 = 1
order by u.user_id
-- binds: []
```

**active=1**

```sql
select
    u.user_id,
    u.user_name,
    u.active
from users u
where 1 = 1
  and (u.active = ?)
order by u.user_id
-- binds: [1]
```

### Sample JSON (`active=1`)

```json
[
  { "id": 1, "name": "admin", "active": 1 },
  { "id": 2, "name": "guest", "active": 1 }
]
```

---

## Paginated nest — `user/page`

No trunk `$.sql`. Two branch files become `paging` and `data` on the result object.

### Layout

```text
user/page/
  $.paging.sql
  $.data.sql
  $.input.yaml
  $.output.yaml
```

**`$.paging.sql`** — `$action=params` stores `total_count`, then returns paging fields:

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

## Multi-twig write — `user/create`

One file, three twigs: insert user → assign role → select shaped result. Uses **payload** (not args).

### Descriptor

**`$.input.yaml`**

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

### Commands

```bash
yaal query user/create --payload '{"id":3,"name":"newbie"}'
```

```python
y.query("user/create", payload={"id": 3, "name": "newbie"})
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

Invalid payload returns soft errors (not raised):

```bash
yaal query user/create --payload '{"name":"x"}'
# {"errors":[...]}
```

---

## Alternate output — `output_mapper`

Same SQL as `user/get`; different YAML:

```text
user/get/$.output.cached.yaml   # cache: true
```

```python
y.query("user/get", args={"id": 1}, output_mapper="cached")
```

JSON shape matches the default get.

---

## Experiment sandbox

Persistent local copy of the fixtures for editing:

```bash
make experiment-init
make experiment ARGS='query user/page --arg page=1 --arg page_size=10'
# edit experiment/api/... then re-run
make experiment-reset
make experiment-clean
```

---

## Your own API tree

```text
my-api/
  orders/
    list/
      $.sql
      $.input.yaml
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

Both print get / cached / list / page / create and show `explain` elision for `user/list`.

See also: [descriptors.md](descriptors.md) · [README.md](README.md)
