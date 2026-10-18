# Examples

Each block is the SQL, the parameter object, and the compiled statement.

## Optional filter and sort

```sql
--(active, sort, dir)--
select u.user_id, u.user_name
from users u
where 1 = 1
  and optional(u.active = {{active}})
order by
  sort({{sort}}, name = u.user_name, id = u.user_id)
  dir({{dir}})
```

```json
{
  "active": 1,
  "sort": "name",
  "dir": "desc"
}
```

```sql
select u.user_id, u.user_name
from users u
where (u.active = ?)
order by
  u.user_name DESC
```

The bind is `active` integer `1`. Omitting `active` drops the `where`. Omitting `sort` drops the dynamic order term.

## One optional predicate

```sql
--(param1)--
select *
from a
where 1 = 1
   or optional(col1 = {{param1}})
```

`{"param1": 1}` compiles to:

```sql
select *
from a
where (col1 = ?)
```

`{}` compiles to:

```sql
select *
from a
```

## Several parameters in one optional

```sql
--(a, b)--
select *
from t
where optional(
  col1 = {{a}}
  and col3 = {{b}}
)
```

`{"a": 1, "b": 1}` compiles to:

```sql
select *
from t
where (
  col1 = ?
  and col3 = ?
)
```

`{}` removes the `where`. `{"b": 1}` is an error: every parameter in the block must be present, or all of them omitted.

## List expansion

```sql
--(cv)--
select *
from t
where optional(col in ({{cv}}...))
```

`{"cv": [1, 1, 1]}` compiles to:

```sql
select *
from t
where (col in (?, ?, ?))
```

`{}` compiles to `select * from t`. An empty list outside `optional(...)` is an error.

## optional_when

```sql
--(apply, id)--
select *
from t
where z = 1
  and optional_when({{apply}}, u.user_id = {{id}})
```

`{"apply": true, "id": 1}` compiles to:

```sql
select *
from t
where z = 1
  and (u.user_id = ?)
```

`{"id": 1}` compiles to:

```sql
select *
from t
where z = 1
```

`apply` is not bound.

## Nested

`optional_when` wraps `optional_groups_or`.

```sql
--(apply, pairs blob)--
select *
from t
where z = 1
  and optional_when(
    {{apply}},
    optional_groups_or({{pairs}}, id = {{self.id}})
  )
```

```json
{
  "apply": true,
  "pairs": [
    {"id": 1},
    {"id": 1}
  ]
}
```

```sql
select *
from t
where z = 1
  and (
    (id = ?)
    or (id = ?)
  )
```

`{"pairs": [{"id": 1}, {"id": 1}]}` compiles to `select * from t where z = 1`. The group is not rendered.

`optional_when` inside a group. The condition is the header argument `flag`.

```sql
--(pairs blob, flag)--
select *
from t
where optional_groups_or(
  {{pairs}},
  c1 = {{self.cv}}
  and optional_when({{flag}}, c2 = {{self.cv2}})
)
```

```json
{
  "flag": true,
  "pairs": [
    {"cv": 1, "cv2": 1},
    {"cv": 1, "cv2": 1}
  ]
}
```

```sql
select *
from t
where (
  (c1 = ? and (c2 = ?))
  or (c1 = ? and (c2 = ?))
)
```

Omitting `flag` leaves `where ((c1 = ?) or (c1 = ?))`.

`optional` wrapping a group and a header argument.

```sql
--(pairs blob, x)--
select *
from t
where z = 1
  and optional(
    a = {{x}}
    and optional_groups_or({{pairs}}, id = {{self.id}})
  )
```

```json
{
  "x": 1,
  "pairs": [
    {"id": 1},
    {"id": 1}
  ]
}
```

```sql
select *
from t
where z = 1
  and (
    a = ?
    and (
      (id = ?)
      or (id = ?)
    )
  )
```

`{"x": 1, "pairs": null}` compiles to `select * from t where z = 1 and (a = ?)`.

`optional` inside a group.

```sql
--(pairs blob, flag)--
select *
from t
where optional_groups_or(
  {{pairs}},
  col1 = {{self.cv1}}
  and optional(col2 = {{flag}})
)
```

```json
{
  "flag": 1,
  "pairs": [
    {"cv1": 1},
    {"cv1": 1}
  ]
}
```

```sql
select *
from t
where (
  (col1 = ? and (col2 = ?))
  or (col1 = ? and (col2 = ?))
)
```

Omitting `flag` leaves `where ((col1 = ?) or (col1 = ?))`.

## Group rows

```sql
--(pairs blob)--
select u.user_id
from users u
where optional_groups_or(
  {{pairs}},
  u.user_id = {{self.id}}
  and u.role_id in ({{self.ids}}...)
)
```

```json
{
  "pairs": [
    {"id": 1, "ids": [8, 9]}
  ]
}
```

```sql
select u.user_id
from users u
where (
  u.user_id = ?
  and u.role_id in (?, ?)
)
```

Binds are `self.id` `1`, then `self.ids` `8` and `9`.

`optional_groups_or` with two rows:

```sql
--(pairs blob)--
select *
from t
where optional_groups_or(
  {{pairs}},
  col2 = {{self.cv}}
  and col1 = {{self.cv1}}
)
```

```json
{
  "pairs": [
    {"cv": 1, "cv1": 1},
    {"cv": 1, "cv1": 1}
  ]
}
```

```sql
select *
from t
where (
  (col2 = ? and col1 = ?)
  or (col2 = ? and col1 = ?)
)
```

`optional_groups_and` with two rows:

```sql
--(pairs blob)--
select *
from t
where optional_groups_and(
  {{pairs}},
  id = {{self.id}}
)
```

```json
{
  "pairs": [
    {"id": 1},
    {"id": 1}
  ]
}
```

```sql
select *
from t
where (
  (id = ?)
  and (id = ?)
)
```

`{"pairs": null}` removes the group.

## Group and sort

```sql
--(pairs blob, sort)--
select *
from t
where optional_groups_or(
  {{pairs}},
  col1 = {{self.cv1}}
)
order by sort({{sort}}, id = id)
```

```json
{
  "pairs": [
    {"cv1": 1}
  ],
  "sort": "id"
}
```

```sql
select *
from t
where (col1 = ?)
order by id ASC
```

## Errors

These inputs do not compile.

### Partial optional

`a` is omitted and `b` is present.

```sql
--(a, b)--
select *
from t
where optional(
  col1 = {{a}}
  and col3 = {{b}}
)
```

```json
{"b": 1}
```

```text
optional(...) requires every parameter to be provided or all omitted; partial parameters: a
```

### Empty list

```sql
--(cv)--
select *
from t
where col in ({{cv}}...)
```

```json
{"cv": []}
```

```text
empty array for {{cv}}; IN list cannot be empty
```

### Empty list on a group row

```sql
--(pairs blob)--
select *
from t
where optional_groups_or(
  {{pairs}},
  col2 in ({{self.cv}}...)
)
```

```json
{
  "pairs": [
    {"cv": []}
  ]
}
```

```text
empty array for optional_groups field {{cv}} in pairs; IN list cannot be empty
```

### Group source is not blob

```sql
--(pairs)--
select *
from t
where optional_groups_or(
  {{pairs}},
  col1 = {{self.cv1}}
)
```

```json
{"pairs": 1}
```

```text
optional_groups_*(...) source {{pairs}} must be declared blob in query.sql
```

### Bad header

```sql
--(a integer junk)--
select {{a}}
```

```json
{"a": 1}
```

```text
invalid parameter declaration 'a integer junk' in query.sql
```

### Nested optional_when

```sql
--(a, b, x)--
select *
from t
where optional_when({{a}}, optional_when({{b}}, c = {{x}}))
```

```json
{"a": true, "b": true, "x": 1}
```

```text
nested optional_when(...) is not supported
```

### Nested groups

```sql
--(pairs blob, pairs1 blob)--
select *
from t
where optional_groups_or(
  {{pairs}},
  optional_groups_and({{pairs1}}, id = {{self.id}})
)
```

```json
{}
```

```text
nested optional_groups_or(...)/optional_groups_and(...) is not supported
```

### is null or without parentheses

```sql
--(a)--
select *
from t
where {{a}} is null or a = 1
```

```json
{"a": 1}
```

```text
{{a}} is null or must be wrapped in parentheses in query.sql
```
