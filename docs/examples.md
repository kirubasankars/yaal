# Examples

These are the fixture queries. Each block is the SQL, the parameter object, and the compiled statement.

## Optional filter and sort

From `tests/fixtures/sql/list.sql`.

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

From `optional_sugar_non_null_keeps_binds` and `optional_sugar_null_strips_and`.

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

From `optional_multi_both_bound`, `optional_multi_all_null_elides`, and `optional_multi_partial_null_errors`.

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

From `array_in_three_elements` and `optional_array_elide_null`.

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

From `optional_when_cond_and_body_present_keeps` and `optional_when_cond_absent_elides`.

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

## Group rows

From `tests/fixtures/sql/groups.sql`.

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

`optional_groups_or` with two rows, from `optional_groups_two_scalar_rows`:

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

`optional_groups_and` with two rows, from `optional_groups_and_two_rows`:

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

From `experimental_groups_with_order_by_sort`.

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

These fixture inputs do not compile.

### Partial optional

From `optional_multi_partial_null_errors`. `a` is omitted and `b` is present.

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

From `array_in_empty_errors`.

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

From `optional_groups_empty_in_errors`.

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

From `optional_groups_source_not_blob_errors`.

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

From `header_junk_errors`.

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

### is null or without parentheses

From `paren_less_is_null_or_errors`.

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
