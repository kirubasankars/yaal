# optional

`optional(...)` can hold several parameters. The block stays when every one of them is present, and it is removed, along with a preceding `and` or `or`, when every one of them is omitted or null. Supplying only some of them is an error: `optional(...) requires every parameter to be provided or all omitted`.

```sql
--(active, name)--
where 1 = 1
  and optional(u.active = {{active}} and u.name = {{name}})
```

`{"active": 1, "name": "ada"}` keeps both comparisons. `{}` removes the block. `{"active": 1}` is rejected. `0`, `""`, and `false` count as present.

Separate `optional(...)` calls are independent, so one can stay while another is removed.

```sql
--(active, name)--
where 1 = 1
  and optional(u.active = {{active}})
  and optional(u.name = {{name}})
```

`optional_when({{flag}}, body)` removes the block when `flag` is absent. The condition is not emitted and is not bound. `false` is present, so the block stays. The condition cannot be written with `...`.

```sql
--(apply, id)--
and optional_when({{apply}}, u.id = {{id}})
```

The body may contain `optional(...)`, `optional_groups_or(...)`, and `optional_groups_and(...)`. A group body may contain `optional(...)` and `optional_when(...)`. The nesting rules are on [Nested](docs/nested.md).

An empty `where`, `prewhere`, or `having`, including one left as `1 = 1`, is removed.

The long form is the same rule: `({{id}} is null or u.id = {{id}})`.

`{{name}}...` expands to one placeholder per element:

```sql
--(id)--
where optional(u.id in ({{id}}...))
```
