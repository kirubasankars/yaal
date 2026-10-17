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

The body may contain `optional(...)`, `optional_groups_or(...)`, and `optional_groups_and(...)`. An absent condition removes that whole body before the inner blocks are checked.

```sql
--(apply, active, pairs blob)--
where 1 = 1
  and optional_when({{apply}},
    optional(u.active = {{active}})
    and optional_groups_or({{pairs}}, u.id = {{self.id}})
  )
```

`{"apply": true, "active": 1, "pairs": [{"id": 1}, {"id": 2}]}` keeps the comparison and both group rows. `{}` removes the block. `{"apply": true}` removes it too. `{"apply": true, "pairs": [{"id": 1}]}` also removes it: `active` is a body parameter, and omitting it drops the `optional_when` block along with the group.

Header names in that body stay all-or-nothing together. `optional_when({{apply}}, optional(a = {{a}}) and optional(b = {{b}}))` with only `a` is the partial-parameters error. Omitting `apply` removes the block.

A body that is only a group follows the group. `optional_groups_and` joins those rows with `and`.

```sql
--(apply, pairs blob)--
and optional_when({{apply}}, optional_groups_and({{pairs}}, u.id = {{self.id}}))
```

`optional_when` inside `optional_when` fails with `nested optional_when(...) is not supported`. A group inside a group fails with `nested optional_groups_or(...)/optional_groups_and(...) is not supported`.

A group body may contain `optional(...)` and `optional_when(...)`. The `optional_when` condition is a header argument. Each rendered row still supplies `{{self.field}}`.

An empty `where`, `prewhere`, or `having`, including one left as `1 = 1`, is removed.

The long form is the same rule: `({{id}} is null or u.id = {{id}})`.

`{{name}}...` expands to one placeholder per element:

```sql
--(id)--
where optional(u.id in ({{id}}...))
```
