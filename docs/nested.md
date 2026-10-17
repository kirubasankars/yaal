# Nested

`optional_when(...)` can wrap `optional(...)`, `optional_groups_or(...)`, and `optional_groups_and(...)`. `optional(...)` can wrap a group. A group body can contain `optional(...)` and `optional_when(...)`.

Worked inputs are on the [examples](docs/examples.md) page.

## optional_when around optional and groups

An absent condition removes the whole body before the inner blocks are checked.

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

`{"apply": true, "pairs": [{"id": 1}, {"id": 2}]}` compiles to `where z = 1 and (((id = ?) and (id = ?)))` when that predicate is the filter. Omitting `apply` leaves the surrounding predicate and does not render the rows.

## optional around a group

`optional(...)` can wrap `optional_groups_or` or `optional_groups_and`. An empty or omitted blob removes the wrapper and a preceding `and` or `or`.

```sql
--(pairs blob, x)--
where z = 1
  and optional(
    a = {{x}}
    and optional_groups_or({{pairs}}, id = {{self.id}})
  )
```

`{"x": 1, "pairs": [{"id": 1}, {"id": 1}]}` keeps `a = ?` and both rows. `{"pairs": null}` removes the block. `{"x": 1, "pairs": null}` keeps `a = ?` and drops the group.

## Inside a group

`optional(...)` and `optional_when({{flag}}, ...)` may sit in the group body. `flag` is a header argument, so one value applies to every row. Omitting it removes that inner block from each row and leaves the rest of the body.

```sql
--(pairs blob, flag)--
where optional_groups_or({{pairs}},
  c1 = {{self.cv}}
  and optional_when({{flag}}, c2 = {{self.cv2}})
)
```

`{"flag": true, "pairs": [{"cv": 1, "cv2": 1}, {"cv": 1, "cv2": 1}]}` keeps `c2` on both rows. Omitting `flag` leaves `(c1 = ?) or (c1 = ?)`.

`{{self.field}}` cannot be the `optional_when` condition, and it cannot be the only parameter of `optional(...)` inside a group. Each rendered row still supplies `{{self.field}}`.

## Rejected nesting

`optional_when` inside `optional_when` fails with `nested optional_when(...) is not supported`.

```sql
--(a, b, x)--
where optional_when({{a}}, optional_when({{b}}, c = {{x}}))
```

A group inside a group fails with `nested optional_groups_or(...)/optional_groups_and(...) is not supported`.

```sql
--(pairs blob, pairs1 blob)--
where optional_groups_or({{pairs}},
  optional_groups_and({{pairs1}}, id = {{self.id}})
)
```
