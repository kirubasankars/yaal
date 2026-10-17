# optional groups

`optional_groups_or` joins one copy of the body per blob row with `or`. `optional_groups_and` joins with `and`. The blob argument is declared `blob` and is a JSON array of objects. `[]` removes the group.

```sql
--(pairs blob, active)--
where optional_groups_or({{pairs}},
  u.id = {{self.id}} and u.active = {{active}}
)
```

`{{self.field}}` reads the current row. `{{name}}` is still an argument from the header. `{{self.field}}` outside a group is an error. A row field cannot gate `optional(...)` or `optional_when(...)`.

The body may contain `optional(...)` and `optional_when({{flag}}, ...)`. `optional_when` may also wrap the whole group. See [Nested](nested.md).

`{{self.ids}}...` expands a list stored on that row. A missing field, `null`, or `[]` on a rendered row is an error. The compiler does not skip that row.

Nested groups are rejected. The blob placeholder cannot be written with `...` and cannot appear in its own body.
