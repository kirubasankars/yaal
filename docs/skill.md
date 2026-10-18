# Skill

Follow this when you write Yaal SQL or call the compiler. Write one statement. Do not concatenate fragments, and do not emit `(? is null or col = ?)`. Values stay in the JSON object. [Plans](plans.md) is why.

The header comes first and names every argument. Only comments may precede it. `{{name}}` compiles to a bind. `0`, `""`, and `false` are present. Omitted and `null` are absent.

Use `optional(...)` when a filter may be missing. Use `optional_when({{flag}}, ...)` when a separate flag gates a clause. The flag is not bound, and `false` keeps the body. Use `optional_groups_or` or `optional_groups_and` when one body repeats for each row. `{{self.field}}` is valid only inside a group.

Use `sort()` and `dir()` for ordering. Never splice caller text into `ORDER BY`.

Choose `?` or numbered `$1`. A literal `?` is an error when the placeholder is `?`. Use numbered placeholders when the SQL contains the PostgreSQL `?` operator.

Call the compiler, then execute the returned SQL with the returned binds, in order.

```bash
yaal compile --sql query.sql --params params.json
```

The result is `statements` of `sql` plus `parameters` (`name`, `type`, `value`). A failure is an error string, not a statement.

These inputs fail the compile:

- A name used as both `{{id}}` and `{{id}}...`.
- `id!` inside `optional(...)`. A required name that is absent is an error.
- A partial `optional(...)`. Every parameter in the block is present, or all of them are omitted.
- Nested `optional_when(...)`.
- Nested `optional_groups_or` or `optional_groups_and`.

The forms are on [optional](optional.md), [optional groups](groups.md), [Nested](nested.md), [sort and dir](sort.md), and [Safety](safety.md). Try a statement in the [playground](playground.md).
