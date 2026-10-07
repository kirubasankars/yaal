# Optional groups DSL

Why groups elide independently of a wrapping `optional`: [Optional semantics](../concepts/optional-semantics.md).

## Form

```sql
optional_groups_or({{blob}}, body)
optional_groups_and({{blob}}, body)
```

The first argument is one `{{blob}}` parameter declared `blob`. `optional_groups_or` joins row copies with `OR`. `optional_groups_and` joins them with `AND`. Omit, null, `[]`, `""`, or `"[]"` removes the clause. The keywords share every other rule.

`optional_groups(...)` with no `_or` / `_and` is a compile error.

Nested `optional_groups_*` inside `optional_groups_*` is not supported.

## Compiled shape

```sql
--($args.pairs blob)--
where optional_groups_or(
  {{$args.pairs}},
  col2 in ({{cv}}) and col1 = {{cv1}}
)
```

Two rows become `((col2 in (?, ?, ?) and col1 = ?) or (col2 in (?) and col1 = ?))`. Each row is parenthesized. Two or more rows are wrapped again so a neighboring `AND` does not bind tighter than the group. A single-row group is not wrapped a second time.

## Row values

| Blob field value | Placeholders |
|---|---|
| scalar | one `?` |
| non-empty JSON array of scalars | `?, ?, …` |
| `[]` | compile error (empty `IN`) |
| nested object or array | compile error |
| missing key or null | compile error |

Bare placeholders are keys in each row object (case-insensitive). They must not be declared as bare header names.

The blob may be a list of objects, a JSON string, or UTF-8 JSON bytes. A bare object, a scalar, or malformed JSON is rejected. `blob` models as an array of objects, so lists are not coerced to strings.

## `$args` in the body

Bare names are row fields. `$args.` names bind from runtime args, must be declared, and repeat in every branch.

Compile errors:

- the blob parameter used in its own body
- a header array parameter in the body (put the list on the row)
- `{{$args.x}}` not declared
- a body whose placeholders are all `$args.` names (nothing to iterate)

An `optional(...)` in the body must reference at least one `{{$args.*}}`. An `optional_when` condition in the body must be `{{$args.*}}`.

Fixture: `user/groups`. Mixed with `optional`: `user/groups_in_optional`.

CLI: `--arg 'pairs=[{"id":1}]'` or `--args '{"pairs":[{"id":1}]}'`.
