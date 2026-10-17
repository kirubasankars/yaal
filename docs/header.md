# Parameter header

The header is the first significant token. Only comments may precede it, and a second header is an error. It declares every argument.

```sql
--(id!, name, page)--
select {{id}}, {{name}}, {{page}}
```

Names are bare identifiers. There is no `$args` prefix and no scalar type. `blob` is the only marker, and it is required for an `optional_groups_*` source. The bind `type` is taken from the JSON value: bool, integer, float, or string.

`!` means the caller must supply a value. There is no default. Explicit `null` stays absent.

A name is one shape in the whole string, including after `--sql--`. `{{id}}` is one value. `{{id}}...` is a list. The same name cannot be both.

Omitted and `null` are absent. `0`, `""`, and `false` are present. `[]` is absent for `{{name}}...` and for a `blob`. Inside `optional(...)`, an absent list drops the block. Outside it, `[]` is an error. A required name that is absent is an error, so `id!` does not elide inside `optional(...)`.
