# Parameter header

The header is the only input model. It is the first significant token in a SQL file. Leading blank lines are fine. Bind with `{{...}}`. Schemas are the union of headers in the operation.

```sql
--($args.id integer, name! string)--
select *
from users u
where optional(u.user_id = {{$args.id}})
  and u.user_name = {{name}}
```

Where each prefix binds: [Parameter namespaces](../concepts/parameter-namespaces.md).

## Types

Scalar: `integer`, `string`, `float`, `bool`, `blob`.

Arrays: the same names plus `[]` (`integer[]`, `string[]`). One `{{param}}` expands to one placeholder per element. `[]` is a compile error on a required filter. Inside `optional(...)`, `null` and `[]` are omitted. Array types do not support defaults.

Each name needs a type. Duplicates and unknown types are errors. Trailing `!` means required: `--($args.id! integer)--`.

## Defaults

Scalar types only. Used when the caller omits the value:

```sql
--($args.sort string = id, $args.dir string = asc, $args.page integer = 1)--
```

| Type | Literal |
|---|---|
| `integer` | `-?\d+` |
| `float` | `-?\d+` or `-?\d+.\d+` |
| `bool` | `true` / `false` |
| `string` | `'...'` or a bare word (`= id`) |
| `blob` | not allowed |

`!` and `=` cannot be combined. Conflicting type, required, or default declarations across files fail at descriptor build. A defaulted parameter is not null: `optional(...)` keeps the filter, and `sort()` / `dir()` use the default.

## Comments and directives

Plain `--` line comments are allowed in query text. Yaal directives are only:

- `--(name type, ...)--`
- `--sql--`
- `--sql(connection)--`
