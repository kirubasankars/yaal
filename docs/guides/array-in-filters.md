# Array IN filters

**Problem:** filter with `IN` when the client sends a list, and drop the clause when the list is missing or empty.

## SQL

```sql
--($args.id integer[])--
select u.user_id, u.user_name
from users u
where optional(u.user_id in ({{$args.id}}))
```

One `{{$args.id}}` expands to one placeholder per element: `in (?, ?)`.

## Explain

```bash
yaal explain user/list
```

`user/list` is a scalar optional, not an array. Use the SQL above in the [experiment sandbox](experiment-sandbox.md), or call `user/optional_multi` with `ids`.

| CLI | Compiled shape |
|---|---|
| no `id`, or `[]` | `WHERE` elided |
| `--arg 'id=[1,2]'` or `--args '{"id":[1,2]}'` | `where (u.user_id in (?, ?))` |

Do not nest the args object inside `--arg` (`--arg id='{"id":[1,2]}'` validates as an object, not a list).

A required (non-optional) empty list is a compile error: an `IN` list cannot be empty. Array types do not take header defaults.

Outside `optional(...)`, the same expansion applies whenever the array is present.

Grammar: [Parameter header](../reference/parameter-header.md). Absence rules: [Compile and elision](../concepts/compile-and-elision.md).
