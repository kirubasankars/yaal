# Dynamic ORDER BY

**Problem:** let the client choose sort columns and direction without splicing their text into SQL.

## SQL

```sql
--($args.sort string = id, $args.dir string = asc)--
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}}),
  u.user_id asc
```

Fixture: `user/list`. Only expressions written next to `sort(...)` can appear. Unknown keys become a soft `{"errors":[...]}`.

## Explain

```bash
yaal explain user/list
yaal explain user/list --arg sort=name --arg dir=desc
yaal query user/list --arg sort=name,id --arg dir=desc,asc
```

| Call | `ORDER BY` |
|---|---|
| defaults | `u.user_id ASC, u.user_id asc` |
| `sort=name`, `dir=desc` | `u.user_name DESC, u.user_id asc` |
| `sort=name,id`, `dir=desc,asc` | `u.user_name DESC, u.user_id ASC, u.user_id asc` |

The static `u.user_id asc` stays when the dynamic term elides. The whole `ORDER BY` drops only when the dynamic term is the only term and `sort` is null with no default.

`dir=desc_nulls_last` emits `DESC NULLS LAST`. MySQL has no `NULLS FIRST/LAST` syntax; the engine errors, not Yaal.

Two `ORDER BY` clauses in one statement need two different `{{param}}` names.

Grammar: [sort and dir](../reference/sort-and-dir.md).
