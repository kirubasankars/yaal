<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Subtractive filters

**Goal:** see an optional predicate disappear when its argument is absent, and stay when the argument is present.

## Commands

```bash
yaal explain user/list
yaal explain user/list --arg active=1
yaal query user/list --arg active=1
yaal explain user/groups --arg 'pairs=[{"id":1}]'
```

## Files to open

- [tests/fixtures/api/user/list/$.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/list/$.sql)

The interesting fragment:

```sql
--($args.active integer, $args.sort string = id, $args.dir string = asc)--
...
and optional(u.active = {{$args.active}})
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}})
```

## What happens

| Call | Result |
|---|---|
| no `active` | filter removed; binds `[]`; leading `where 1 = 1` cleaned away |
| `active=1` | `(u.active = ?)` with bind `[1]` |
| no `sort` / `dir` | header defaults → `ORDER BY u.user_id ASC` plus the static tiebreaker |
| `sort=name` / `dir=desc` | splices the allowlisted expression `u.user_name DESC` |

When the only predicate in a `WHERE`, `PREWHERE`, or `HAVING` is elided, the bare clause is removed. There is no leftover `1 = 1` and no empty `HAVING`.

Two related patterns, covered in the guides rather than here:

- **Optional `IN`:** declare `integer[]` and write `optional(col in ({{$args.ids}}))`. Omit or `[]` drops the block. See [Array IN filters](../guides/array-in-filters.md).
- **Optional groups:** a `blob` of row objects repeats one predicate. See [Optional groups](../guides/optional-groups.md).

Behavior of several parameters in one `optional(...)` is in [Optional semantics](../concepts/optional-semantics.md).

## Exercise

Change nothing in the repo. Run explain for `user/list` twice: once with no args, once with `--arg active=1 --arg sort=name --arg dir=desc`. Write down the `ORDER BY` you see in each case.

Next: [Output shaping](05-output-shaping.md).
