<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# sort and dir

Identifiers cannot be bound as values. `sort()` and `dir()` splice author-written expressions only.

```sql
--($args.sort string, $args.dir string)--
order by
  sort({{$args.sort}}, name = u.user_name, id = u.user_id)
  dir({{$args.dir}})
```

| Construct | Behavior |
|---|---|
| `sort({{param}}, key = expr, …)` | Resolve `param` to one or more declared keys (case-insensitive, comma-separated). Splice the matching author expressions. Unknown non-null key → soft `{"errors":[…]}`. |
| `dir({{param}})` | Positional list matched to `sort` keys. Allowed: `asc`, `desc`, `asc_nulls_first`, `asc_nulls_last`, `desc_nulls_first`, `desc_nulls_last`. Missing trailing entries default to `asc`. Too many entries or an unknown value → soft error. |
| Null or omitted `sort`, no header default | Elide the dynamic term. `dir` is ignored. |
| Header default on `sort` or `dir` | Omitted args use the default instead of eliding. |

## Multi-column

`sort` and `dir` are comma-separated lists, matched by position. Keys must be declared and not repeated. `dir` may be shorter than `sort` (missing entries are `asc`) but not longer.

```bash
yaal query user/list --arg sort=name,id --arg dir=desc,asc
```

resolves to `u.user_name DESC, u.user_id ASC` (plus any static terms).

## NULLS FIRST / LAST

```bash
yaal query user/list --arg sort=name --arg dir=desc_nulls_last
```

emits `ORDER BY u.user_name DESC NULLS LAST`. MySQL rejects that syntax at the engine. It is not a Yaal soft error.

## Static terms and multiple pairs

At most one dynamic term (`sort()` plus an immediately following `dir()`) per `ORDER BY`. Static terms may sit before or after it. When `sort` is null and has no default, only the dynamic term elides. The whole clause elides only when nothing remains.

A statement may contain more than one `ORDER BY` (a subquery and an outer query). Each pair must use a distinct `{{param}}`. Reusing a param is a parse error.

Keys must match a word (`\w+`). Empty or whitespace-only sort keys are soft errors, not null. `sort` / `dir` outside `ORDER BY` parse and are the author’s responsibility.

Only descriptor expressions and the fixed direction vocabulary are spliced. Client text never becomes SQL.
