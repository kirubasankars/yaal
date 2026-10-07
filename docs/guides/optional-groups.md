<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Optional groups

**Problem:** repeat one predicate for each object in a JSON array, OR-joined (any row matches) or AND-joined (every row matches). Omit the array and the clause disappears.

## SQL

```sql
--($args.pairs blob)--
select * from (select 1 as id union select 2) t
where optional_groups_or({{$args.pairs}}, id = {{id}})
```

Fixture: [tests/fixtures/api/user/groups/$.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/groups/$.sql).

`{{id}}` is a key on each row object. It is not a header parameter. Swap the keyword to `optional_groups_and` when every row must match.

A shared arg in every branch is a declared `$args` name:

```sql
--($args.pairs blob, $args.flag integer)--
where optional_groups_or(
  {{$args.pairs}},
  col1 = {{cv1}} and col2 = {{$args.flag}}
)
```

## Explain

```bash
yaal query user/groups
yaal explain user/groups --arg 'pairs=[{"id":1},{"id":2}]'
```

| Call | Expect |
|---|---|
| no `pairs` | filter gone; both rows |
| two rows | `where ((id = ?) or (id = ?))` binds `[1, 2]` |

There is no bare `optional_groups(...)` keyword. That spelling is a compile error that names the two replacements.

Per-row `IN` puts an array on the row (`{"ids":[1,2]}`), not in the header. Header `integer[]` inside a group body is rejected.

Nesting with `optional(...)`: [Optional semantics](../concepts/optional-semantics.md). Full body rules: [Optional groups DSL](../reference/optional-groups-dsl.md).
