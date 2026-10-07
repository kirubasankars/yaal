<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Subtractive ORM

Additive ORMs start from models and **add** SQL: a filter object, a join method, a sort clause, assembled in host code. Yaal starts from a full statement and **subtracts** the parts the caller did not supply.

```sql
where 1 = 1
  and optional(u.active = {{$args.active}})
```

| Args | Compiled predicate |
|---|---|
| omitted | predicate removed; `where 1 = 1` cleaned away |
| `active=1` | `and (u.active = ?)` with bind `[1]` |

That is the whole product shape:

1. Author SQL you would be willing to run by hand.
2. Mark the predicates, sorts, and groups that are request-scoped.
3. Let bind-time compilation drop what is absent.
4. Shape whatever rows come back.

Yaal does not track entities, own migrations, or expose a query-builder DSL. `WITH`, window functions, and engine clauses such as ClickHouse `PREWHERE` stay in the file because nothing has to translate them.

The same idea is why a reporting UI (toggles, sorts, paging, more than one database) maps cleanly onto descriptors. The longer argument is [Why SQL-first](../essays/why-sql-first.md). The cost of walking tokens to subtract filters, compared with appending strings, is [StringBuilder vs optional](../essays/stringbuilder-vs-optional.md).

See fixture `user/list`.
