<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Fixture index

Every operation under `tests/fixtures/api/`. Seed: `docker/sqlite/schema.sql` (two users, two roles).

```bash
make example
make yaal ARGS='list'
```

| Path | Shows | Read more |
|---|---|---|
| `user/get` | Join + `parent_rows` | [Nested get](../tutorial/03-nested-get.md) |
| `user/nested` | Child `$.roles.sql` | [Child SQL](../tutorial/06-child-sql.md) |
| `user/list` | `optional`, `sort`, `dir` | [Subtractive filters](../tutorial/04-subtractive-filters.md) |
| `user/optional_multi` | All-or-nothing multi-param `optional` | [Optional semantics](../concepts/optional-semantics.md) |
| `user/groups` | `optional_groups_or` + `blob` | [Optional groups](../guides/optional-groups.md) |
| `user/groups_in_optional` | Group nested in `optional` | [Optional semantics](../concepts/optional-semantics.md) |
| `user/when_optional` | `optional_when` | [Optional DSL](../reference/optional-dsl.md) |
| `user/page` | Siblings + `$mode=params` | [Pagination](../guides/pagination-and-mode-params.md) |
| `report/summary` | `WITH` and aggregates | [Real SQL](../tutorial/08-real-sql.md) |
| `user/create` | Insert twigs + payload | [Writes and modes](../guides/writes-and-modes.md) |

Compile goldens (no database): `tests/fixtures/sql_compile/`.

Most fixtures are read-only. `user/create` writes and is not part of `make example`.
