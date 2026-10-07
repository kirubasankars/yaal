# Child SQL

**Goal:** the same JSON as `user/get`, with roles loaded from `$.roles.sql` instead of a join plus `parent_rows`.

## Commands

```bash
yaal query user/nested --arg id=1
yaal query user/get --arg id=1
```

## Files to open

- [tests/fixtures/api/user/nested/$.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/nested/$.sql)
- [tests/fixtures/api/user/nested/$.roles.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/nested/$.roles.sql)
- [tests/fixtures/api/user/nested/$.output.json](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/nested/$.output.json)

## Compare

| Approach | Files | Nesting |
|---|---|---|
| `user/get` | one join in `$.sql` | `parent_rows: true` |
| `user/nested` | `$.sql` + `$.roles.sql` | `partition_by` on the join key in both result sets |

Parent and child both return `user_id`. The parent’s `partition_by` stitches child rows onto matching parents. The `roles` property has no `parent_rows`.

## Exercise

Copy `user/get` into the experiment sandbox and split roles into `$.roles.sql` the way `user/nested` does. Query both and confirm the JSON shape matches.

Task write-up: [Child SQL and siblings](../guides/child-sql-and-siblings.md).

Next: [Pagination](07-pagination.md).
