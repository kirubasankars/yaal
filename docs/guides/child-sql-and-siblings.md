<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Child SQL and siblings

**Problem:** either load a nested array from its own statement, or build one JSON object from several SQL files.

## Child SQL

`user/nested` returns the same JSON as `user/get`. Roles come from `$.roles.sql`. Both statements select `user_id`. The output property `roles` has `partition_by` and does **not** set `parent_rows`.

```bash
yaal query user/nested --arg id=1
```

## Siblings

`user/page` has no `$.sql`. `$.paging.sql` and `$.data.sql` become `paging` and `data`.

```bash
yaal query user/page --arg page=1 --arg page_size=1
```

## When to page before joining

`LIMIT` on a joined query truncates child rows. Page users in a subquery, then join roles, as `$.data.sql` does.

File naming: [Branches, twigs, and output](../concepts/branches-twigs-and-output.md). Layout reference: [Descriptor layout](../reference/descriptor-layout.md).
