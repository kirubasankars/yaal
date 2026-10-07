<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Shaping strategies

Flat rows become nested JSON in one of four ways. Pick the one that matches where the rows come from.

| Strategy | Rows come from | Use when |
|---|---|---|
| `mapped` only | one row, or a list of flat rows | no nesting |
| `partition_by` + `parent_rows` | one joined result set | children are columns on the parent query |
| child SQL + parent `partition_by` | two result sets | the child is its own statement |
| sibling files | two or more branches | the JSON object is several independent queries (`paging` and `data`) |

## `mapped`

`"id": { "mapped": "user_id" }` copies column `user_id` to JSON `id`. Root `type: object` returns one object (the first shaped row after partitioning). Root `type: array` returns a list.

## Join fan-out (`user/get`)

One SQL file joins users to roles. Two roles produce two rows for the same user. `partition_by: user_id` on the root keeps one user. `roles` is `type: array`, `partition_by: role_id`, `parent_rows: true`. Those role columns are read from the parent rows. There is no `$.roles.sql`.

The parent must set `partition_by` when a child uses `parent_rows`.

If you also `LIMIT` the joined query, the limit cuts **join rows**, so a user can lose roles. Page the parent entity in a subquery first, then join. `user/page` does this.

## Child SQL (`user/nested`)

`$.roles.sql` returns role rows that include `user_id`. The parent’s `partition_by: user_id` stitches them on. The `roles` property does **not** set `parent_rows`. Both queries must return the join key.

## Siblings (`user/page`)

No trunk file. `$.paging.sql` and `$.data.sql` become properties `paging` and `data`. Nesting inside `data` can still use `parent_rows`. The paging file uses `$mode=params` so a count is available to a later twig **in that same file**. It does not automatically appear in `$.data.sql`.

## Invalid shapes

A bare `type: object` or `type: array` directly under `properties`, including an item wrapper around array elements, is rejected. The branch’s own `type` already says object or array. Fields under it are a flat `mapped` map, plus named nested branches.

`$mode=json` skips shaping for that branch and uses the engine’s JSON column instead. See [Mode rows](../reference/mode-rows.md).
