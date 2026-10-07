<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Tutorial

This path walks the shared fixtures. The examples are read-only except where a later guide says otherwise. Each step has a goal, commands, files to open, and one exercise.

| Step | You learn | Page |
|---|---|---|
| 1 | Install and run the tour | [Install and tour](01-install-and-tour.md) |
| 2 | How a call moves through Yaal | [Mental model](02-mental-model.md) |
| 3 | One get, nested JSON | [Nested get](03-nested-get.md) |
| 4 | Filters that disappear | [Subtractive filters](04-subtractive-filters.md) |
| 5 | `mapped`, `partition_by`, `parent_rows` | [Output shaping](05-output-shaping.md) |
| 6 | Roles from a second SQL file | [Child SQL](06-child-sql.md) |
| 7 | Paging with `$mode=params` | [Pagination](07-pagination.md) |
| 8 | CTEs and aggregates | [Real SQL](08-real-sql.md) |
| 9 | Compile once, elide per request | [Precompile](10-precompile.md) |

After the tour, edit a copy of the fixtures in the [experiment sandbox](../guides/experiment-sandbox.md).

## Practice path

1. Run `make example` and read the printed JSON.
2. Re-run `yaal explain user/list` with and without `--arg active=1`.
3. In a sandbox copy, map one more column on `user/get`.
4. Copy `user/get` and switch roles to a child SQL file like `user/nested`.
5. Sketch a paged list using `$mode=params` like `user/page`.
6. Point `make experiment` at those edits until the shape looks right.
