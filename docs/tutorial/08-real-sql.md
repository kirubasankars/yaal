<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Real SQL

**Goal:** see that CTEs and aggregates stay ordinary SQL.

## Commands

```bash
yaal query report/summary
```

## Files to open

- [tests/fixtures/api/report/summary/$.sql](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/report/summary/$.sql)
- [tests/fixtures/api/report/summary/$.output.json](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/report/summary/$.output.json)

The statement uses `WITH role_counts AS (...)` and `COUNT` / `SUM`. Yaal does not rewrite that SQL. It binds parameters (none here) and maps columns:

```json
{
  "user_count": 2,
  "active_count": 2,
  "assignment_count": 3
}
```

Why this shape fits reporting workloads: [Why SQL-first](../essays/why-sql-first.md).

## Exercise

Read `$.sql` and name the three selected columns. Match each one to a key in the JSON you just printed.

Next: [Precompile](10-precompile.md).
