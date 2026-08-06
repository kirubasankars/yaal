# Why SQL-first fits ClickHouse-like engines and complex reporting

This is not a feature-by-feature comparison against any specific ORM. It is the reasoning behind why a **SQL-first, subtractive** tool (Yaal's model) tends to fit two related workloads better than a traditional **additive, entity-tracking** ORM: analytical/OLAP databases like ClickHouse, and complex reporting applications in general — which often run on ordinary Postgres/MySQL, not just OLAP engines, but share the same query *shape*.

## The mismatch

Traditional ORMs (ActiveRecord-style or otherwise) are built around OLTP assumptions:

- Row-level CRUD against entities with a stable identity
- Unit-of-work / change tracking (diff an object, emit an `UPDATE`)
- Foreign keys and unique constraints the ORM can rely on
- Migrations the ORM owns and applies

ClickHouse — and OLAP engines generally — don't work that way, and reporting queries don't need any of it either:

- Mutations are async/background (ClickHouse `ALTER TABLE ... UPDATE/DELETE` are mutations, not row-level statements); there's nothing for change-tracking to diff against
- No enforced foreign keys or unique constraints to hang relationship-mapping on
- Schema is `MergeTree`-family engines with an explicit `ORDER BY`/partition key that a general-purpose migration DSL doesn't model
- Queries lean on `WITH` CTEs, window functions, array functions, approximate aggregates (`uniq`, `quantile`), and dialect-specific clauses like `PREWHERE` and sampling
- Reporting queries — on *any* engine — lean on the same things: heavy `GROUP BY`/aggregation, ad hoc filters the user toggles on and off, sortable columns the user picks at request time, and results assembled from more than one data source

## Why that breaks additive ORMs in practice

A query builder models a query as an object graph assembled from entities and relations. That works well for `SELECT * FROM orders WHERE user_id = ?`. It stops paying for itself the moment you need:

- A window function or array function — most query builders don't expose it, so you drop to raw SQL/`.raw()` anyway
- `PREWHERE`, sampling, or another engine-specific clause — the ORM needs its own dialect for every engine it targets, and dialect support for newer or less common SQL always lags
- A handful of independent, optional filters and a couple of sortable columns — this usually means hand-written conditional branches that build up a query object piece by piece, one branch per filter/sort combination
- A second data source (an analytics replica, a flags DB, a warehouse) — most ORMs assume one connection/`DbContext` per model, so combining sources means stepping outside the ORM anyway

Once the abstraction is bypassed for the hard 20% of a report, you're paying its cost (mapping, change tracking, migrations) without getting the benefit where it matters.

## Why SQL-first fits

### ClickHouse-like engines

With Yaal you author the real, per-engine SQL directly — CTEs, window functions, `PREWHERE`, `MergeTree`-aware `ORDER BY` — because it is just SQL, not something a dialect layer needs to model. Yaal's only two cross-cutting jobs are engine-agnostic:

1. Subtract unused `optional(...)` predicates before running the statement
2. Shape the resulting flat rows into JSON

That elision is careful enough to also clean up the ClickHouse-specific clause: an elided filter that was the only predicate drops the empty `WHERE` *or* `PREWHERE`, not just `WHERE` (see [Optional filters](descriptors.md#optional-filters)). Because Yaal never owns schema or migrations, it composes cleanly with externally managed ClickHouse DDL such as [`docker/clickhouse/init.sql`](../docker/clickhouse/init.sql):

```sql
CREATE TABLE IF NOT EXISTS yaal.users (
    user_id   Int32,
    user_name String,
    active    UInt8
) ENGINE = MergeTree
ORDER BY user_id;
```

And a `WITH` CTE + aggregation ([`report/summary`](../tests/fixtures/api/report/summary/$.sql)) runs completely unmodified — Yaal never needed to understand it, only to bind its parameters and shape its output:

```sql
WITH role_counts AS (
    SELECT ur.user_id, COUNT(*) AS role_count
    FROM user_roles ur
    GROUP BY ur.user_id
)
SELECT
    COUNT(*) AS user_count,
    SUM(CASE WHEN u.active = 1 THEN 1 ELSE 0 END) AS active_count,
    COALESCE(SUM(rc.role_count), 0) AS assignment_count
FROM users u
LEFT JOIN role_counts rc ON rc.user_id = u.user_id
```

This same descriptor already runs against SQLite, Postgres, MySQL, and ClickHouse (see [Database URLs](descriptors.md#database-urls) and the [experiment sandbox](examples.md#experiment-sandbox)) without changes — there is no ClickHouse-specific code path to maintain, because there was never an ORM-side model of the query to begin with.

### Complex reporting apps, on any engine

Reporting and dashboard UIs are defined by a query *shape*, not a specific engine: ad hoc filters, sortable/dynamic columns, heavy aggregation, results from more than one source, and paginated counts. Yaal's primitives map onto that shape directly, in SQL, instead of through conditional query-builder branches:

- **Ad hoc filters** — `optional(...)` declares each toggleable predicate in place; the filters a caller omits are subtracted before the statement runs, so there's no branch-per-filter code path to maintain ([`user/list`](../tests/fixtures/api/user/list/$.sql)):

  ```sql
  where 1 = 1
    and optional(u.active = {{$args.active}})
  ```

- **Dynamic, multi-column sort** — `sort()` / `dir()` (with multi-column and `NULLS FIRST/LAST` support) let a caller pick sortable columns without ever binding a raw identifier as a value, which SQL doesn't allow:

  ```sql
  order by
    sort({{$args.sort}}, name = u.user_name, id = u.user_id)
    dir({{$args.dir}}),
    u.user_id asc
  ```

- **Paginated counts without a second round trip through the ORM** — `$mode=params` lets one twig's `COUNT(*)` feed the next twig's `page`/`page_size`/`total_count`, in one operation ([`user/page`](../tests/fixtures/api/user/page/$.paging.sql)):

  ```sql
  SELECT 'params' AS "$mode", COUNT(*) AS total_count FROM users WHERE active = 1
  --sql--
  SELECT {{$args.page}} AS page, {{$args.page_size}} AS page_size, {{$params.total_count}} AS total_count
  ```

- **Multi-source reports** — `--sql(name)--` twigs run against a second named connection (a flags DB, an analytics replica, a warehouse) and combine into one shape in a single operation ([`user/combine`](../tests/fixtures/api/user/combine/$.output.yaml)), instead of requiring a separate query outside the ORM's own `DbContext`/session.

None of this is ClickHouse-specific — it's the same argument for a Postgres or MySQL reporting schema. The report-shaped parts of an app (ad hoc filters, dynamic sort, aggregation, multi-source joins, pagination) are exactly where additive ORMs are weakest, because that shape doesn't map onto entities and relations in the first place.

## Where a traditional ORM is still the right call

This isn't "ORMs are bad." A simple OLTP CRUD app on Postgres/MySQL — transactional integrity, foreign-key-enforced relationships, migration-owned schema, and no ad hoc/report-style querying — is exactly what additive ORMs are designed for, and they do it well. The mismatch above is specifically about OLAP engines like ClickHouse and reporting-shaped workloads, where the query shape (ad hoc filters, dynamic sort, heavy aggregation, multi-source data, pagination) doesn't fit an entity/relation model regardless of which ORM you pick.

## See also

- [Optional filters](descriptors.md#optional-filters) and [Dynamic ORDER BY](descriptors.md#dynamic-order-by--sort--dir) — the mechanics behind `optional()` / `sort()` / `dir()`
- [`$mode` rows](descriptors.md#mode-rows) — the `params` mode behind paginated counts
- [Database URLs](descriptors.md#database-urls) — connecting to SQLite, Postgres, MySQL, and ClickHouse
- [Real SQL — `report/summary`](examples.md#real-sql--reportsummary), [Paginated nest — `user/page`](examples.md#paginated-nest--userpage), [Multi-database — `user/combine`](examples.md#multi-database--usercombine), and the [experiment sandbox](examples.md#experiment-sandbox)'s Compose ClickHouse walkthrough in [`docs/examples.md`](examples.md)
