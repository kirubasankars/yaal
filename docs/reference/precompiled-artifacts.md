# Precompiled artifacts

```bash
yaal --api path/to/api compile --out path/to/precompiled
```

One JSON file per path: `user/get.json`. Alternate mappers: `user/get#summary.json`.

Tokens are compacted at compile time: adjacent whitespace and static SQL are merged. Parameters, braces, and `sort()` / `dir()` stay structural. Optional-filter elision still runs per request. Twigs without `sort()` / `dir()` store `has_sort_dir: false` and skip sort resolution.

`debug` forces live SQL and JSON and ignores the precompiled directory.

## Load order

When debug is off:

1. Registered descriptor (C# `RegisterDescriptor`)
2. Memory cache (`clear_cache` / `ClearCache` clears this only)
3. Precompiled JSON directory
4. Live SQL and JSON on disk

## Performance notes

- Providers drain cursors with `fetchmany` into a per-branch row list. `partition_by` still buffers that branch.
- Compiled SQL is cached per twig, nulls set, and placeholder for one trunk execution.
- Nested stitching uses shallow copies where that is safe (`parent_rows`, `partition_by`, single-parent branches).
- Postgres and MySQL URL query knobs: `pool_size` (Postgres also `minconn` / `maxconn`). Defaults: Postgres max 20, MySQL 10.
- C# uses driver pooling (Npgsql / MySqlConnector). Pass `pooling` and pool size in the URL query string.

How to run it: [Precompile guide](../guides/precompile.md).
