# Yaal documentation

Yaal turns SQL + YAML descriptors into nested JSON. These docs use the shared fixtures under [`tests/fixtures/api/`](../tests/fixtures/api/) and the seed schema in [`docker/sqlite/schema.sql`](../docker/sqlite/schema.sql). Project overview / install: [`../README.md`](../README.md).

| Doc | Purpose |
|---|---|
| [learn.md](learn.md) | Step-by-step learning path (install → fixtures → your own API) |
| [examples.md](examples.md) | End-to-end examples: files, commands, sample JSON |
| [descriptors.md](descriptors.md) | Reference: trunk/branch/twig, parameters, shaping, [`$mode`](descriptors.md#mode-rows), precompile, database URLs, errors, public API |
| [why-sql-first.md](why-sql-first.md) | Why SQL-first fits ClickHouse-like engines and complex reporting apps |
| [`../python/README.md`](../python/README.md) | Python library layout (`python/src`, tests, examples) |
| [`../csharp/README.md`](../csharp/README.md) | .NET 8 port — same descriptors, Docker-based test/example tooling |

## Try it

```bash
make install
make example                 # Python: all fixtures + explain
make example-csharp          # same tour in .NET (Docker SDK)
make yaal ARGS='list'
make yaal ARGS='query user/get --arg id=1'
```

Runnable demos:

- Python: [`python/examples/demo.py`](../python/examples/demo.py)
- C#: [`csharp/examples/Yaal.Example/`](../csharp/examples/Yaal.Example/)

Fixture operations:

| Path | Shows |
|---|---|
| `user/get` | Nested object + `parent_rows` (one join SQL) |
| `user/nested` | Nested child SQL file (`$.roles.sql`) + `partition_by` |
| `user/list` | Root array + `optional()` filter + `sort()` / `dir()` |
| `user/page` | Sibling branches + `$mode=params` (multi-twig read) |
| `report/summary` | `WITH` + aggregations → JSON |
| `user/combine` | Multi-database (`--sql(flags)--`) |
