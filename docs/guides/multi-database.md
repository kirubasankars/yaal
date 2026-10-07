# Multi-database

**Problem:** one operation reads two databases and returns one JSON object.

## Layout

```text
user/combine/
  $.app.sql          # provider "db"
  $.flags.sql        # --sql(flags)--
  $.output.json
```

`$.flags.sql` begins with `--sql(flags)--` after its parameter header. That twig uses the provider named `flags`. Args are shared (`$args.id` is required in both files).

## Commands

`make example` registers both SQLite files and queries `user/combine`. From Python, after both databases are seeded (`docker/sqlite/schema.sql` and `docker/sqlite/flags_schema.sql`):

```python
y.setup_data_provider("db", "sqlite3:///" + app_db)
y.setup_data_provider("flags", "sqlite3:///" + flags_db)
y.query("user/combine", args={"id": 1})
```

Expect:

```json
{
  "app": { "id": 1, "name": "admin" },
  "flags": { "user_id": 1, "vip": 1 }
}
```

The ClickHouse experiment sandbox uses one database. `user/combine` still needs the flags file; use the SQLite experiment for that operation.

Provider setup: [Python](../appendix/python.md), [C#](../appendix/csharp.md).
