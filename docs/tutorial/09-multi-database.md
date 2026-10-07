# Multi-database

**Goal:** one operation, two named providers.

`make example` wires this for you: an app database and a flags database. From the CLI, a single `--db` is the default provider `"db"`. A second provider is set up in application code. See [Multi-database](../guides/multi-database.md) and the [Python appendix](../appendix/python.md).

## Files to open

- [tests/fixtures/api/user/combine/$.app.sql](https://github.com/kirubasankars/yaal/blob/main/tests/fixtures/api/user/combine/$.app.sql)
- [tests/fixtures/api/user/combine/$.flags.sql](https://github.com/kirubasankars/yaal/blob/main/tests/fixtures/api/user/combine/$.flags.sql)
- [docker/sqlite/flags_schema.sql](https://github.com/kirubasankars/yaal/blob/main/docker/sqlite/flags_schema.sql)

## What the files do

- `$.app.sql` uses the default connection `"db"`.
- `$.flags.sql` starts with `--sql(flags)--`, so that twig uses provider `"flags"`.
- `$.output.json` shapes `{ app, flags }`.

For `id=1` the combined object is:

```json
{
  "app": { "id": 1, "name": "admin" },
  "flags": { "user_id": 1, "vip": 1 }
}
```

## Exercise

Run `make example` and find the `user/combine` JSON. Confirm `app.name` and `flags.vip` both come back in one result.

Next: [Precompile](10-precompile.md).
