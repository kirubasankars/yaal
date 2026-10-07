# CLI

Entry point: `yaal` (same as `python -m yaal_cli`). `make yaal ARGS='...'` forwards arguments.

## Global options

| Option | Meaning |
|---|---|
| `--api PATH` | Descriptor root. Default: `tests/fixtures/api`. |
| `--db URL` | Database URL. If omitted, a temp SQLite database is seeded from `--schema`. |
| `--provider NAME` | Provider name. Default: `db`. |
| `--schema PATH` | SQLite schema used when `--db` is omitted. Default: `docker/sqlite/schema.sql`. |
| `--debug` | Reload SQL and JSON every call. Ignores `--precompiled`. |
| `--precompiled DIR` | Load artifacts from `yaal compile`. |

## Commands

| Command | Purpose |
|---|---|
| `list` | Print descriptor paths under `--api`. |
| `compile --out DIR` | Write JSON artifacts. No database. |
| `query PATH` | Run the operation and print nested JSON. |
| `explain PATH` | Print compiled SQL and `binds:` per twig. No database execute. |

`query` and `explain` take:

| Option | Meaning |
|---|---|
| `--arg KEY=VALUE` | Repeatable. Values parse as JSON when they can (`1`, `true`, `[1,2]`). |
| `--args JSON` | One JSON object merged with `--arg`. |
| `--payload JSON` | Payload object for bare header names. |

```bash
yaal query user/get --arg id=1
yaal explain user/list --arg active=1 --arg sort=name --arg dir=desc
yaal query user/groups --arg 'pairs=[{"id":1}]'
yaal query user/create --db 'sqlite3:////tmp/app.db' --payload '{"id":99,"name":"newbie"}'
yaal --api tests/fixtures/api compile --out /tmp/yaal-precompiled
```

Do not wrap an array arg in an extra object (`--arg id='{"id":[1,2]}'`).

URL syntax: [Database URLs](database-urls.md).
