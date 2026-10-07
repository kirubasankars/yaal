# Experiment sandbox

**Problem:** edit descriptors without changing `tests/fixtures/api`.

`experiment/` is gitignored. `make experiment` copies the fixtures on first use, seeds SQLite, and runs the CLI with `--debug`.

## Commands

```bash
make experiment-init
make experiment
make experiment ARGS='query user/page --arg page=1 --arg page_size=10'
make experiment-reset
make experiment-clean

make experiment-clickhouse-init
make experiment-clickhouse
make experiment-clickhouse ARGS='query user/list --arg sort=name'
make experiment-clickhouse-reset
```

`experiment-reset` reseeds SQLite and keeps API edits. `experiment-clean` deletes the directory. The ClickHouse targets share `experiment/api/` and point `--db` at Compose (`clickhouse://yaal:yaal@127.0.0.1:9000/yaal`). Seed SQL: `docker/clickhouse/experiment_seed.sql`.

`user/combine` needs a second flags database. Use the SQLite experiment for that, not the ClickHouse one.

## Your own tree

```text
my-api/
  orders/
    list/
      $.sql
      $.output.json
```

```bash
yaal query orders/list \
  --api ./my-api \
  --db 'sqlite3:////tmp/app.db' \
  --args '{"status":"open"}'
```

Checklist:

1. At least one `*.sql`.
2. A parameter header for every `{{...}}` you bind from the caller.
3. `$.output.json` with `mapped` columns.
4. `yaal explain` before `yaal query`.
