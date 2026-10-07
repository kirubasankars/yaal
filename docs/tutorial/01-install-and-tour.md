# Install and tour

**Goal:** install Yaal and run the shared fixture tour.

## Commands

```bash
make install
make example
make yaal ARGS='list'
make yaal ARGS='query user/get --arg id=1'
```

Optional .NET tour (Docker SDK, no local `dotnet` required):

```bash
make example-csharp
```

When `--db` is omitted, the CLI seeds a temp SQLite database from `docker/sqlite/schema.sql` and uses `tests/fixtures/api`.

## Files to open

- [python/examples/demo.py](https://github.com/kirubasankars/yaal/blob/main/python/examples/demo.py) — what `make example` runs
- [docker/sqlite/schema.sql](https://github.com/kirubasankars/yaal/blob/main/docker/sqlite/schema.sql) — two users, two roles

## What you should see

`yaal list` prints operation paths such as `user/get`, `user/list`, and `user/page`. `query user/get --arg id=1` prints one user object with a nested `roles` array.

## Exercise

Run `make example` and find the `user/list` explain output. Note whether the `active` predicate is present.

Next: [Mental model](02-mental-model.md).
