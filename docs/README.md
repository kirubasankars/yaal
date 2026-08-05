# Yaal documentation

Yaal turns SQL + YAML descriptors into nested JSON. These docs use the shared fixtures under [`tests/fixtures/api/`](../tests/fixtures/api/) and the seed schema in [`docker/sqlite/schema.sql`](../docker/sqlite/schema.sql).

| Doc | Purpose |
|---|---|
| [examples.md](examples.md) | End-to-end examples: files, commands, sample JSON |
| [descriptors.md](descriptors.md) | Reference: trunk/branch/twig, shaping, `$action`, errors |

## Try it

```bash
make install
make example                 # Python: all fixtures + explain
make example-csharp          # same tour in .NET (Docker SDK)
make yaal ARGS='list'
make yaal ARGS='query user/get --arg id=1'
```

Runnable demos:

- Python: [`examples/demo.py`](../examples/demo.py)
- C#: [`csharp/examples/Yaal.Example/`](../csharp/examples/Yaal.Example/)

Fixture operations:

| Path | Shows |
|---|---|
| `user/get` | Nested object + `parent_rows` |
| `user/list` | Root array + `optional()` filter |
| `user/page` | Multi-file branches + `$action=params` |
| `user/create` | Multi-twig INSERT → SELECT |
