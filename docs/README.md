# Yaal docs

| Doc | Contents |
|---|---|
| [examples.md](examples.md) | End-to-end walkthroughs of every fixture (SQL, YAML, JSON, CLI, Python, C#) |
| [descriptors.md](descriptors.md) | Trunk / branch / twig reference, shaping, `$action`, errors, dual-port schema subset |

Seed schema used by the fixtures: [`docker/sqlite/schema.sql`](../docker/sqlite/schema.sql) (`users`, `roles`, `user_roles`).

```bash
make install
make yaal ARGS='list'
make example
```
