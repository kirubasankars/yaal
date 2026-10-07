# Yaal documentation

The documentation site is [index.md](index.md). Preview it with:

```bash
make docs-install
make docs-serve
```

| Section | Purpose |
|---|---|
| [Tutorial](tutorial/index.md) | Install through precompile, on the shared fixtures |
| [Guides](guides/index.md) | One task per page (filters, groups, paging, explain) |
| [Concepts](concepts/index.md) | Lifecycle, elision, optional semantics, shaping |
| [Reference](reference/index.md) | Header, DSL, output JSON, CLI, URLs |
| [Fixture index](cookbook/fixtures.md) | Which folder demonstrates what |
| [Python](appendix/python.md) · [C#](appendix/csharp.md) | Runtime APIs |

Older filenames still resolve:

- [learn.md](learn.md) → tutorial
- [examples.md](examples.md) → guides
- [descriptors.md](descriptors.md) → reference

Fixtures: `tests/fixtures/api/`. Seed: `docker/sqlite/schema.sql`.
