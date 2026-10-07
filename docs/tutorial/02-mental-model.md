# Mental model

**Goal:** name the pieces you will see in every operation.

```text
descriptor folder  →  SQL (+ child or sibling .sql)  →  bind {{params}}
                   →  subtract optional(...) when absent
                   →  execute twigs (maybe named DBs)
                   →  shape with $.output.json  →  nested JSON
```

## Ideas that stay true

- **Descriptors are folders**, not HTTP routes. The call path is the folder path (`user/get`).
- **SQL is the source of truth.** There is no query builder. Aggregations and `WITH` are normal SQL.
- **Subtractive, not additive.** Unused filters are removed. They are not generated from models.
- **`$` files.** Trunk `$.sql` when present. Branches such as `$.roles.sql` and `$.paging.sql`. Shape in `$.output.json`.
- **Discovery is filesystem-first.** Yaal lists `*.sql`, then output JSON shapes each branch.

Deeper treatment: [Descriptor lifecycle](../concepts/descriptor-lifecycle.md) and [Branches, twigs, and output](../concepts/branches-twigs-and-output.md).

## Files to open

- [tests/fixtures/api/user/get/](https://github.com/kirubasankars/yaal/tree/main/tests/fixtures/api/user/get) — one trunk SQL file plus a shape

## Exercise

List the files in `tests/fixtures/api/user/page/`. There is no `$.sql`. The operation is still valid because sibling SQL files are enough.

Next: [Nested get](03-nested-get.md).
