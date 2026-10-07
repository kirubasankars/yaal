# Output shaping

**Goal:** read `$.output.json` without guessing what each key does.

## Vocabulary

| Key | Meaning |
|---|---|
| `mapped` | SQL column → JSON field |
| `partition_by` | Collapse duplicate parent keys from a join |
| `parent_rows: true` | Nest children from the **parent** result set (no child SQL file) |
| `type: object` / `array` | One object vs a list at that branch |

Root `type` sets object vs array. Named nested branches each have their own `type` and `properties`. A bare anonymous `type: object` or `type: array` under `properties` is invalid. A JSON field that is literally named `type` uses `type: { "mapped": "col" }`.

## Files to open

- [tests/fixtures/api/user/get/$.output.json](https://github.com/kirubasankars/yaal/blob/main/tests/fixtures/api/user/get/$.output.json)
- [tests/fixtures/api/user/list/$.output.json](https://github.com/kirubasankars/yaal/blob/main/tests/fixtures/api/user/list/$.output.json)

`user/get` is one object. `user/list` is an array of objects. Both map columns with `mapped`. Only `user/get` nests `roles` from the same row set.

## Commands

```bash
yaal query user/get --arg id=1
yaal query user/list --arg active=1
```

## Exercise

In the [experiment sandbox](../guides/experiment-sandbox.md), add a mapped field to `user/get` for a column the SQL already returns (or add the column to the `SELECT` first). Re-run the query and confirm the new key appears.

Why these strategies differ: [Shaping strategies](../concepts/shaping-strategies.md). Grammar: [Output JSON schema](../reference/output-json-schema.md).

Next: [Child SQL](06-child-sql.md).
