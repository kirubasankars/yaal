# Output shaping

**Problem:** turn a flat `SELECT` into the JSON object or array your API returns.

## Shape

`user/get` maps a join into one object and a `roles` array. Open [$.output.json](https://github.com/kirubasankars/yaal/blob/master/tests/fixtures/api/user/get/$.output.json).

```json
{
  "type": "object",
  "partition_by": "user_id",
  "properties": {
    "id": { "mapped": "user_id" },
    "name": { "mapped": "user_name" },
    "roles": {
      "type": "array",
      "partition_by": "role_id",
      "parent_rows": true,
      "properties": {
        "id": { "mapped": "role_id" },
        "name": { "mapped": "role_name" }
      }
    }
  }
}
```

## Commands

```bash
yaal query user/get --arg id=1
```

Expect `id`, `name`, and `roles` with two role objects for admin.

`user/list` uses `"type": "array"` and does not nest. Same `mapped` keys.

Which strategy to pick: [Shaping strategies](../concepts/shaping-strategies.md). Key reference: [Output JSON schema](../reference/output-json-schema.md).

Alternate file `$.output.summary.json` loads when the caller passes `output_mapper="summary"`.
