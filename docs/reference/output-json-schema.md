<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Output JSON schema

How to choose a strategy: [Shaping strategies](../concepts/shaping-strategies.md).

Root `type` is `object` (one result) or `array` (a list). Fields are a flat map under `properties`. Named nested branches have their own `type` and `properties`.

```json
{
  "type": "object",
  "partition_by": "user_id",
  "properties": {
    "id": { "mapped": "user_id" },
    "roles": {
      "type": "array",
      "partition_by": "role_id",
      "parent_rows": true,
      "properties": {
        "id": { "mapped": "role_id" }
      }
    }
  }
}
```

| Key | Role |
|---|---|
| `mapped` | SQL column → JSON field |
| `partition_by` | Collapse join fan-out on this column |
| `parent_rows` | Nest from parent rows. The parent must set `partition_by`. |
| `type` | `object` → one object; `array` → list. On the root or on a named branch. |

Invalid: a bare `type: object` or `type: array` under `properties` (including a nested item wrapper). A JSON field named `type` is written as an object with `mapped`, for example `{ "mapped": "some_column" }`.

`$.output.<mapper>.json` is selected with `output_mapper`. See [Descriptor layout](descriptor-layout.md).
