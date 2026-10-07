<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Descriptor layout

Operations are folders of SQL and JSON. You call them by path (`user/get`).

How discovery builds the tree: [Branches, twigs, and output](../concepts/branches-twigs-and-output.md).

```text
api/user/get/
  $.sql
  $.output.json
  $.output.summary.json     # optional; output_mapper "summary"

api/user/nested/
  $.sql
  $.roles.sql               # child → property "roles"
  $.output.json

api/user/page/
  $.paging.sql              # sibling; no trunk $.sql
  $.data.sql
  $.output.json
```

| Concept | Meaning |
|---|---|
| Operation | Folder under the API root |
| Trunk | `$`, file `$.sql` when present |
| Branch | `$.roles` from `$.roles.sql` |
| Twig | One statement, split by `--sql--` |

## Multi-twig

`--sql--` starts the next statement in the same file. Every twig uses the provider passed to `query`. Args and payload are shared. Cross-statement values go through `$params`.

## Multi-file

- `$.{name}.sql` → branch `$.{name}` → JSON property `name`.
- An object or array property with no SQL file is a slot for `parent_rows`.
- Sibling-only operations omit `$.sql`.

`LIMIT` / `OFFSET` with join fan-out and `parent_rows` should page the parent in a subquery first.

## `output_mapper`

`$.output.<name>.json` loads instead of `$.output.json` when the caller sets `output_mapper` to `<name>`. There is no process-wide result cache. `clear_cache()` only drops cached descriptors.
