<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Writes and modes

**Problem:** run several statements in one operation (insert, then insert a link, then select the shaped row).

## SQL

Fixture `user/create` uses payload names, not `$args`. The header marks them required:

```sql
--(id! integer, name! string)--

INSERT INTO users (user_id, user_name, active) VALUES ({{id}}, {{name}}, 1)

--sql--

INSERT INTO user_roles (user_id, role_id) VALUES ({{id}}, 2)

--sql--

SELECT
    u.user_id, u.user_name, r.role_id, r.role_name
FROM users u
INNER JOIN user_roles ur ON ur.user_id = u.user_id
INNER JOIN roles r ON r.role_id = ur.role_id
WHERE u.user_id = {{id}}
ORDER BY r.role_id
```

The output shape matches `user/get`. After a write, providers set `$params.$last_inserted_id` when you need the engine’s id. This fixture passes `id` in the payload instead, which stays stable.

## Commands

`make example` does not run this. Use a writable database:

```bash
sqlite3 /tmp/yaal-writable.db < docker/sqlite/schema.sql

yaal query user/create \
  --api tests/fixtures/api \
  --db 'sqlite3:////tmp/yaal-writable.db' \
  --payload '{"id": 99, "name": "newbie"}'
```

Missing `id` or `name` returns `{"errors":[...]}` and does not execute.

## Other `$mode` values

Use `$mode=error` for a soft business failure, `$mode=break` to return rows early, and `$mode=json` when the engine already built JSON. Reference: [Mode rows](../reference/mode-rows.md).

## Custom providers

To log SQL, route tenants, or fake a database, supply your own provider. The descriptor tree stays the same. Method list: [Python](../appendix/python.md), [C#](../appendix/csharp.md).
