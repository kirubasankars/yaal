<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Explain and debug

**Problem:** see compiled SQL and binds without hitting the database.

`explain` uses the same elision, array expansion, and `sort()` / `dir()` resolution as `query`. It does not shape JSON.

## Commands

```bash
yaal explain user/list
yaal explain user/list --arg active=1
yaal explain user/list --arg sort=name --arg dir=desc
yaal explain user/page --arg page=1 --arg page_size=10
yaal explain user/groups --arg 'pairs=[{"id":1},{"id":2}]'
```

| | `query` | `explain` |
|---|---|---|
| Hits the database | yes | no |
| Elides `optional()` | yes | yes |
| Resolves `sort()` / `dir()` | yes | yes |
| Shapes nested JSON | yes | no |
| Returns | JSON | SQL text and binds per twig |

The CLI prints SQL, then `binds: [...]`, once per twig. Multi-twig operations such as `user/page` print more than one block.

Compile goldens for elision live under `tests/fixtures/sql_compile/`.

`--debug` reloads descriptors from disk. It is not a log level. Soft input errors still return `{"errors":[...]}` from `query`; explain surfaces compile errors the same way `query` would before execute.

What the nulls set contains: [Compile and elision](../concepts/compile-and-elision.md).
