<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Yaal

Yaal compiles a SQL string into dialect SQL and ordered bind values. You write the full statement, mark the parts that depend on arguments, and pass a JSON object. Yaal removes the unused parts and returns the SQL plus the binds. It does not open a database, shape rows, or track entities.

```sql
--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
```

`{"id": 1}` compiles to `select * from users u where (u.id = ?)` with one bind. An omitted or null argument drops its `optional(...)` block. A list expands only where the placeholder is written with `...`:

```sql
--(id)--
where optional(u.user_id in ({{id}}...))
```

`{"id": [1, 2]}` becomes `in (?, ?)` and binds `[1, 2]`.

License: [MIT](LICENSE).

## Library

```rust
let params = serde_json::json!({"id": [1, 2]}).as_object().unwrap().clone();
let compiled = yaal::compile(sql, &params, &yaal::CompileOptions::default())?;
```

`CompileOptions.placeholder` is `?` or `$1`, `$2`, …. `--sql--` splits the input into separate statements that share the same parameters. Each statement numbers its own `$1`.

The result is JSON:

```json
{
  "statements": [
    {
      "sql": "select * from users u where (u.user_id in (?, ?))",
      "parameters": [
        {"name": "id", "type": "integer", "value": 1},
        {"name": "id", "type": "integer", "value": 2}
      ]
    }
  ]
}
```

## CLI

```bash
cargo run -p yaal-cli -- compile --sql query.sql --params params.json
cargo run -p yaal-cli -- compile --sql query.sql --param id=1 --placeholder numbered
```

SQL is read from stdin when `--sql` is omitted and stdin is not a terminal. `yaal compile` with no SQL exits instead of waiting.

## Bindings

`cargo build -p yaal-ffi` produces the shared library. Set `YAAL_LIB` to that file (`libyaal_ffi.so`, `libyaal_ffi.dylib`, or `yaal_ffi.dll`). The C functions are `yaal_compile` and `yaal_free` (`bindings/yaal.h`). Each binding returns the same JSON: `statements` of `sql` plus `parameters` (`name`, `type`, `value`). A compiler failure raises the language's compile error.

```python
import yaal
compiled = yaal.compile(sql, {"id": 1}, placeholder="question")
```

```csharp
var compiled = Yaal.Compiler.Compile(sql, new { id = 1 }, "question");
```

```go
compiled, err := yaal.Compile(sql, map[string]any{"id": 1}, "question")
```

```java
Yaal.Compiled compiled = Yaal.compile(sql, Map.of("id", 1), "question");
```

Java calls the library with the foreign function API, so the program needs `--enable-native-access=ALL-UNNAMED`.

## Syntax

- [Parameter header](docs/header.md)
- [optional and optional_when](docs/optional.md)
- [optional groups](docs/groups.md)
- [sort and dir](docs/sort.md)
- [examples](docs/examples.md)

## Build

```bash
make test
make ci
make docs
make nuget
```

`make nuget` packs the C# binding into `artifacts/`. Publishing happens from a published GitHub release: the tag, such as `v2.0.0`, must match `<Version>` in the C# project. The workflow runs the C# tests, packs, validates the package, and pushes it to NuGet with trusted publishing.

`make ci` is what GitHub Actions runs: the Rust tests, the language bindings, and `cargo fmt --check`.

`make docs` serves this README and the syntax pages at http://127.0.0.1:8080. Set `DOCS_PORT` to use another port. A push to `main` publishes the same pages to https://kirubasankars.github.io/yaal/.

`make test` runs the Rust tests, then the Python, Go, C#, and Java bindings against `libyaal_ffi`. The Java step needs a JDK.
