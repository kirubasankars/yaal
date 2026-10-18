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

`pip install yaal-sql` installs that module with the shared library inside the wheel. The PyPI project is `yaal-sql` because the name `yaal` is already taken.

```csharp
var compiled = Yaal.Compiler.Compile(sql, new { id = 1 }, "question");
```

The NuGet package targets .NET Standard 2.0, so .NET Framework 4.6.1, .NET Core 2.0, and later can reference it. The package includes the native compiler for Linux x86_64, macOS Apple silicon, and Windows x64.

```go
import "github.com/kirubasankars/yaal/bindings/go"

compiled, err := yaal.Compile(sql, map[string]any{"id": 1}, "question")
```

`go get github.com/kirubasankars/yaal/bindings/go@v1.0.0` installs that module. The module tag is `bindings/go/v1.0.0`, and the module contains the native compiler for Linux x86_64, macOS Apple silicon, and Windows x64. `Compile` loads `YAAL_LIB` when that variable is set, then the embedded library, then the shared library built under `target/debug` or `target/release`.

```java
Yaal.Compiled compiled = Yaal.compile(sql, Map.of("id", 1), "question");
```

Java calls the library with the foreign function API, so the program needs `--enable-native-access=ALL-UNNAMED`. The Maven coordinate is `io.github.kirubasankars:yaal`. That jar contains the native compiler for Linux x86_64, macOS Apple silicon, and Windows x64.

```xml
<dependency>
  <groupId>io.github.kirubasankars</groupId>
  <artifactId>yaal</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Syntax

- [Languages](docs/languages.md)
- [Parameter header](docs/header.md)
- [optional and optional_when](docs/optional.md)
- [optional groups](docs/groups.md)
- [Nested](docs/nested.md)
- [sort and dir](docs/sort.md)
- [Safety](docs/safety.md)
- [examples](docs/examples.md)

## Build

```bash
make test
make ci
make docs
make nuget
make pypi
make maven
make go
```

`make nuget` packs the C# binding for this machine into `artifacts/`. `make pypi` builds a Python wheel for this machine into the same directory. It needs the `build` and `wheel` packages (`python3 -m pip install build wheel`). `make maven` packs the Java binding for this machine into the same directory. The first run downloads Maven. `make go` builds the release library for this machine, stages it into the Go package, and tests that package with the embedded library.

Publishing happens from a published GitHub release. The tag, such as `v1.0.0`, must match `version` in the Rust crates, `<Version>` in the C# project, `version` in `pyproject.toml`, `<version>` in `bindings/java/pom.xml`, and `bindings/go/version.txt`. The crates.io workflow publishes `yaal` and `yaal-cli`. The NuGet workflow runs the C# tests, packs the native compiler for Linux x86_64, macOS Apple silicon, and Windows x64, validates the package, and pushes it to NuGet with trusted publishing. The PyPI workflow runs the Python tests, builds wheels for Linux x86_64, macOS Apple silicon, and Windows x64, and pushes them to PyPI with trusted publishing. PyPI has to list this repository's `pypi-publish.yml` workflow and the `pypi` environment as a trusted publisher. The Maven workflow runs the Java tests, packs those same three native compilers into one jar, and pushes it to Maven Central. The `central` environment needs `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, `MAVEN_GPG_KEY`, and `MAVEN_GPG_PASSPHRASE`. Central Portal has to verify the `io.github.kirubasankars` namespace before the first publish. The Go workflow, `.github/workflows/go-publish.yml`, runs the Go tests on Linux x86_64, macOS Apple silicon, and Windows x64, embeds those three native compilers into the module, and pushes the tag `bindings/go/v1.0.0`. `go get` resolves that tag. The release tag `v1.0.0` publishes the Rust, NuGet, PyPI, and Maven packages. `bindings/go/version.txt` has to match both tags.

crates.io can attach a trusted publisher only after the crate exists. For the first release, add the crates.io token as the `CARGO_REGISTRY_TOKEN` repository secret, then delete that secret once the publisher is in place. On each crate's settings page, add a GitHub trusted publisher for owner `kirubasankars`, repository `yaal`, workflow `crates-publish.yml`, and environment `crates-io`.

`make ci` is what GitHub Actions runs: the Rust tests, the language bindings, and `cargo fmt --check`.

`make docs` serves this README and the syntax pages at http://127.0.0.1:8080. Set `DOCS_PORT` to use another port. A push to `main` publishes the same pages to https://kirubasankars.github.io/yaal/.

`make test` runs the Rust tests, then the Python, Go, C#, and Java bindings against `libyaal_ffi`. The Java step needs a JDK.
