# Languages

Each package compiles the same SQL. `placeholder` is `question` (`?`) or `numbered` (`$1`, `$2`, …). The samples pass `{"id": 1}` and print `select * from users u where (u.id = ?)`. The published packages include the native compiler for Linux x86_64, macOS Apple silicon, and Windows x64.

```sql
--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
```

## Rust

```bash
cargo add yaal serde_json
```

```rust
let sql = r#"--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
"#;
let params = serde_json::json!({"id": 1}).as_object().unwrap().clone();
let compiled = yaal::compile(sql, &params, &yaal::CompileOptions::default())?;
println!("{}", compiled.statements[0].sql);
```

`CompileOptions { placeholder: yaal::Placeholder::Numbered, .. }` uses `$1`.

## Python

```bash
pip install yaal-sql
```

The PyPI project is `yaal-sql` because the name `yaal` is already taken. The import name is `yaal`.

```python
import yaal

sql = """--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
"""
compiled = yaal.compile(sql, {"id": 1}, placeholder="question")
print(compiled["statements"][0]["sql"])
```

## C#

```bash
dotnet add package Yaal
```

The package targets .NET Standard 2.0, so .NET Framework 4.6.1, .NET Core 2.0, and later can reference it.

```csharp
using Yaal;

var sql = """
--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
""";
var compiled = Yaal.Compiler.Compile(sql, new { id = 1 }, "question");
Console.WriteLine(compiled.Statements[0].Sql);
```

## Go

```bash
go get github.com/kirubasankars/yaal/bindings/go@v0.13.0
```

The module tag is `bindings/go/v0.13.0`.

```go
package main

import (
	"fmt"

	"github.com/kirubasankars/yaal/bindings/go"
)

func main() {
	sql := `--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
`
	compiled, err := yaal.Compile(sql, map[string]any{"id": 1}, "question")
	if err != nil {
		panic(err)
	}
	fmt.Println(compiled.Statements[0].SQL)
}
```

## Java

```xml
<dependency>
  <groupId>io.github.kirubasankars</groupId>
  <artifactId>yaal</artifactId>
  <version>0.13.0</version>
</dependency>
```

The program needs `--enable-native-access=ALL-UNNAMED`.

```java
import java.util.Map;
import yaal.Yaal;

String sql = """
        --(id, active)--
        select *
        from users u
        where optional(u.id = {{id}})
          and optional(u.active = {{active}})
        """;
Yaal.Compiled compiled = Yaal.compile(sql, Map.of("id", 1), "question");
System.out.println(compiled.statements().get(0).sql());
```
