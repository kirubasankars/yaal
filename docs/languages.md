# Languages

Each file compiles the same query, runs it on the `users` table that is already in `users.db`, and prints the rows. `placeholder` is `question`, so the binds are `?`. The published packages include the native compiler for Linux x86_64, macOS Apple silicon, and Windows x64.

## Rust

```bash
cargo add yaal serde_json
cargo add rusqlite --features bundled
```

`src/main.rs`:

```rust
use rusqlite::types::Value as SqlValue;
use rusqlite::{types::ValueRef, Connection};
use serde_json::Value;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sql = r#"--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
"#;
    let params = serde_json::json!({"id": 1}).as_object().unwrap().clone();
    let compiled = yaal::compile(sql, &params, &yaal::CompileOptions::default())?;
    let statement = &compiled.statements[0];
    let conn = Connection::open("users.db")?;
    let mut stmt = conn.prepare(&statement.sql)?;
    let binds: Vec<SqlValue> = statement.values().map(sql_value).collect();
    let mut rows = stmt.query(rusqlite::params_from_iter(binds))?;
    while let Some(row) = rows.next()? {
        for index in 0..row.as_ref().column_count() {
            if index > 0 {
                print!("\t");
            }
            print!("{}", cell(row.get_ref(index)?));
        }
        println!();
    }
    Ok(())
}

fn sql_value(value: &Value) -> SqlValue {
    match value {
        Value::Null => SqlValue::Null,
        Value::Bool(value) => SqlValue::Integer(if *value { 1 } else { 0 }),
        Value::Number(value) => value
            .as_i64()
            .map(SqlValue::Integer)
            .unwrap_or_else(|| SqlValue::Real(value.as_f64().unwrap_or(0.0))),
        Value::String(value) => SqlValue::Text(value.clone()),
        other => SqlValue::Text(other.to_string()),
    }
}

fn cell(value: ValueRef<'_>) -> String {
    match value {
        ValueRef::Null => String::new(),
        ValueRef::Integer(value) => value.to_string(),
        ValueRef::Real(value) => value.to_string(),
        ValueRef::Text(value) => String::from_utf8_lossy(value).into_owned(),
        ValueRef::Blob(value) => format!("{value:?}"),
    }
}
```

## Python

```bash
pip install yaal-sql
```

The PyPI project is `yaal-sql` because the name `yaal` is already taken. The import name is `yaal`.

`list_users.py`:

```python
import sqlite3

import yaal

sql = """--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
"""
compiled = yaal.compile(sql, {"id": 1}, placeholder="question")
statement = compiled["statements"][0]
with sqlite3.connect("users.db") as conn:
    rows = conn.execute(statement["sql"], yaal.values(statement))
    for row in rows:
        print(*row, sep="\t")
```

## C#

```bash
dotnet add package Yaal
dotnet add package Microsoft.Data.Sqlite
```

The package targets .NET Standard 2.0, so .NET Framework 4.6.1, .NET Core 2.0, and later can reference it.

`Program.cs`:

```csharp
using Microsoft.Data.Sqlite;
using Yaal;

var sql = """
--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
""";
var statement = Compiler.Compile(sql, new { id = 1 }, "question").Statements[0];
using var connection = new SqliteConnection("Data Source=users.db");
connection.Open();
using var command = connection.CreateCommand();
command.CommandText = statement.Sql;
foreach (var value in statement.Values())
{
    command.Parameters.Add(new SqliteParameter { Value = value ?? DBNull.Value });
}
using var reader = command.ExecuteReader();
while (reader.Read())
{
    for (var index = 0; index < reader.FieldCount; index++)
    {
        if (index > 0)
        {
            Console.Write('\t');
        }
        Console.Write(reader.GetValue(index));
    }
    Console.WriteLine();
}
```

## Go

```bash
go get github.com/kirubasankars/yaal/bindings/go@v1.0.0
go get modernc.org/sqlite
```

The module tag is `bindings/go/v1.0.0`.

`main.go`:

```go
package main

import (
	"database/sql"
	"fmt"

	"github.com/kirubasankars/yaal/bindings/go"
	_ "modernc.org/sqlite"
)

func main() {
	sqlText := `--(id, active)--
select *
from users u
where optional(u.id = {{id}})
  and optional(u.active = {{active}})
`
	compiled, err := yaal.Compile(sqlText, map[string]any{"id": 1}, "question")
	if err != nil {
		panic(err)
	}
	statement := compiled.Statements[0]
	binds, err := statement.Values()
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("sqlite", "users.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	rows, err := db.Query(statement.SQL, binds...)
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		values := make([]any, len(columns))
		dest := make([]any, len(columns))
		for index := range values {
			dest[index] = &values[index]
		}
		if err := rows.Scan(dest...); err != nil {
			panic(err)
		}
		for index, value := range values {
			if index > 0 {
				fmt.Print("\t")
			}
			fmt.Print(value)
		}
		fmt.Println()
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}
```

## Java

```xml
<dependency>
  <groupId>io.github.kirubasankars</groupId>
  <artifactId>yaal</artifactId>
  <version>1.0.0</version>
</dependency>
<dependency>
  <groupId>org.xerial</groupId>
  <artifactId>sqlite-jdbc</artifactId>
  <version>3.53.4.0</version>
</dependency>
```

The program needs `--enable-native-access=ALL-UNNAMED`.

`ListUsers.java`:

```java
import java.sql.DriverManager;
import java.util.Map;
import yaal.Yaal;

public final class ListUsers {
    public static void main(String[] args) throws Exception {
        String sql = """
                --(id, active)--
                select *
                from users u
                where optional(u.id = {{id}})
                  and optional(u.active = {{active}})
                """;
        var statement = Yaal.compile(sql, Map.of("id", 1), "question").statements().get(0);
        try (var connection = DriverManager.getConnection("jdbc:sqlite:users.db");
                var query = connection.prepareStatement(statement.sql())) {
            var index = 1;
            for (var value : statement.values()) {
                query.setObject(index++, value);
            }
            try (var rows = query.executeQuery()) {
                var columns = rows.getMetaData().getColumnCount();
                while (rows.next()) {
                    for (var column = 1; column <= columns; column++) {
                        if (column > 1) {
                            System.out.print('\t');
                        }
                        System.out.print(rows.getString(column));
                    }
                    System.out.println();
                }
            }
        }
    }
}
```
