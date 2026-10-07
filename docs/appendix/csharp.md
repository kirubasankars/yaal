<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# C#

NuGet package `Yaal`, targeting .NET Standard 2.0 (.NET Framework 4.6.1+ and .NET Core 2.0 / .NET 5+). The library does not reference a database driver. The CLI and the example need the .NET 8 runtime. Full packaging notes: [csharp/README.md](https://github.com/kirubasankars/yaal/blob/master/csharp/README.md).

Descriptors are the same folders Python uses. This page is only the runtime.

## API

```csharp
var y = new Yaal.Yaal("path/to/api", debug: true);
// or precompiled: "path/to/precompiled"

y.Query(provider, "user/get", args: new { id = 1 });
y.QueryJson(provider, "user/get", args: new { id = 1 });
y.ExplainSql(provider, "user/get", args: new { id = 1 });
```

`debug: true` forces live SQL and JSON.

## Register and source compile

In-memory registration wins over precompiled JSON when debug is off:

```csharp
y.RegisterDescriptor("user/get", myBranch);
y.UnregisterDescriptor("user/get");
```

Generate C# branches (no JSON parse at startup):

```bash
dotnet run --project csharp/src/Yaal.Cli -- \
  compile --api tests/fixtures/api --format cs --out Generated/YaalDescriptors
```

```csharp
foreach (var (path, branch) in Yaal.Generated.YaalDescriptorRegistry.All)
    y.RegisterDescriptor(path, branch);
```

JSON artifacts (`--format json`) match the Python `yaal compile` layout. Generated C# is faster to load and must be regenerated when SQL changes. `make benchmark-csharp` compares load paths.

## Application provider

The application opens the connection and passes an `IDataProvider` to `Query`. Yaal compiles each twig, then calls `Execute` with the SQL and bind values. It does not open, commit, roll back, or close that connection.

```csharp
sealed class SqliteProvider : IDataProvider
{
    public string Placeholder => "?"; // postgres, mysql, and clickhouse use "%s"
    private readonly SqliteConnection _connection;

    public SqliteProvider(SqliteConnection connection) => _connection = connection;

    public void Begin() { }  // the connection is already open

    public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
        string sql, IReadOnlyList<object?> parameters)
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = sql;
        for (var i = 0; i < parameters.Count; i++)
            cmd.Parameters.AddWithValue("$p" + i, parameters[i] ?? DBNull.Value);
        using var reader = cmd.ExecuteReader();
        var rows = new List<IDictionary<string, object?>>();
        while (reader.Read())
        {
            var row = new Dictionary<string, object?>();
            for (var i = 0; i < reader.FieldCount; i++)
                row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            rows.Add(row);
        }
        return (rows, null);
    }

    public void End() { }    // the application commits and closes
    public void Error() { }  // the application rolls back and closes
}
```

Microsoft.Data.Sqlite binds `$p0` rather than `?`. Replace each `?` in `sql` with `$p0`, `$p1`, and so on before `ExecuteReader`. Npgsql and MySqlConnector need the same kind of rewrite (`$1`, `@p0`).

```csharp
var connection = new SqliteConnection("Data Source=/tmp/app.db");
connection.Open();
var provider = new SqliteProvider(connection);
y.Query(provider, "user/get", args: new { id = 1 });
```

`Placeholder` is `?` or `%s`. A second database is a second `Query` call with a second provider.

`Begin` and `End` are where an application may start a transaction or leave the connection alone. `Error` is called when the query fails.

URL strings such as `sqlite3:////tmp/app.db` stay for the example and tests. They call `Yaal.Drivers.DriverRegistry.Open`. Those providers still open in `Begin` and close in `End`. The `Yaal` package does not reference that project.

## URLs

[Database URLs](../reference/database-urls.md). Pooling is the driver’s: pass `pooling` and pool size on the URL query string.

Tour without a local SDK: `make example-csharp`.
