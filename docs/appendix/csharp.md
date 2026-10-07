# C#

.NET 8 package `Yaal` on NuGet. Install the driver you use (`Microsoft.Data.Sqlite`, `Npgsql`, `MySqlConnector`, or `ClickHouse.Client`). Full packaging notes: [csharp/README.md](https://github.com/kirubasankars/yaal/blob/master/csharp/README.md).

Descriptors are the same folders Python uses. This page is only the runtime.

## API

```csharp
var y = new Yaal.Yaal("path/to/api", debug: true);
// or precompiled: "path/to/precompiled"
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");

y.Query("user/get", args: new { id = 1 });
y.QueryJson("user/get", args: new { id = 1 });
y.ExplainSql("user/get", args: new { id = 1 });
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

## Custom provider

Implement `IDataProviderContextManager` and pass it to `SetupDataProvider`. Optional `scheme` selects the explain placeholder: `postgresql`, `mysql`, and `clickhouse` use `%s`; anything else uses `?`.

```csharp
y.SetupDataProvider("db", new MyContextManager(), scheme: "postgresql");
```

## URLs

[Database URLs](../reference/database-urls.md). Pooling is the driver’s: pass `pooling` and pool size on the URL query string.

Tour without a local SDK: `make example-csharp`.
