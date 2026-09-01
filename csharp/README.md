# Yaal

**Subtractive SQL→JSON for .NET 8.** You author full SQL (plus YAML shapes). At bind time Yaal **subtracts** unused `optional(...)` fragments, runs the remaining statements (optionally across named databases), and shapes flat rows into **nested JSON**.

Yaal is not an additive ORM: no entity tracking, migrations, or query-builder DSL. SQL files stay the source of truth.

Pipeline: *write SQL → subtract optionals → run → shape → JSON*.

## Install

```bash
dotnet add package Yaal
dotnet add package Microsoft.Data.Sqlite   # or Npgsql / MySqlConnector / ClickHouse.Client
```

Database clients are **not** shipped as NuGet dependencies. Add the driver your app uses. If a client is missing, `SetupDataProvider` throws with the package name to install.

| Engine | Package |
|---|---|
| SQLite | [Microsoft.Data.Sqlite](https://www.nuget.org/packages/Microsoft.Data.Sqlite) |
| PostgreSQL | [Npgsql](https://www.nuget.org/packages/Npgsql) |
| MySQL | [MySqlConnector](https://www.nuget.org/packages/MySqlConnector) |
| ClickHouse | [ClickHouse.Client](https://www.nuget.org/packages/ClickHouse.Client) |

Requires **.NET 8**. License: [MIT](https://github.com/kirubasankars/yaal/blob/master/LICENSE).

## Usage

Point Yaal at a folder of descriptor operations (`*.sql` plus optional `$.output.yaml`). Call operations by path:

```csharp
using Yaal;

var y = new Yaal("./api");
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");

var result = y.Query("user/get", args: new { id = 1 });
string json = y.QueryJson("user/get", args: new { id = 1 });
```

Preview compiled SQL after optional-filter elision:

```csharp
foreach (var twig in y.ExplainSql("user/get", args: new { id = 1 }))
    Console.WriteLine($"{twig["sql"]}  {twig["parameters"]}");
```

A descriptor is a folder such as `api/user/get/`:

```sql
--($args.id integer)--
select u.user_id as id, u.user_name as name
from users u
where u.user_id = {{$args.id}}
  and optional(u.active = {{$args.active}})
```

`optional(...)` is removed when that parameter is omitted or null. Aggregations, `WITH` / CTEs, and window functions stay ordinary SQL.

Python and .NET share the same descriptor files.

## Database URLs

| Engine | Example |
|---|---|
| SQLite (absolute) | `sqlite3:////tmp/app.db` |
| SQLite (relative) | `sqlite3://./data/app.db` |
| SQLite (memory) | `sqlite3:///` |
| Postgres | `postgresql://user:pass@127.0.0.1:5432/yaal` |
| MySQL | `mysql://user:pass@127.0.0.1:3306/yaal` |
| ClickHouse | `clickhouse://user:pass@127.0.0.1:9000/yaal` |

ClickHouse uses HTTP via ClickHouse.Client. Port `9000` (native default) is remapped to `8123`.

Named providers can run in one operation (`--sql(flags)--` twigs). Register each connection:

```csharp
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");
y.SetupDataProvider("flags", "sqlite3:////tmp/flags.db");
```

## Custom providers

Register your own engine, mock, or wrapper by implementing `IDataProviderContextManager`:

```csharp
y.SetupDataProvider("db", new MyContextManager());
y.SetupDataProvider("db", new MyContextManager(), scheme: "postgresql");
```

`scheme` is optional. `postgresql`, `mysql`, and `clickhouse` use `%s` placeholders in `ExplainSql`; anything else uses `?`.

## Documentation

- [Learning path](https://github.com/kirubasankars/yaal/blob/master/docs/learn.md)
- [Examples](https://github.com/kirubasankars/yaal/blob/master/docs/examples.md) (SQL, YAML, sample JSON, C#)
- [Descriptor reference](https://github.com/kirubasankars/yaal/blob/master/docs/descriptors.md)
- [Source repository](https://github.com/kirubasankars/yaal)

## Feedback

[Open an issue](https://github.com/kirubasankars/yaal/issues) on GitHub.
