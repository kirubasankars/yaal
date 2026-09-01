# Yaal for .NET

C# port of the Yaal SQL→JSON library. Same descriptor files, optional-filter SQL DSL, nested JSON shaping, and multi-engine providers.

Package: [`Yaal`](https://www.nuget.org/packages/Yaal) `0.1.0` (MIT).

```bash
dotnet add package Yaal
```

Or add a project reference to [`src/Yaal/Yaal.csproj`](src/Yaal/Yaal.csproj).

Docs: [examples](../docs/examples.md) · [descriptors](../docs/descriptors.md) · [index](../docs/README.md).

Runnable tour (get / list / page / create + explain): `make example-csharp`.

## Requirements

- Docker (tests run in `mcr.microsoft.com/dotnet/sdk:8.0` — no local SDK required)

## Quick start

From the repo root:

```bash
make example-csharp              # full fixture tour
make test-csharp                 # unit + SQLite e2e in container
make test-csharp-integration     # also Postgres/MySQL/ClickHouse
```

With a local .NET 8 SDK you can still run directly:

```bash
cd csharp && dotnet test && dotnet run --project examples/Yaal.Example
dotnet pack src/Yaal/Yaal.csproj -c Release
```

### Programmatic usage

```csharp
var y = new Yaal.Yaal("tests/fixtures/api", debug: true);
y.SetupDataProvider("db", "sqlite3:////tmp/app.db");

var result = y.Query("user/get", args: new { id = 1 });
string json = y.QueryJson("user/get", args: new { id = 1 });
var page = y.Query("user/page", args: new { page = 1, page_size = 10 });

foreach (var twig in y.ExplainSql("user/get", args: new { id = 1 }))
    Console.WriteLine(twig["sql"]);
```

Descriptors are shared with the Python library under [`../tests/fixtures/api/`](../tests/fixtures/api/) (`user/get`, `user/nested`, `user/list`, `user/page`, `report/summary`, `user/combine`).

## Database URLs

| Engine | Example |
|---|---|
| SQLite (absolute) | `sqlite3:////tmp/app.db` |
| SQLite (relative) | `sqlite3://./data/app.db` |
| SQLite (memory) | `sqlite3:///` |
| Postgres | `postgresql://user:pass@127.0.0.1:5432/yaal` |
| MySQL | `mysql://user:pass@127.0.0.1:3306/yaal` |
| ClickHouse | `clickhouse://user:pass@127.0.0.1:9000/yaal` |

ClickHouse uses HTTP via ClickHouse.Client. Port `9000` (Python native default) is remapped to `8123`.

## Layout

```text
csharp/
  src/Yaal/           # library
  tests/Yaal.Tests/   # xUnit parity tests
  examples/Yaal.Example/
```

## Tests

```bash
make test-csharp                 # docker compose run --no-deps (SQLite / unit)
make test-csharp-integration     # docker compose run with DBs + YAAL_INTEGRATION=1
```

Integration URLs inside the container use compose service hostnames (`postgres`, `mysql`, `clickhouse`) via `YAAL_PG_URL` / `YAAL_MYSQL_URL` / `YAAL_CH_URL`.

## Publishing

A GitHub Release tagged `vX.Y.Z` runs [`.github/workflows/nuget-publish.yml`](../.github/workflows/nuget-publish.yml): test, pack, push to nuget.org.

1. Bump `<Version>` in [`src/Yaal/Yaal.csproj`](src/Yaal/Yaal.csproj) (and Python `version` in [`../pyproject.toml`](../pyproject.toml) if you want them aligned).
2. Commit the bump. The workflow file must exist on that commit.
3. Create a GitHub Release tagged `vX.Y.Z` (tag version must match `<Version>`).
