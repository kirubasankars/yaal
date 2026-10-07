<!--
Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
Use of this source code is governed by a MIT style
license that can be found in the LICENSE file.
-->

# Yaal

**Subtractive SQL→JSON for .NET 8.** You author full SQL (plus JSON shapes). At bind time Yaal **subtracts** unused `optional(...)`, `optional_when(...)`, and `optional_groups_or(...)` / `optional_groups_and(...)` fragments (including empty `WHERE` / `PREWHERE` / `HAVING` cleanup), expands header `integer[]` (and sibling types) for `IN` lists, runs the remaining statements on the provider you pass to `Query`, and shapes flat rows into **nested JSON**.

Yaal is not an additive ORM: no entity tracking, migrations, or query-builder DSL. SQL files stay the source of truth.

Pipeline: *write SQL → subtract optionals → run → shape → JSON*.

## Install

```bash
dotnet add package Yaal
```

The library does not reference a database driver. Your application implements `IDataProvider` around a connection it already opened. The example and the tests open URLs with `Yaal.Drivers.DriverRegistry.Open`.

Requires **.NET 8**. License: [MIT](https://github.com/kirubasankars/yaal/blob/master/LICENSE).

## Usage

Point Yaal at a folder of descriptor operations (`*.sql` plus optional `$.output.json`). Call operations by path:

```csharp
using Yaal;

var y = new Yaal("./api");
var result = y.Query(provider, "user/get", args: new { id = 1 });
string json = y.QueryJson(provider, "user/get", args: new { id = 1 });
```

### Precompiled descriptors

Compile SQL/JSON ahead of time so startup skips lexing sources. Optional-filter elision still runs per request.

**JSON artifacts** (same layout as the Python CLI):

```bash
dotnet run --project src/Yaal.Cli -- compile --api ./api --format json --out ./precompiled
var y = new Yaal("./api", precompiled: "./precompiled");
```

**C# source** (fastest load — register generated `Branch` instances at startup):

```bash
dotnet run --project src/Yaal.Cli -- \
  compile --api ./api --format cs --out Generated/YaalDescriptors --namespace MyApp.Descriptors
```

```csharp
var y = new Yaal("./api");
foreach (var (path, branch) in MyApp.Descriptors.YaalDescriptorRegistry.All)
    y.RegisterDescriptor(path, branch);
```

**In-memory registration** (built or hand-authored descriptors):

```csharp
y.RegisterDescriptor("user/get", myBranch);
y.UnregisterDescriptor("user/get");
```

Load order when `debug=false`: registered → cache → precompiled JSON directory → live SQL/JSON. `debug=true` forces live SQL/JSON and ignores `precompiled`.

### yaal CLI

The `Yaal.Cli` project ships a `compile` command (`--format json|cs`). From the repo:

```bash
dotnet run --project csharp/src/Yaal.Cli -- compile --api ./api --format cs --out ./Generated
```

### Benchmarks

Compare descriptor load cost (live SQL vs JSON precompile vs `RegisterDescriptor`):

```bash
make benchmark-csharp
```

Preview compiled SQL after optional-filter elision:

```csharp
foreach (var twig in y.ExplainSql(provider, "user/get", args: new { id = 1 }))
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

## Provider

An application that already has a connection implements `IDataProvider` (`Begin`, `Execute(sql, parameters)`, `End`, `Error`) and passes that instance to `Query`. See the [C# appendix](https://github.com/kirubasankars/yaal/blob/master/docs/appendix/csharp.md).

```csharp
var result = y.Query(new MyProvider(connection), "user/get", args: new { id = 1 });
```

`Placeholder` is `?` or `%s`. A second database is a second `Query` call with a second provider.

## Documentation

- [Documentation home](https://github.com/kirubasankars/yaal/blob/master/docs/index.md)
- [Tutorial](https://github.com/kirubasankars/yaal/blob/master/docs/tutorial/index.md)
- [C# appendix](https://github.com/kirubasankars/yaal/blob/master/docs/appendix/csharp.md)
- [Descriptor reference](https://github.com/kirubasankars/yaal/blob/master/docs/reference/index.md)
- [Source repository](https://github.com/kirubasankars/yaal)

## Feedback

[Open an issue](https://github.com/kirubasankars/yaal/issues) on GitHub.
