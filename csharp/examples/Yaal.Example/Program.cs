// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Microsoft.Data.Sqlite;
using Yaal;
using Yaal.Drivers;

static string RepoRoot() =>
    Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));

static void Print(string title, object? value)
{
    Console.WriteLine($"-- {title} --");
    Console.WriteLine(JsonUtil.Serialize(value));
    Console.WriteLine();
}

static async Task SeedAsync(string dbPath, string schemaPath)
{
    await using var con = new SqliteConnection("Data Source=" + dbPath);
    await con.OpenAsync();
    await using var cmd = con.CreateCommand();
    cmd.CommandText = await File.ReadAllTextAsync(schemaPath);
    await cmd.ExecuteNonQueryAsync();
}

var root = RepoRoot();
var api = Path.Combine(root, "tests", "fixtures", "api");
var schema = Path.Combine(root, "docker", "sqlite", "schema.sql");

var dbPath = Path.Combine(Path.GetTempPath(), "yaal-example-" + Guid.NewGuid().ToString("N") + ".db");

try
{
    await SeedAsync(dbPath, schema);

    var y = new Yaal.Yaal(api, debug: true);
    var db = DriverRegistry.Open("sqlite3:///" + dbPath);

    Print("user/get id=1", y.Query(db, "user/get", args: new { id = 1 }));
    Print("user/nested id=1", y.Query(db, "user/nested", args: new { id = 1 }));
    Print("user/list active=1", y.Query(db, "user/list", args: new { active = 1 }));
    Print("user/list sort=name dir=desc", y.Query(db, "user/list", args: new { sort = "name", dir = "desc" }));
    Print(
        "user/list sort=name,id dir=desc,asc (multi-column)",
        y.Query(db, "user/list", args: new { sort = "name,id", dir = "desc,asc" }));
    Print("user/page page=1 page_size=1", y.Query(db, "user/page", args: new { page = 1, page_size = 1 }));
    Print("report/summary", y.Query(db, "report/summary"));

    Console.WriteLine("-- explain user/list (active omitted → optional elided) --");
    foreach (var twig in y.ExplainSql(db, "user/list"))
    {
        Console.WriteLine(twig["sql"]?.ToString()?.Trim());
        Console.WriteLine("binds: " + JsonUtil.Serialize(twig["parameters"]));
        Console.WriteLine();
    }

    Console.WriteLine("-- explain user/list active=1 --");
    foreach (var twig in y.ExplainSql(db, "user/list", args: new { active = 1 }))
    {
        Console.WriteLine(twig["sql"]?.ToString()?.Trim());
        Console.WriteLine("binds: " + JsonUtil.Serialize(twig["parameters"]));
        Console.WriteLine();
    }
}
finally
{
    try { File.Delete(dbPath); } catch { /* ignore */ }
}
