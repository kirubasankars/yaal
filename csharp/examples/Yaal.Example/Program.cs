// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Microsoft.Data.Sqlite;
using Yaal;

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
var flagsSchema = Path.Combine(root, "docker", "sqlite", "flags_schema.sql");

var dbPath = Path.Combine(Path.GetTempPath(), "yaal-example-" + Guid.NewGuid().ToString("N") + ".db");
var flagsPath = Path.Combine(Path.GetTempPath(), "yaal-example-flags-" + Guid.NewGuid().ToString("N") + ".db");

try
{
    await SeedAsync(dbPath, schema);
    await SeedAsync(flagsPath, flagsSchema);

    var y = new Yaal.Yaal(api, debug: true);
    y.SetupDataProvider("db", "sqlite3:///" + dbPath);
    y.SetupDataProvider("flags", "sqlite3:///" + flagsPath);

    Print("user/get id=1", y.Query("user/get", args: new { id = 1 }));
    Print("user/nested id=1", y.Query("user/nested", args: new { id = 1 }));
    Print("user/list active=1", y.Query("user/list", args: new { active = 1 }));
    Print("user/list sort=name dir=desc", y.Query("user/list", args: new { sort = "name", dir = "desc" }));
    Print(
        "user/list sort=name,id dir=desc,asc (multi-column)",
        y.Query("user/list", args: new { sort = "name,id", dir = "desc,asc" }));
    Print("user/page page=1 page_size=1", y.Query("user/page", args: new { page = 1, page_size = 1 }));
    Print("report/summary", y.Query("report/summary"));
    Print("user/combine id=1", y.Query("user/combine", args: new { id = 1 }));

    Console.WriteLine("-- explain user/list (active omitted → optional elided) --");
    foreach (var twig in y.ExplainSql("user/list"))
    {
        Console.WriteLine(twig["sql"]?.ToString()?.Trim());
        Console.WriteLine("binds: " + JsonUtil.Serialize(twig["parameters"]));
        Console.WriteLine();
    }

    Console.WriteLine("-- explain user/list active=1 --");
    foreach (var twig in y.ExplainSql("user/list", args: new { active = 1 }))
    {
        Console.WriteLine(twig["sql"]?.ToString()?.Trim());
        Console.WriteLine("binds: " + JsonUtil.Serialize(twig["parameters"]));
        Console.WriteLine();
    }
}
finally
{
    try { File.Delete(dbPath); } catch { /* ignore */ }
    try { File.Delete(flagsPath); } catch { /* ignore */ }
}
