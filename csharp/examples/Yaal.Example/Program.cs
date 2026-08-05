// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json;
using Microsoft.Data.Sqlite;
using Yaal;

var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));
var schemaPath = Path.Combine(repoRoot, "docker", "sqlite", "schema.sql");
var flagsSchemaPath = Path.Combine(repoRoot, "docker", "sqlite", "flags_schema.sql");
var apiPath = Path.Combine(repoRoot, "tests", "fixtures", "api");

var dbPath = Path.Combine(Path.GetTempPath(), "yaal-example-" + Guid.NewGuid().ToString("N") + ".db");
var flagsPath = Path.Combine(Path.GetTempPath(), "yaal-example-flags-" + Guid.NewGuid().ToString("N") + ".db");
try
{
    await using (var con = new SqliteConnection("Data Source=" + dbPath))
    {
        await con.OpenAsync();
        await using var cmd = con.CreateCommand();
        cmd.CommandText = await File.ReadAllTextAsync(schemaPath);
        await cmd.ExecuteNonQueryAsync();
    }

    await using (var con = new SqliteConnection("Data Source=" + flagsPath))
    {
        await con.OpenAsync();
        await using var cmd = con.CreateCommand();
        cmd.CommandText = await File.ReadAllTextAsync(flagsSchemaPath);
        await cmd.ExecuteNonQueryAsync();
    }

    var y = new Yaal.Yaal(apiPath, debug: true);
    y.SetupDataProvider("db", "sqlite3:///" + dbPath);
    y.SetupDataProvider("flags", "sqlite3:///" + flagsPath);

    var opts = new JsonSerializerOptions { WriteIndented = true };

    void Print(string title, object? value)
    {
        Console.WriteLine($"-- {title} --");
        Console.WriteLine(JsonSerializer.Serialize(value, opts));
        Console.WriteLine();
    }

    Print("user/get id=1", y.Query("user/get", args: new { id = 1 }));
    Print("user/nested id=1", y.Query("user/nested", args: new { id = 1 }));
    Print("user/list active=1", y.Query("user/list", args: new { active = 1 }));
    Print(
        "user/page page=1 page_size=1",
        y.Query("user/page", args: new { page = 1, page_size = 1 }));
    Print("report/summary", y.Query("report/summary"));
    Print("user/combine id=1", y.Query("user/combine", args: new { id = 1 }));
    Print(
        "user/create payload id=3 name=newbie",
        y.Query("user/create", payload: new { id = 3, name = "newbie" }));

    Console.WriteLine("-- explain user/list (active omitted) --");
    foreach (var twig in y.ExplainSql("user/list"))
    {
        Console.WriteLine(twig["sql"]!.ToString()!.Trim());
        Console.WriteLine("binds: " + JsonSerializer.Serialize(twig["parameters"]));
        Console.WriteLine();
    }

    Console.WriteLine("-- explain user/list active=1 --");
    foreach (var twig in y.ExplainSql("user/list", args: new { active = 1 }))
    {
        Console.WriteLine(twig["sql"]!.ToString()!.Trim());
        Console.WriteLine("binds: " + JsonSerializer.Serialize(twig["parameters"]));
        Console.WriteLine();
    }
}
finally
{
    try { File.Delete(dbPath); } catch { /* ignore */ }
    try { File.Delete(flagsPath); } catch { /* ignore */ }
}
