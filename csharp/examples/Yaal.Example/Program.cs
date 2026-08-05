using System.Text.Json;
using Microsoft.Data.Sqlite;
using Yaal;

var repoRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));
var schemaPath = Path.Combine(repoRoot, "docker", "sqlite", "schema.sql");
var apiPath = Path.Combine(repoRoot, "tests", "fixtures", "api");

var dbPath = Path.Combine(Path.GetTempPath(), "yaal-example-" + Guid.NewGuid().ToString("N") + ".db");
try
{
    await using (var con = new SqliteConnection("Data Source=" + dbPath))
    {
        await con.OpenAsync();
        await using var cmd = con.CreateCommand();
        cmd.CommandText = await File.ReadAllTextAsync(schemaPath);
        await cmd.ExecuteNonQueryAsync();
    }

    var y = new Yaal.Yaal(apiPath, debug: true);
    y.SetupDataProvider("db", "sqlite3:///" + dbPath);

    var result = y.Query("user/get", args: new { id = 1 });
    Console.WriteLine(JsonSerializer.Serialize(result, new JsonSerializerOptions { WriteIndented = true }));

    Console.WriteLine("\n-- explain_sql (args.id present) --");
    foreach (var twig in y.ExplainSql("user/get", args: new { id = 1 }))
    {
        Console.WriteLine(twig["sql"]!.ToString()!.Trim());
        Console.WriteLine("binds: " + JsonSerializer.Serialize(twig["parameters"]));
    }
}
finally
{
    try { File.Delete(dbPath); } catch { /* ignore */ }
}
