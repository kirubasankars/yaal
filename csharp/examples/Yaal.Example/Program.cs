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

    var opts = new JsonSerializerOptions { WriteIndented = true };

    Console.WriteLine("-- user/get --");
    Console.WriteLine(JsonSerializer.Serialize(y.Query("user/get", args: new { id = 1 }), opts));

    Console.WriteLine("\n-- user/page --");
    Console.WriteLine(JsonSerializer.Serialize(
        y.Query("user/page", args: new { page = 1, page_size = 10 }), opts));

    Console.WriteLine("\n-- explain_sql user/get (args.id present) --");
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
