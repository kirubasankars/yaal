using FluentAssertions;
using Microsoft.Data.Sqlite;

namespace Yaal.Tests;

public class SqliteIntegrationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly Yaal _yaal;

    private static string RepoRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));

    private static string FixtureApi => Path.Combine(RepoRoot, "tests", "fixtures", "api");
    private static string SchemaSql => Path.Combine(RepoRoot, "docker", "sqlite", "schema.sql");

    public SqliteIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "yaal-csharp-" + Guid.NewGuid().ToString("N") + ".db");
        using var con = new SqliteConnection("Data Source=" + _dbPath);
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = File.ReadAllText(SchemaSql);
        cmd.ExecuteNonQuery();

        _yaal = new Yaal(FixtureApi, debug: true);
        _yaal.SetupDataProvider("db", "sqlite3:///" + _dbPath);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
    }

    [Fact]
    public void User_with_nested_roles()
    {
        var result = _yaal.Query("user/get", args: new { id = 1 });
        var json = JsonUtil.Serialize(result);
        json.Should().Contain("\"admin\"");
        json.Should().Contain("Administrator");
        json.Should().Contain("User");

        var dict = (Dictionary<string, object?>)result!;
        dict["id"].Should().Be(1L);
        dict["name"]!.ToString().Should().Be("admin");
        var roles = ((System.Collections.IEnumerable)dict["roles"]!).Cast<object>().ToList();
        roles.Should().HaveCount(2);
    }

    [Fact]
    public void User_with_single_role()
    {
        var result = _yaal.Query("user/get", args: new { id = 2 });
        var dict = (Dictionary<string, object?>)result!;
        dict["id"].Should().Be(2L);
        dict["name"]!.ToString().Should().Be("guest");
        var roles = ((System.Collections.IEnumerable)dict["roles"]!).Cast<object>().ToList();
        roles.Should().HaveCount(1);
    }

    [Fact]
    public void Query_json_returns_string()
    {
        var json = _yaal.QueryJson("user/get", args: new { id = 1 });
        json.Should().StartWith("{");
        json.Should().Contain("admin");
    }
}
