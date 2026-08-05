// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Microsoft.Data.Sqlite;

namespace Yaal.Tests;

public class SqliteIntegrationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly string _flagsPath;
    private readonly Yaal _yaal;

    private static string RepoRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));

    private static string FixtureApi => Path.Combine(RepoRoot, "tests", "fixtures", "api");
    private static string SchemaSql => Path.Combine(RepoRoot, "docker", "sqlite", "schema.sql");
    private static string FlagsSchemaSql => Path.Combine(RepoRoot, "docker", "sqlite", "flags_schema.sql");

    public SqliteIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "yaal-csharp-" + Guid.NewGuid().ToString("N") + ".db");
        _flagsPath = Path.Combine(Path.GetTempPath(), "yaal-csharp-flags-" + Guid.NewGuid().ToString("N") + ".db");

        using (var con = new SqliteConnection("Data Source=" + _dbPath))
        {
            con.Open();
            using var cmd = con.CreateCommand();
            cmd.CommandText = File.ReadAllText(SchemaSql);
            cmd.ExecuteNonQuery();
        }

        using (var con = new SqliteConnection("Data Source=" + _flagsPath))
        {
            con.Open();
            using var cmd = con.CreateCommand();
            cmd.CommandText = File.ReadAllText(FlagsSchemaSql);
            cmd.ExecuteNonQuery();
        }

        _yaal = new Yaal(FixtureApi, debug: true);
        _yaal.SetupDataProvider("db", "sqlite3:///" + _dbPath);
        _yaal.SetupDataProvider("flags", "sqlite3:///" + _flagsPath);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
        try { File.Delete(_flagsPath); } catch { /* ignore */ }
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
    public void User_nested_child_sql_matches_parent_rows()
    {
        var nested = JsonUtil.Serialize(_yaal.Query("user/nested", args: new { id = 1 }));
        var joined = JsonUtil.Serialize(_yaal.Query("user/get", args: new { id = 1 }));
        nested.Should().Be(joined);
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

    [Fact]
    public void User_page_branches()
    {
        var result = _yaal.Query("user/page", args: new { page = 1, page_size = 10 });
        var dict = (Dictionary<string, object?>)result!;
        var paging = (Dictionary<string, object?>)dict["paging"]!;
        AsInt64(paging["page"]).Should().Be(1);
        AsInt64(paging["page_size"]).Should().Be(10);
        AsInt64(paging["total_count"]).Should().Be(2);

        var data = ((System.Collections.IEnumerable)dict["data"]!).Cast<object>().ToList();
        data.Should().HaveCount(2);
        var admin = (Dictionary<string, object?>)data[0];
        admin["name"]!.ToString().Should().Be("admin");
        var roles = ((System.Collections.IEnumerable)admin["roles"]!).Cast<object>().ToList();
        roles.Should().HaveCount(2);
    }

    private static long AsInt64(object? value) => value switch
    {
        long l => l,
        int i => i,
        short s => s,
        byte b => b,
        System.Text.Json.JsonElement je when je.TryGetInt64(out var n) => n,
        _ => Convert.ToInt64(Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)),
    };

    [Fact]
    public void User_create_multi_twig()
    {
        var result = _yaal.Query("user/create", payload: new { id = 3, name = "newbie" });
        var dict = (Dictionary<string, object?>)result!;
        dict["id"].Should().Be(3L);
        dict["name"]!.ToString().Should().Be("newbie");
        var roles = ((System.Collections.IEnumerable)dict["roles"]!).Cast<object>().ToList();
        roles.Should().HaveCount(1);

        var loaded = (Dictionary<string, object?>)_yaal.Query("user/get", args: new { id = 3 })!;
        loaded["name"]!.ToString().Should().Be("newbie");
    }

    [Fact]
    public void Report_summary_with_aggregation()
    {
        var result = _yaal.Query("report/summary");
        var dict = (Dictionary<string, object?>)result!;
        AsInt64(dict["user_count"]).Should().Be(2);
        AsInt64(dict["active_count"]).Should().Be(2);
        AsInt64(dict["assignment_count"]).Should().Be(3);
    }

    [Fact]
    public void User_combine_multi_database()
    {
        var result = _yaal.Query("user/combine", args: new { id = 1 });
        var dict = (Dictionary<string, object?>)result!;
        var app = (Dictionary<string, object?>)dict["app"]!;
        AsInt64(app["id"]).Should().Be(1);
        app["name"]!.ToString().Should().Be("admin");
        var flags = (Dictionary<string, object?>)dict["flags"]!;
        AsInt64(flags["user_id"]).Should().Be(1);
        AsInt64(flags["vip"]).Should().Be(1);
    }
}
