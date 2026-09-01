// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Microsoft.Data.Sqlite;
using Yaal.Execution;
using Yaal.Providers;
using Yaal.Sql;

namespace Yaal.Tests;

public class DxApiTests
{
    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..", "tests", "fixtures", "api"));

    private static string SchemaPath =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..", "docker", "sqlite", "schema.sql"));

    private static string SeedTempDb()
    {
        var path = Path.Combine(Path.GetTempPath(), "yaal-test-" + Guid.NewGuid().ToString("n") + ".db");
        using var con = new SqliteConnection("Data Source=" + path);
        con.Open();
        using var cmd = con.CreateCommand();
        cmd.CommandText = File.ReadAllText(SchemaPath);
        cmd.ExecuteNonQuery();
        return path;
    }

    [Fact]
    public void Query_user_list()
    {
        var path = SeedTempDb();
        try
        {
            var y = new Yaal(FixtureApi, debug: true);
            y.SetupDataProvider("db", "sqlite3:///" + path);
            var result = ((System.Collections.IEnumerable)y.Query("user/list")!).Cast<object>().ToList();
            result.Should().HaveCount(2);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Unsupported_scheme_throws()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var act = () => y.SetupDataProvider("db", "oracle://x");
        act.Should().Throw<UnsupportedDatabaseUrlException>();
    }

    [Fact]
    public void Missing_provider_throws()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var act = () => y.GetDataProvider("db");
        act.Should().Throw<YaalException>().WithMessage("*not configured*");
    }

    [Fact]
    public void Missing_descriptor_throws()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var act = () => y.CreateDescriptor("no/such");
        act.Should().Throw<DescriptorNotFoundException>();
    }

    [Fact]
    public void Explain_sql_binds_args_id()
    {
        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", "sqlite3:///");
        var plan = y.ExplainSql("user/get", args: new { id = 1 });
        plan.Should().NotBeEmpty();
        plan[0]["sql"]!.ToString().Should().Contain("?");
        var parameters = (List<object?>)plan[0]["parameters"]!;
        parameters.Should().ContainSingle().Which.Should().Be(1L);
    }

    [Fact]
    public void Registers_app_provider()
    {
        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", new StubContextManager());
        y.GetDataProvider("db").Should().BeOfType<StubDataProvider>();
    }

    [Fact]
    public void Registers_clickhouse_scheme()
    {
        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", "clickhouse://default:@127.0.0.1:8123/default");
        y.GetDataProvider("db").Should().NotBeNull();
    }

    private sealed class StubContextManager : IDataProviderContextManager
    {
        public IDataProvider GetContext() => new StubDataProvider();
    }

    private sealed class StubDataProvider : IDataProvider
    {
        public void Begin() { }
        public void End() { }
        public void Error() { }

        public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
            Twig twig, Shape inputShape, DataProviderHelper helper) =>
            (Array.Empty<IDictionary<string, object?>>(), null);
    }
}
