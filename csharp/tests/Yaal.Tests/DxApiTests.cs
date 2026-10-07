// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Microsoft.Data.Sqlite;
using Yaal.Drivers;
using Yaal.Providers;

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
            var db = DriverRegistry.Open("sqlite3:///" + path);
            var result = ((System.Collections.IEnumerable)y.Query(db, "user/list")!).Cast<object>().ToList();
            result.Should().HaveCount(2);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void App_provider_leaves_connection_open()
    {
        var path = SeedTempDb();
        using var connection = new SqliteConnection("Data Source=" + path);
        connection.Open();
        try
        {
            var y = new Yaal(FixtureApi, debug: true);
            var result = (IDictionary<string, object?>)y.Query(
                new OpenConnectionProvider(connection), "user/get", args: new { id = 1 })!;
            result["id"].Should().Be(1L);
            connection.State.Should().Be(System.Data.ConnectionState.Open);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "select 1";
            cmd.ExecuteScalar().Should().Be(1L);
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void Unsupported_scheme_throws()
    {
        var act = () => DriverRegistry.Open("oracle://x");
        act.Should().Throw<UnsupportedDatabaseUrlException>();
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
        var plan = y.ExplainSql(DriverRegistry.Open("sqlite3:///"), "user/get", args: new { id = 1 });
        plan.Should().NotBeEmpty();
        plan[0]["sql"]!.ToString().Should().Contain("?");
        var parameters = (List<object?>)plan[0]["parameters"]!;
        parameters.Should().ContainSingle().Which.Should().Be(1L);
    }

    [Fact]
    public void Opens_clickhouse_provider()
    {
        DriverRegistry.Open("clickhouse://default:@127.0.0.1:8123/default")
            .Placeholder.Should().Be("%s");
    }

    private sealed class OpenConnectionProvider : IDataProvider
    {
        private readonly SqliteConnection _connection;

        public OpenConnectionProvider(SqliteConnection connection) => _connection = connection;

        public string Placeholder => "?";

        public void Begin() { }

        public void End() { }

        public void Error() { }

        public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
            string sql, IReadOnlyList<object?> parameters)
        {
            using var cmd = _connection.CreateCommand();
            cmd.CommandText = sql;
            for (var i = 0; i < parameters.Count; i++)
            {
                var p = cmd.CreateParameter();
                p.ParameterName = "$p" + i;
                p.Value = parameters[i] ?? DBNull.Value;
                cmd.Parameters.Add(p);
            }

            if (parameters.Count > 0)
            {
                var parts = sql.Split('?');
                if (parts.Length - 1 == parameters.Count)
                {
                    var rendered = parts[0];
                    for (var i = 0; i < parameters.Count; i++)
                        rendered += "$p" + i + parts[i + 1];
                    cmd.CommandText = rendered;
                }
            }

            var rows = new List<IDictionary<string, object?>>();
            using var reader = cmd.ExecuteReader();
            if (reader.FieldCount > 0)
            {
                while (reader.Read())
                {
                    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < reader.FieldCount; i++)
                        row[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    rows.Add(row);
                }
            }

            return (rows, null);
        }
    }
}
