// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Microsoft.Data.Sqlite;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Tests;

public class ParamDefaultTests
{
    private static string RepoRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));

    [Fact]
    public void Parse_string_bare_and_quoted()
    {
        var ast = SqlParser.Parse(
            Lexer.Lex("--($args.sort string = id, note string = 'a,b')--\nselect 1\n"), "$")!;
        ast.Parameters!["$args.sort"].Default.Should().Be("id");
        ast.Parameters!["$args.sort"].HasDefault.Should().BeTrue();
        ast.Parameters!["note"].Default.Should().Be("a,b");
    }

    [Fact]
    public void Parse_integer_float_bool()
    {
        var ast = SqlParser.Parse(
            Lexer.Lex("--(n integer = 1, f float = 1.5, b bool = false)--\nselect 1\n"), "$")!;
        Convert.ToInt64(ast.Parameters!["n"].Default).Should().Be(1);
        Convert.ToDouble(ast.Parameters!["f"].Default).Should().Be(1.5);
        ast.Parameters!["b"].Default.Should().Be(false);
    }

    [Fact]
    public void Parse_required_with_default_rejected()
    {
        Action act = () => SqlParser.Parse(Lexer.Lex("--(n! integer = 1)--\nselect 1\n"), "$");
        act.Should().Throw<Exception>().WithMessage("*cannot have a default*");
    }

    [Fact]
    public void Parse_blob_default_rejected()
    {
        Action act = () => SqlParser.Parse(Lexer.Lex("--(b blob = x)--\nselect 1\n"), "$");
        act.Should().Throw<Exception>().WithMessage("*not supported for blob*");
    }

    [Fact]
    public void Derived_schema_includes_default()
    {
        var y = new Yaal(Path.Combine(RepoRoot, "tests", "fixtures", "api"), debug: true);
        var desc = y.CreateDescriptor("user/list");
        var args = desc.Model!.Args!;
        var props = (Dictionary<string, object?>)args["properties"]!;
        var sort = (Dictionary<string, object?>)props["sort"]!;
        sort["default"].Should().Be("id");
        var dir = (Dictionary<string, object?>)props["dir"]!;
        dir["default"].Should().Be("asc");
    }

    [Fact]
    public void Context_get_prop_uses_default()
    {
        var y = new Yaal(Path.Combine(RepoRoot, "tests", "fixtures", "api"), debug: true);
        var desc = y.CreateDescriptor("user/list");
        var ctx = ContextFactory.CreateContext(desc, args: new { });
        ctx.GetProp("$args.sort").Should().Be("id");
        ctx.GetProp("$args.dir").Should().Be("asc");
        ctx.GetProp("$args.active").Should().BeNull();
    }

    [Fact]
    public void Optional_with_defaulted_arg_keeps_bind()
    {
        var sql = """
            --($args.active integer = 1)--
            select u.user_id from users u
            where 1 = 1 and optional(u.active = {{$args.active}})
            """;
        var twig = SqlParser.Parse(Lexer.Lex(sql), "$")!.SqlStmts![0];
        var args = new Shape(
            schema: new Dictionary<string, object?>
            {
                ["type"] = "object",
                ["properties"] = new Dictionary<string, object?>
                {
                    ["active"] = new Dictionary<string, object?>
                    {
                        ["type"] = "integer",
                        ["default"] = 1L,
                    },
                },
            },
            data: new Dictionary<string, object?>());
        var shape = new Shape(extras: new Dictionary<string, Shape> { ["$args"] = args });
        var compiled = new DataProviderHelper().GetExecutableContent("?", twig, shape);
        compiled.Content.Should().Contain("?");
        compiled.Parameters.Select(p => p.Name).Should().Equal("$args.active");
    }
}

public class ParamDefaultIntegrationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly Yaal _yaal;

    private static string RepoRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));

    public ParamDefaultIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "yaal-defaults-" + Guid.NewGuid().ToString("N") + ".db");
        using (var con = new SqliteConnection("Data Source=" + _dbPath))
        {
            con.Open();
            using var cmd = con.CreateCommand();
            cmd.CommandText = File.ReadAllText(Path.Combine(RepoRoot, "docker", "sqlite", "schema.sql"));
            cmd.ExecuteNonQuery();
        }

        _yaal = new Yaal(Path.Combine(RepoRoot, "tests", "fixtures", "api"), debug: true);
        _yaal.SetupDataProvider("db", "sqlite3:///" + _dbPath);
    }

    public void Dispose()
    {
        try { File.Delete(_dbPath); } catch { /* ignore */ }
    }

    [Fact]
    public void List_default_sort_id_asc()
    {
        var result = _yaal.Query("user/list");
        var rows = ((System.Collections.IEnumerable)result!).Cast<Dictionary<string, object?>>().ToList();
        rows.Select(r => Convert.ToInt64(r["id"])).Should().Equal(1L, 2L);

        var explained = _yaal.ExplainSql("user/list");
        var sql = explained[0]["sql"]!.ToString()!;
        sql.Should().Contain("u.user_id");
        sql.Should().Contain("ASC");
    }
}
