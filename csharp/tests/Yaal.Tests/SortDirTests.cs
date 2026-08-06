// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.RegularExpressions;
using FluentAssertions;
using Microsoft.Data.Sqlite;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Tests;

public class SortDirTests
{
    private const string ListSql = """
        --($args.sort string, $args.dir string, $args.active integer)--
        select u.user_id, u.user_name from users u
        where 1 = 1
          and optional(u.active = {{$args.active}})
        order by
          sort({{$args.sort}}, name = u.user_name, id = u.user_id)
          dir({{$args.dir}})
        """;

    private static string Normalize(string sql) =>
        Regex.Replace(sql, @"\s+", " ").Trim();

    private static Twig Twig(string sql = ListSql) =>
        SqlParser.Parse(Lexer.Lex(sql), "$")!.SqlStmts![0];

    private static Shape ArgsShape(string? sort = null, string? dir = null, int? active = null)
    {
        var data = new Dictionary<string, object?>();
        if (sort != null) data["sort"] = sort;
        if (dir != null) data["dir"] = dir;
        if (active != null) data["active"] = active;
        var args = new Shape(data: data);
        return new Shape(extras: new Dictionary<string, Shape> { ["$args"] = args });
    }

    private static void ExpectParseError(string fragment, string needle)
    {
        var sql = "--(a string)--\nselect 1 order by " + fragment + "\n";
        Action act = () => SqlParser.Parse(Lexer.Lex(sql), "$");
        act.Should().Throw<Exception>().Where(ex =>
            ex.Message.Contains(needle, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Parse_sort_empty() => ExpectParseError("sort()", "empty");

    [Fact]
    public void Parse_sort_missing_param() => ExpectParseError("sort(name = x)", "{{param}}");

    [Fact]
    public void Parse_sort_two_params() => ExpectParseError("sort({{a}}, {{b}}, name = x)", "exactly one");

    [Fact]
    public void Parse_sort_zero_pairs() => ExpectParseError("sort({{a}})", "at least one");

    [Fact]
    public void Parse_duplicate_keys() => ExpectParseError("sort({{a}}, name = x, name = y)", "duplicate");

    [Fact]
    public void Parse_dir_empty() => ExpectParseError("dir()", "empty");

    [Fact]
    public void Parse_dir_with_pairs() => ExpectParseError("dir({{a}}, name = x)", "does not accept");

    [Fact]
    public void Parse_unclosed_sort()
    {
        Action act = () => SqlParser.Parse(
            Lexer.Lex("--(a string)--\nselect 1 order by sort({{a}}, name = x\n"), "$");
        act.Should().Throw<Exception>().WithMessage("*unclosed*");
    }

    [Fact]
    public void Parse_nested_sort() => ExpectParseError("sort(sort({{a}}, name = x), id = y)", "nested");

    [Fact]
    public void Parse_key_illegal_chars() => ExpectParseError("sort({{a}}, bad-key = x)", "word characters");

    [Fact]
    public void Parse_mixed_static_order_by_rejected()
    {
        Action act = () => SqlParser.Parse(
            Lexer.Lex("--(s string)--\nselect 1 order by sort({{s}}, a = x), y\n"), "$");
        act.Should().Throw<Exception>().WithMessage("*must not include other terms*");
    }

    [Fact]
    public void Parse_sort_outside_order_by_allowed()
    {
        var ast = SqlParser.Parse(Lexer.Lex("--(s string)--\nselect sort({{s}}, a = x) from t\n"), "$")!;
        ast.SqlStmts![0].Content.Should().Contain(t => t.Type == "sort");
    }

    [Fact]
    public void Resolve_sort_name_and_dir_desc()
    {
        var helper = new DataProviderHelper();
        var compiled = helper.GetExecutableContent("?", Twig(), ArgsShape(sort: "name", dir: "desc"));
        var n = Normalize(compiled.Content);
        n.Should().Contain("u.user_name");
        n.Should().Contain("DESC");
        n.Should().NotContain("sort(");
    }

    [Fact]
    public void Resolve_sort_case_insensitive()
    {
        var (sortMap, _) = SortDirDesugar.ResolveValues(Twig(), ArgsShape(sort: "NAME"));
        sortMap["$args.sort"].Should().Be("u.user_name");
    }

    [Fact]
    public void Resolve_elide_order_by_when_sort_null()
    {
        var helper = new DataProviderHelper();
        var compiled = helper.GetExecutableContent("?", Twig(), ArgsShape(dir: "desc"));
        compiled.Content.ToLowerInvariant().Should().NotContain("order by");
    }

    [Fact]
    public void Resolve_optional_and_sort_both_null()
    {
        var helper = new DataProviderHelper();
        var compiled = helper.GetExecutableContent("?", Twig(), ArgsShape());
        var n = Normalize(compiled.Content).ToLowerInvariant();
        n.Should().NotContain("where");
        n.Should().NotContain("order by");
    }

    [Fact]
    public void Resolve_unknown_key_throws()
    {
        Action act = () => SortDirDesugar.ResolveValues(Twig(), ArgsShape(sort: "nope"));
        act.Should().Throw<SortDirException>().WithMessage("*unknown sort key*");
    }

    [Fact]
    public void Resolve_injection_key_throws()
    {
        Action act = () => SortDirDesugar.ResolveValues(Twig(), ArgsShape(sort: "id; drop table"));
        act.Should().Throw<SortDirException>();
    }

    [Fact]
    public void Resolve_empty_and_whitespace_soft_error()
    {
        Action empty = () => SortDirDesugar.ResolveValues(Twig(), ArgsShape(sort: ""));
        Action ws = () => SortDirDesugar.ResolveValues(Twig(), ArgsShape(sort: "   "));
        empty.Should().Throw<SortDirException>();
        ws.Should().Throw<SortDirException>();
    }

    [Theory]
    [InlineData("ascending")]
    [InlineData("1")]
    [InlineData("desc;")]
    public void Resolve_bad_dir_soft_error(string dir)
    {
        Action act = () => SortDirDesugar.ResolveValues(Twig(), ArgsShape(sort: "id", dir: dir));
        act.Should().Throw<SortDirException>().WithMessage("*unknown sort direction*");
    }

    [Fact]
    public void Resolve_multi_token_expr()
    {
        var sql = """
            --(s string, d string)--
            select 1 order by
              sort({{s}}, name = lower(u.user_name))
              dir({{d}})
            """;
        var twig = Twig(sql);
        var shape = new Shape(data: new Dictionary<string, object?> { ["s"] = "name", ["d"] = "asc" });
        var (sortMap, dirMap) = SortDirDesugar.ResolveValues(twig, shape);
        var compiled = SqlCompiler.Compile(twig, Array.Empty<string>(), "?", sortMap, dirMap);
        compiled.Content.Should().Contain("lower(u.user_name)");
    }

    [Theory]
    [InlineData("x'--")]
    [InlineData("name\0")]
    [InlineData("u.user_name")]
    public void Resolve_security_keys_soft_error(string key)
    {
        Action act = () => SortDirDesugar.ResolveValues(Twig(), ArgsShape(sort: key));
        act.Should().Throw<SortDirException>();
    }

    [Fact]
    public void Cache_different_sort_keys()
    {
        var helper = new DataProviderHelper();
        var twig = Twig();
        var a = helper.GetExecutableContent("?", twig, ArgsShape(sort: "name"));
        var b = helper.GetExecutableContent("?", twig, ArgsShape(sort: "id"));
        a.Content.Should().NotBe(b.Content);
    }

    [Fact]
    public void Cache_same_sort_dir_hit()
    {
        var helper = new DataProviderHelper();
        var twig = Twig();
        var a = helper.GetExecutableContent("?", twig, ArgsShape(sort: "name", dir: "desc"));
        var b = helper.GetExecutableContent("?", twig, ArgsShape(sort: "name", dir: "desc"));
        a.Content.Should().Be(b.Content);
    }
}

public class SortDirIntegrationTests : IDisposable
{
    private readonly string _dbPath;
    private readonly Yaal _yaal;

    private static string RepoRoot =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", ".."));

    public SortDirIntegrationTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "yaal-sort-" + Guid.NewGuid().ToString("N") + ".db");
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
    public void List_sort_id_desc()
    {
        var result = _yaal.Query("user/list", args: new { sort = "id", dir = "desc" });
        var rows = ((System.Collections.IEnumerable)result!).Cast<Dictionary<string, object?>>().ToList();
        rows.Select(r => Convert.ToInt64(r["id"])).Should().Equal(2L, 1L);
    }

    [Fact]
    public void Unknown_sort_soft_errors()
    {
        var result = _yaal.Query("user/list", args: new { sort = "nope" });
        var dict = (Dictionary<string, object?>)result!;
        dict.Should().ContainKey("errors");
    }

    [Fact]
    public void Explain_shows_resolved_order_by()
    {
        var explained = _yaal.ExplainSql("user/list", args: new { sort = "name", dir = "desc" });
        var sql = explained[0]["sql"]!.ToString()!;
        sql.Should().Contain("u.user_name");
        sql.Should().Contain("DESC");
    }
}
