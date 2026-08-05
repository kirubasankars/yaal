using System.Text.RegularExpressions;
using FluentAssertions;
using Yaal.Sql;

namespace Yaal.Tests;

public class NullableFiltersTests
{
    private static CompiledSql Compile(string sql, IEnumerable<string> nulls, string placeholder = "?")
    {
        var ast = SqlParser.Parse(Lexer.Lex(sql), "$")!;
        var twig = ast.SqlStmts![0];
        return SqlCompiler.Compile(twig, nulls, placeholder);
    }

    private static string NormalizeWs(string sql) =>
        Regex.Replace(sql, @"\s+", " ").Trim();

    [Fact]
    public void Null_strips_or_after_one_equals_one()
    {
        var sql = """
                  --(param1 integer)--
                  select * from a where 1 = 1 or ({{param1}} is null or col1 = {{param1}})
                  """;
        var compiled = Compile(sql, new[] { "param1" });
        NormalizeWs(compiled.Content).Should().Be("select * from a where 1 = 1");
        compiled.Parameters.Should().BeEmpty();
    }

    [Fact]
    public void Null_strips_and_after_predicate()
    {
        var sql = """
                  --(p integer)--
                  select * from a where a = 1 and ({{p}} is null or col = {{p}})
                  """;
        var compiled = Compile(sql, new[] { "p" });
        NormalizeWs(compiled.Content).Should().Be("select * from a where a = 1");
        compiled.Parameters.Should().BeEmpty();
    }

    [Fact]
    public void Sole_nullable_predicate_falls_back_to_one_equals_one()
    {
        var sql = """
                  --(p integer)--
                  select * from a where ({{p}} is null or col = {{p}})
                  """;
        var compiled = Compile(sql, new[] { "p" });
        NormalizeWs(compiled.Content).Should().Be("select * from a where 1 = 1");
        compiled.Parameters.Should().BeEmpty();
    }

    [Fact]
    public void Non_null_keeps_binds_and_rewrites_is_null()
    {
        var sql = """
                  --(param1 integer)--
                  select * from a where 1 = 1 or ({{param1}} is null or col1 = {{param1}})
                  """;
        var compiled = Compile(sql, Array.Empty<string>());
        NormalizeWs(compiled.Content).Should().Be("select * from a where 1 = 1 or (1 = 2 or col1 = ?)");
        compiled.Parameters.Should().HaveCount(1);
        compiled.Parameters[0].Name.Should().Be("param1");
    }

    [Fact]
    public void Elided_params_omitted_from_bind_list()
    {
        var sql = """
                  --(a integer, b integer)--
                  select * from t where col = {{a}} and ({{b}} is null or other = {{b}})
                  """;
        var compiled = Compile(sql, new[] { "b" });
        NormalizeWs(compiled.Content).Should().Be("select * from t where col = ?");
        compiled.Parameters.Select(p => p.Name).Should().Equal("a");
    }

    [Fact]
    public void Nullable_name_match_is_case_insensitive()
    {
        var sql = """
                  --(Param1 integer)--
                  select * from a where 1 = 1 OR ({{Param1}} is null or col1 = {{Param1}})
                  """;
        var ast = SqlParser.Parse(Lexer.Lex(sql), "$")!;
        var twig = ast.SqlStmts![0];
        twig.Nullable.Should().Contain("param1");
        var compiled = SqlCompiler.Compile(twig, new[] { "PARAM1" }, "?");
        NormalizeWs(compiled.Content).Should().Be("select * from a where 1 = 1");
        compiled.Parameters.Should().BeEmpty();
    }

    [Fact]
    public void Args_id_style_filter_like_fixture()
    {
        var sql = """
                  --($args.id integer)--
                  select * from users u
                  where u.active = 1
                    and r.active = 1
                    and ({{$args.id}} is null or u.user_id = {{$args.id}})
                  order by u.user_id
                  """;
        var compiled = Compile(sql, new[] { "$args.id" });
        NormalizeWs(compiled.Content).Should().Be(
            "select * from users u where u.active = 1 and r.active = 1 order by u.user_id");
        compiled.Parameters.Should().BeEmpty();
    }

    [Fact]
    public void Optional_sugar_null_strips_and()
    {
        var sql = """
                  --(p integer)--
                  select * from a where a = 1 and optional(col = {{p}})
                  """;
        var compiled = Compile(sql, new[] { "p" });
        NormalizeWs(compiled.Content).Should().Be("select * from a where a = 1");
        compiled.Parameters.Should().BeEmpty();
    }

    [Fact]
    public void Optional_sugar_non_null_keeps_binds()
    {
        var sql = """
                  --(param1 integer)--
                  select * from a where 1 = 1 or optional(col1 = {{param1}})
                  """;
        var compiled = Compile(sql, Array.Empty<string>());
        NormalizeWs(compiled.Content).Should().Be("select * from a where 1 = 1 or (1 = 2 or col1 = ?)");
        compiled.Parameters.Should().HaveCount(1);
        compiled.Parameters[0].Name.Should().Be("param1");
    }

    [Fact]
    public void Optional_sugar_sole_predicate_falls_back()
    {
        var sql = """
                  --(p integer)--
                  select * from a where optional(col = {{p}})
                  """;
        var compiled = Compile(sql, new[] { "p" });
        NormalizeWs(compiled.Content).Should().Be("select * from a where 1 = 1");
        compiled.Parameters.Should().BeEmpty();
    }

    [Fact]
    public void Optional_sugar_args_id_fixture_style()
    {
        var sql = """
                  --($args.id integer)--
                  select * from users u
                  where u.active = 1
                    and r.active = 1
                    and optional(u.user_id = {{$args.id}})
                  order by u.user_id
                  """;
        var compiled = Compile(sql, new[] { "$args.id" });
        NormalizeWs(compiled.Content).Should().Be(
            "select * from users u where u.active = 1 and r.active = 1 order by u.user_id");
        compiled.Parameters.Should().BeEmpty();
    }
}
