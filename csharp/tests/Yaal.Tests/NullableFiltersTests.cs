using System.Text.Json;
using System.Text.RegularExpressions;
using FluentAssertions;
using Yaal.Sql;

namespace Yaal.Tests;

public class NullableFiltersTests
{
    private static string CasesPath =>
        Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "tests", "fixtures", "sql_compile", "cases.json"));

    private static string NormalizeWs(string sql) =>
        Regex.Replace(sql, @"\s+", " ").Trim();

    [Fact]
    public void Shared_sql_compile_goldens()
    {
        var json = File.ReadAllText(CasesPath);
        using var doc = JsonDocument.Parse(json);
        doc.RootElement.GetArrayLength().Should().BeGreaterThan(0);

        foreach (var caseEl in doc.RootElement.EnumerateArray())
        {
            var name = caseEl.GetProperty("name").GetString()!;
            var sql = caseEl.GetProperty("sql").GetString()!;

            if (caseEl.TryGetProperty("expect_error_contains", out var errEl))
            {
                var needle = errEl.GetString()!;
                Action act = () => SqlParser.Parse(Lexer.Lex(sql), "$");
                act.Should().Throw<Exception>(because: name)
                    .Where(ex => ex.Message.Contains(needle, StringComparison.OrdinalIgnoreCase));
                continue;
            }

            var ast = SqlParser.Parse(Lexer.Lex(sql), "$")!;
            var twig = ast.SqlStmts![0];

            if (caseEl.TryGetProperty("expect_nullable_contains", out var nullableEl))
            {
                foreach (var n in nullableEl.EnumerateArray())
                    twig.Nullable.Should().Contain(n.GetString(), because: name);
            }

            var nulls = caseEl.TryGetProperty("nulls", out var nullsEl)
                ? nullsEl.EnumerateArray().Select(x => x.GetString()!).ToArray()
                : Array.Empty<string>();
            var placeholder = caseEl.TryGetProperty("placeholder", out var phEl)
                ? phEl.GetString() ?? "?"
                : "?";

            var compiled = SqlCompiler.Compile(twig, nulls, placeholder);
            NormalizeWs(compiled.Content).Should().Be(
                NormalizeWs(caseEl.GetProperty("expect_sql").GetString()!),
                because: name);

            var expectParams = caseEl.TryGetProperty("expect_param_names", out var epEl)
                ? epEl.EnumerateArray().Select(x => x.GetString()!).ToArray()
                : Array.Empty<string>();
            compiled.Parameters.Select(p => p.Name).Should().Equal(expectParams, because: name);
        }
    }
}
