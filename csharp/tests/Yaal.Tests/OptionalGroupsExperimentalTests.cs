// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json;
using System.Text.RegularExpressions;
using FluentAssertions;
using Yaal.Execution;
using Yaal.Sql;

namespace Yaal.Tests;

public class OptionalGroupsExperimentalTests
{
    private static string CasesPath =>
        Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "tests", "fixtures", "sql_compile", "optional_groups_experimental.json"));

    private static string NormalizeWs(string sql) =>
        Regex.Replace(sql, @"\s+", " ").Trim();

    [Fact]
    public void Experimental_sql_compile_goldens()
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

            if (caseEl.TryGetProperty("expect_group_fields", out var gfEl))
            {
                var open = twig.Content.FirstOrDefault(t =>
                    t.Type == "brace" && t.Value == "(" && t.GroupSource != null);
                open.Should().NotBeNull(because: name);
                var expected = gfEl.EnumerateArray().Select(x => x.GetString()!).ToList();
                open!.GroupFields.Should().Equal(expected, because: name);
            }

            var nulls = caseEl.TryGetProperty("nulls", out var nullsEl)
                ? nullsEl.EnumerateArray().Select(x => x.GetString()!).ToArray()
                : Array.Empty<string>();
            var placeholder = caseEl.TryGetProperty("placeholder", out var phEl)
                ? phEl.GetString() ?? "?"
                : "?";

            var arrayLengths = ReadIntMap(caseEl, "array_lengths");
            var groupCounts = ReadIntMap(caseEl, "group_counts");
            var groupFieldLengths = ReadGroupFieldLengths(caseEl);

            Dictionary<string, string?>? sortMap = null;
            if (caseEl.TryGetProperty("sort_map", out var smEl))
            {
                sortMap = new Dictionary<string, string?>(StringComparer.Ordinal);
                foreach (var prop in smEl.EnumerateObject())
                    sortMap[prop.Name] = prop.Value.GetString();
            }

            if (caseEl.TryGetProperty("expect_compile_error_contains", out var compileErrEl))
            {
                var needle = compileErrEl.GetString()!;
                Action act = () => SqlCompiler.Compile(
                    twig, nulls, placeholder, sortMap, arrayLengths, groupCounts, groupFieldLengths);
                act.Should().Throw<Exception>(because: name)
                    .Where(ex => ex.Message.Contains(needle, StringComparison.OrdinalIgnoreCase));
                continue;
            }

            var compiled = SqlCompiler.Compile(
                twig, nulls, placeholder, sortMap, arrayLengths, groupCounts, groupFieldLengths);
            NormalizeWs(compiled.Content).Should().Be(
                NormalizeWs(caseEl.GetProperty("expect_sql").GetString()!),
                because: name);

            var expectParams = caseEl.TryGetProperty("expect_param_names", out var epEl)
                ? epEl.EnumerateArray().Select(x => x.GetString()!).ToArray()
                : Array.Empty<string>();
            compiled.Parameters.Select(p => p.Name).Should().Equal(expectParams, because: name);
        }
    }

    private static Dictionary<string, int> ReadIntMap(JsonElement caseEl, string propName)
    {
        var map = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        if (!caseEl.TryGetProperty(propName, out var el))
            return map;
        foreach (var prop in el.EnumerateObject())
            map[prop.Name] = prop.Value.GetInt32();
        return map;
    }

    private static Dictionary<string, List<Dictionary<string, int>>> ReadGroupFieldLengths(JsonElement caseEl)
    {
        var map = new Dictionary<string, List<Dictionary<string, int>>>(StringComparer.OrdinalIgnoreCase);
        if (!caseEl.TryGetProperty("group_field_lengths", out var gflEl))
            return map;
        foreach (var srcProp in gflEl.EnumerateObject())
        {
            var rows = new List<Dictionary<string, int>>();
            foreach (var rowEl in srcProp.Value.EnumerateArray())
            {
                var row = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                foreach (var fieldProp in rowEl.EnumerateObject())
                    row[fieldProp.Name] = fieldProp.Value.GetInt32();
                rows.Add(row);
            }
            map[srcProp.Name] = rows;
        }
        return map;
    }
}
