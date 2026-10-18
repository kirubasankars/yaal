// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json;
using Yaal;

const string Sql = """
    --(id, active)--
    select * from users u
    where optional(u.id = {{id}})
      and optional(u.active = {{active}})
    """;

const string Partial = """
    --(id, active)--
    select * from users u
    where optional(u.id = {{id}} and u.active = {{active}})
    """;

var compiled = Compiler.Compile(Sql, new Dictionary<string, int> { ["id"] = 1 });
Expect(compiled.Statements.Count == 1, "one statement");
Expect(Norm(compiled.Statements[0].Sql) == "select * from users u where (u.id = ?)", compiled.Statements[0].Sql);
var parameter = compiled.Statements[0].Parameters[0];
Expect(parameter.Name == "id" && parameter.Type == "integer" && parameter.Value.GetInt32() == 1, parameter.ToString() ?? "");

var expanded = Compiler.Compile(
    "--(id)--\nselect * from t where id in ({{id}}...)",
    new Dictionary<string, int[]> { ["id"] = [1, 2] });
var expandedValues = expanded.Statements[0].Values();
Expect(expandedValues.Length == 2 && expandedValues[0] is long first && first == 1 && expandedValues[1] is long second && second == 2,
    string.Join(",", expandedValues));

var numbered = Compiler.Compile(Sql, new Dictionary<string, int> { ["id"] = 1 }, "numbered");
Expect(numbered.Statements[0].Sql.Contains("$1") && !numbered.Statements[0].Sql.Contains('?'), numbered.Statements[0].Sql);

try
{
    Compiler.Compile(Partial, new Dictionary<string, int> { ["id"] = 1 });
    throw new Exception("partial optional compiled");
}
catch (CompileException ex)
{
    Expect(ex.Message.Contains("partial", StringComparison.OrdinalIgnoreCase), ex.Message);
}

try
{
    Compiler.Compile("--(id)--\nselect {{id}}\0 or 1 = 1", new Dictionary<string, int> { ["id"] = 1 });
    throw new Exception("NUL in sql compiled");
}
catch (CompileException ex)
{
    Expect(ex.Message.Contains("NUL", StringComparison.Ordinal), ex.Message);
}

using var cases = JsonDocument.Parse(File.ReadAllText(CasesPath()));
foreach (var item in cases.RootElement.EnumerateArray())
{
    var name = item.GetProperty("name").GetString() ?? "";
    var sql = item.GetProperty("sql").GetString() ?? "";
    var parameters = item.GetProperty("params");
    if (item.TryGetProperty("expect_error_contains", out var needleEl))
    {
        var needle = needleEl.GetString() ?? "";
        CompileException? caught = null;
        try
        {
            Compiler.Compile(sql, parameters);
        }
        catch (CompileException ex)
        {
            caught = ex;
        }
        if (caught is null || !caught.Message.Contains(needle, StringComparison.OrdinalIgnoreCase))
            throw new Exception($"{name}: expected {needle} in {caught?.Message}");
        continue;
    }
    var compiledCase = Compiler.Compile(sql, parameters);
    if (compiledCase.Statements.Count != 1)
        throw new Exception($"{name}: {compiledCase.Statements.Count} statements");
    var statement = compiledCase.Statements[0];
    var expectSql = item.GetProperty("expect_sql").GetString() ?? "";
    if (Norm(statement.Sql) != Norm(expectSql))
        throw new Exception($"{name}:\n  got {Norm(statement.Sql)}\n  exp {Norm(expectSql)}");
    var names = string.Join(",", statement.Parameters.Select(parameter => parameter.Name));
    var expectNames = item.TryGetProperty("expect_param_names", out var expectEl)
        ? string.Join(",", expectEl.EnumerateArray().Select(value => value.GetString()))
        : "";
    if (names != expectNames)
        throw new Exception($"{name} names: {names} exp {expectNames}");
}

Console.WriteLine("ok");

static string CasesPath()
{
    foreach (var start in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
    {
        var dir = new DirectoryInfo(start);
        while (dir is not null)
        {
            var candidate = Path.Combine(dir.FullName, "tests", "fixtures", "sql_compile", "cases.json");
            if (File.Exists(candidate))
                return candidate;
            dir = dir.Parent;
        }
    }
    throw new Exception("tests/fixtures/sql_compile/cases.json not found");
}

static string Norm(string sql) => string.Join(' ', sql.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

static void Expect(bool condition, string detail)
{
    if (!condition)
        throw new Exception(detail);
}
