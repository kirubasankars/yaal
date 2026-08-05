// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.RegularExpressions;

namespace Yaal.Sql;

public static class SqlParser
{
    private static readonly Regex ParameterRx = new(
        @"\s*(?<name>[$_.A-Za-z0-9\[\]]+)(\s+(?<type>\w+))?\s*",
        RegexOptions.Compiled);

    private static readonly Regex SqlRx = new(
        @"--sql\(\s*(?<name>\w+)?\s*\)--",
        RegexOptions.Compiled);

    private static readonly Regex PossibleNullParameterRx = new(
        @"^\(\s*{{(?<name>[A-Za-z0-9_.$-]*?)}}\s+is\s+null\s+or",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    public static SqlAst? Parse(List<SqlToken>? tokens, string method)
    {
        if (tokens == null || tokens.Count == 0)
            return null;

        tokens = OptionalDesugar.Desugar(tokens);

        var ast = new SqlAst();
        var sqlStmts = new List<Twig>();
        var braceGroups = new List<SqlToken>();

        var sqlStmt = new Twig();

        var tc = 0;
        while (tc < tokens.Count)
        {
            var token = tokens[tc];
            var tokenValue = token.Value;
            var tokenType = token.Type;

            if (tokenType == "parameter")
            {
                var parameterName = tokenValue[2..^2].Trim().ToLowerInvariant();
                token.Name = parameterName;
                sqlStmt.Parameters.Add(new ParamDecl { Name = parameterName });

                if (tc + 4 < tokens.Count)
                {
                    var token2 = tokens[tc + 1];
                    var token3 = tokens[tc + 2];
                    var token4 = tokens[tc + 3];
                    var token5 = tokens[tc + 4];

                    if (token2.Type == "space" &&
                        token3.Value == "is" &&
                        token4.Type == "space" &&
                        token5.Value == "null")
                    {
                        token = new SqlToken
                        {
                            Type = "parameter",
                            Name = parameterName,
                            Value = "{{" + parameterName + "}} is null",
                            Nullable = true,
                        };
                        tc += 4;
                    }
                }
            }

            if (tokenType == "dash")
            {
                if (tokenValue.StartsWith("--(", StringComparison.Ordinal) &&
                    tokenValue.EndsWith(")--", StringComparison.Ordinal) &&
                    tc == 0)
                {
                    tokenValue = tokenValue[3..^3];
                    var paramsList = tokenValue.Split(',');
                    token.Parameters = new List<ParamDecl>();
                    foreach (var p in paramsList)
                    {
                        var m = ParameterRx.Match(p);
                        if (m.Success)
                        {
                            var paramName = m.Groups["name"].Value.Trim().ToLowerInvariant();
                            var paramType = m.Groups["type"].Success ? m.Groups["type"].Value : "";
                            token.Parameters.Add(new ParamDecl { Name = paramName, Type = paramType });
                        }
                    }
                    ast.Parameters = token.Parameters.ToDictionary(x => x.Name, x => x);
                    tc += 1;
                    continue;
                }

                token.Type = "sql";
                if (sqlStmt.Content.Any(x => x.Type == "word"))
                    sqlStmts.Add(sqlStmt);

                sqlStmt = new Twig();
                var sqlMatch = SqlRx.Match(tokenValue);
                if (sqlMatch.Success)
                    sqlStmt.Connection = sqlMatch.Groups["name"].Success &&
                                         !string.IsNullOrEmpty(sqlMatch.Groups["name"].Value)
                        ? sqlMatch.Groups["name"].Value
                        : "db";
                tc += 1;
                continue;
            }

            if (tokenType == "brace")
            {
                var exists = braceGroups.FirstOrDefault(x => x.Group == token.Group);
                if (exists == null)
                {
                    braceGroups.Add(token);
                    token.Content = new List<string>();
                }
                else
                {
                    var contentList = (List<string>)exists.Content!;
                    contentList.Add(token.Value);
                    exists.Content = string.Join("", contentList);
                    braceGroups.Remove(exists);
                }
            }

            if (braceGroups.Count > 0)
            {
                foreach (var g in braceGroups)
                {
                    if (g.Content is List<string> list)
                        list.Add(token.Value);
                }
            }

            sqlStmt.Content.Add(token);
            tc += 1;
        }

        if (sqlStmt.Content.Any(x => x.Type == "word"))
            sqlStmts.Add(sqlStmt);

        var astParameters = ast.Parameters;
        foreach (var stmt in sqlStmts)
        {
            var parameters = new List<ParamDecl>();
            foreach (var p in stmt.Parameters)
            {
                if (astParameters != null && astParameters.TryGetValue(p.Name, out var declared))
                    parameters.Add(declared);
                else
                    throw new InvalidOperationException(
                        "type missing for {{" + p.Name + "}} in the " + method + ".sql");
            }
            stmt.Parameters = parameters;
        }

        foreach (var stmt in sqlStmts)
        {
            stmt.Nullable = new List<string>();
            foreach (var token in stmt.Content)
            {
                if (token.Type == "brace" && token.Content is string contentStr)
                {
                    var m = PossibleNullParameterRx.Match(contentStr);
                    if (m.Success)
                    {
                        var name = m.Groups["name"].Value.ToLowerInvariant();
                        stmt.Nullable.Add(name);
                        token.NullableParameter = name;
                    }
                    token.Content = null;
                }
            }

            if (stmt.Nullable.Count == 0)
                stmt.Nullable = null;
        }

        if (sqlStmts.Count > 0)
            ast.SqlStmts = sqlStmts;

        return ast;
    }
}
