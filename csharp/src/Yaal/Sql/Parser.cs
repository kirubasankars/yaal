// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.RegularExpressions;

namespace Yaal.Sql;

public static class SqlParser
{
    private static readonly HashSet<string> KnownParamTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "integer", "string", "float", "bool", "blob",
    };

    private static readonly Regex ParameterRx = new(
        @"\s*(?<name>[$_.A-Za-z0-9\[\]]+)\s+(?<type>\w+)\s*",
        RegexOptions.Compiled);

    private static readonly Regex SqlRx = new(
        @"--sql\(\s*(?<name>\w+)?\s*\)--",
        RegexOptions.Compiled);

    private static readonly Regex PossibleNullParameterRx = new(
        @"^\(\s*{{(?<name>[A-Za-z0-9_.$-]*?)}}\s+is\s+null\s+or",
        RegexOptions.IgnoreCase | RegexOptions.Compiled);

    private static int SkipWs(List<SqlToken> tokens, int i)
    {
        while (i < tokens.Count && tokens[i].Type is "space" or "newline")
            i += 1;
        return i;
    }

    /// <summary>
    /// If tokens after paramIndex match <c>is null or</c> (any ws/case), return index of <c>null</c>.
    /// </summary>
    private static int? MatchIsNullOrAfter(List<SqlToken> tokens, int paramIndex)
    {
        var i = SkipWs(tokens, paramIndex + 1);
        if (i >= tokens.Count || tokens[i].Type != "word" ||
            !tokens[i].Value.Equals("is", StringComparison.OrdinalIgnoreCase))
            return null;
        i = SkipWs(tokens, i + 1);
        if (i >= tokens.Count || tokens[i].Type != "word" ||
            !tokens[i].Value.Equals("null", StringComparison.OrdinalIgnoreCase))
            return null;
        var j = SkipWs(tokens, i + 1);
        if (j >= tokens.Count || tokens[j].Type != "word" ||
            !tokens[j].Value.Equals("or", StringComparison.OrdinalIgnoreCase))
            return null;
        return i;
    }

    private static List<ParamDecl> ParseParameterHeader(string tokenValue, string method)
    {
        var inner = tokenValue[3..^3];
        if (string.IsNullOrWhiteSpace(inner))
            throw new InvalidOperationException("empty parameter header in " + method + ".sql");

        var paramsList = new List<ParamDecl>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var segment in inner.Split(','))
        {
            if (string.IsNullOrWhiteSpace(segment))
                throw new InvalidOperationException("invalid parameter declaration in " + method + ".sql");

            var m = ParameterRx.Match(segment);
            if (!m.Success || m.Index != 0 || m.Length != segment.Length)
            {
                throw new InvalidOperationException(
                    "invalid parameter declaration '" + segment.Trim() + "' in " + method + ".sql");
            }

            var paramName = m.Groups["name"].Value.Trim().ToLowerInvariant();
            var paramType = m.Groups["type"].Value.ToLowerInvariant();
            if (!KnownParamTypes.Contains(paramType))
            {
                throw new InvalidOperationException(
                    "unknown parameter type '" + paramType + "' for {{" + paramName + "}} in " +
                    method + ".sql (expected bool, blob, float, integer, string)");
            }
            if (!seen.Add(paramName))
            {
                throw new InvalidOperationException(
                    "duplicate parameter {{" + paramName + "}} in " + method + ".sql");
            }
            paramsList.Add(new ParamDecl { Name = paramName, Type = paramType });
        }
        return paramsList;
    }

    private static bool TryParseParameterHeader(string tokenValue, string method, out List<ParamDecl>? parameters)
    {
        parameters = null;
        if (!tokenValue.StartsWith("--(", StringComparison.Ordinal) ||
            !tokenValue.EndsWith(")--", StringComparison.Ordinal))
            return false;
        parameters = ParseParameterHeader(tokenValue, method);
        return true;
    }

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
        var significantSeen = false;
        while (tc < tokens.Count)
        {
            var token = tokens[tc];
            var tokenValue = token.Value;
            var tokenType = token.Type;

            if ((tokenType is "space" or "newline") && !significantSeen)
            {
                tc += 1;
                continue;
            }

            if (tokenType == "parameter")
            {
                var parameterName = tokenValue[2..^2].Trim().ToLowerInvariant();
                token.Name = parameterName;
                sqlStmt.Parameters.Add(new ParamDecl { Name = parameterName });

                var nullIdx = MatchIsNullOrAfter(tokens, tc);
                if (nullIdx != null)
                {
                    if (braceGroups.Count == 0)
                    {
                        throw new InvalidOperationException(
                            "{{" + parameterName + "}} is null or must be wrapped in parentheses in " +
                            method + ".sql");
                    }
                    token = new SqlToken
                    {
                        Type = "parameter",
                        Name = parameterName,
                        Value = "{{" + parameterName + "}} is null",
                        Nullable = true,
                    };
                    tc = nullIdx.Value;
                }
            }

            if (tokenType == "dash")
            {
                if (!significantSeen && TryParseParameterHeader(tokenValue, method, out var headerParams))
                {
                    ast.Parameters = headerParams!.ToDictionary(x => x.Name, x => x);
                    significantSeen = true;
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
                significantSeen = true;
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
            significantSeen = true;
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
