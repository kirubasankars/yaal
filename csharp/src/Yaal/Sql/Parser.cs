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
        @"\s*(?<name>[$_.A-Za-z0-9\[\]]+)(?<required>!)?\s+(?<type>\w+)(?:\s*=\s*(?<default>.+))?\s*",
        RegexOptions.Compiled);

    private static readonly Regex IntegerDefaultRx = new(
        @"^-?\d+$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex FloatDefaultRx = new(
        @"^-?\d+(\.\d+)?$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly Regex BareStringDefaultRx = new(
        @"^\w+$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

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

    private static List<string> SplitParameterHeaderSegments(string inner)
    {
        var segments = new List<string>();
        var buf = new System.Text.StringBuilder();
        var inQuote = false;
        for (var i = 0; i < inner.Length; i++)
        {
            var ch = inner[i];
            if (inQuote)
            {
                buf.Append(ch);
                if (ch == '\\' && i + 1 < inner.Length)
                {
                    buf.Append(inner[i + 1]);
                    i += 1;
                    continue;
                }
                if (ch == '\'')
                    inQuote = false;
                continue;
            }
            if (ch == '\'')
            {
                inQuote = true;
                buf.Append(ch);
                continue;
            }
            if (ch == ',')
            {
                segments.Add(buf.ToString());
                buf.Clear();
                continue;
            }
            buf.Append(ch);
        }
        if (inQuote)
            throw new InvalidOperationException("unclosed string literal in parameter header default");
        segments.Add(buf.ToString());
        return segments;
    }

    private static object ParseHeaderDefaultLiteral(string raw, string paramType, string paramName, string method)
    {
        var text = raw.Trim();
        if (paramType == "blob")
        {
            throw new InvalidOperationException(
                "default values are not supported for blob parameter {{" + paramName + "}} in " +
                method + ".sql");
        }
        if (paramType == "integer")
        {
            if (!IntegerDefaultRx.IsMatch(text))
            {
                throw new InvalidOperationException(
                    "invalid integer default '" + text + "' for {{" + paramName + "}} in " +
                    method + ".sql");
            }
            return long.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
        }
        if (paramType == "float")
        {
            if (!FloatDefaultRx.IsMatch(text))
            {
                throw new InvalidOperationException(
                    "invalid float default '" + text + "' for {{" + paramName + "}} in " +
                    method + ".sql");
            }
            return double.Parse(text, System.Globalization.CultureInfo.InvariantCulture);
        }
        if (paramType == "bool")
        {
            if (text.Equals("true", StringComparison.OrdinalIgnoreCase))
                return true;
            if (text.Equals("false", StringComparison.OrdinalIgnoreCase))
                return false;
            throw new InvalidOperationException(
                "invalid bool default '" + text + "' for {{" + paramName + "}} in " +
                method + ".sql");
        }

        if (text.StartsWith('\''))
        {
            if (text.Length < 2 || !text.EndsWith('\''))
            {
                throw new InvalidOperationException(
                    "unclosed string literal in default for {{" + paramName + "}} in " +
                    method + ".sql");
            }
            var outBuf = new System.Text.StringBuilder();
            for (var j = 1; j < text.Length - 1; j++)
            {
                if (text[j] == '\\' && j + 1 < text.Length - 1)
                {
                    outBuf.Append(text[j + 1]);
                    j += 1;
                    continue;
                }
                if (text[j] == '\'')
                {
                    throw new InvalidOperationException(
                        "invalid string default for {{" + paramName + "}} in " + method + ".sql");
                }
                outBuf.Append(text[j]);
            }
            return outBuf.ToString();
        }
        if (BareStringDefaultRx.IsMatch(text))
            return text;
        throw new InvalidOperationException(
            "invalid string default '" + text + "' for {{" + paramName + "}} in " + method + ".sql");
    }

    private static List<ParamDecl> ParseParameterHeader(string tokenValue, string method)
    {
        var inner = tokenValue[3..^3];
        if (string.IsNullOrWhiteSpace(inner))
            throw new InvalidOperationException("empty parameter header in " + method + ".sql");

        var paramsList = new List<ParamDecl>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var segment in SplitParameterHeaderSegments(inner))
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
            var paramRequired = m.Groups["required"].Success;
            var hasDefault = m.Groups["default"].Success;
            if (!KnownParamTypes.Contains(paramType))
            {
                throw new InvalidOperationException(
                    "unknown parameter type '" + paramType + "' for {{" + paramName + "}} in " +
                    method + ".sql (expected bool, blob, float, integer, string)");
            }
            if (paramRequired && hasDefault)
            {
                throw new InvalidOperationException(
                    "required parameter {{" + paramName + "}} cannot have a default in " +
                    method + ".sql");
            }
            if (!seen.Add(paramName))
            {
                throw new InvalidOperationException(
                    "duplicate parameter {{" + paramName + "}} in " + method + ".sql");
            }
            var decl = new ParamDecl
            {
                Name = paramName,
                Type = paramType,
                Required = paramRequired,
            };
            if (hasDefault)
            {
                decl.Default = ParseHeaderDefaultLiteral(
                    m.Groups["default"].Value, paramType, paramName, method);
                decl.HasDefault = true;
            }
            paramsList.Add(decl);
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
        tokens = SortDirDesugar.Desugar(tokens);

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

            if (tokenType is "sort" or "dir")
                sqlStmt.Parameters.Add(new ParamDecl { Name = token.Param! });

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

            SortDirDesugar.ValidateDynamicOrderBy(stmt.Content, method);
        }

        if (sqlStmts.Count > 0)
            ast.SqlStmts = sqlStmts;

        return ast;
    }
}
