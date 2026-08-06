// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.RegularExpressions;

namespace Yaal.Sql;

public static class SortDirDesugar
{
    private static readonly Regex SortKeyRx = new(@"^\w+$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly HashSet<string> OrderByClauseEnd = new(StringComparer.OrdinalIgnoreCase)
    {
        "limit", "offset", "fetch", "for", "union", "except", "intersect", ")", ";",
    };

    private static int SkipWs(List<SqlToken> tokens, int i)
    {
        while (i < tokens.Count && tokens[i].Type is "space" or "newline")
            i += 1;
        return i;
    }

    private static string TokensToSql(IEnumerable<SqlToken> tokens) =>
        string.Concat(tokens.Select(t => t.Value ?? ""));

    private static bool BodyContainsSortOrDirCall(List<SqlToken> tokens)
    {
        for (var i = 0; i < tokens.Count; i++)
        {
            var t = tokens[i];
            if (t.Type == "word" &&
                (t.Value.Equals("sort", StringComparison.OrdinalIgnoreCase) ||
                 t.Value.Equals("dir", StringComparison.OrdinalIgnoreCase)))
            {
                var j = SkipWs(tokens, i + 1);
                if (j < tokens.Count && tokens[j].Type == "brace" && tokens[j].Value == "(")
                    return true;
            }
        }
        return false;
    }

    private static (string Param, Dictionary<string, string> Choices) ParseSortBody(List<SqlToken> body)
    {
        if (BodyContainsSortOrDirCall(body))
            throw new InvalidOperationException("nested sort(...)/dir(...) is not allowed");

        var i = SkipWs(body, 0);
        if (i >= body.Count || body[i].Type != "parameter")
            throw new InvalidOperationException("sort(...) requires {{param}} as the first argument");

        var paramName = OptionalDesugar.ParameterNameFromToken(body[i]);
        var paramCount = body.Count(t => t.Type == "parameter");
        if (paramCount != 1)
            throw new InvalidOperationException("sort(...) requires exactly one {{param}}");

        i = SkipWs(body, i + 1);
        if (i >= body.Count)
            throw new InvalidOperationException("sort(...) requires at least one key = expr pair");

        if (body[i].Type == "word" && body[i].Value == ",")
            i = SkipWs(body, i + 1);
        else if (body[i].Type == "word" && body[i].Value.StartsWith(",", StringComparison.Ordinal))
            throw new InvalidOperationException("invalid sort(...) argument list");

        if (i >= body.Count)
            throw new InvalidOperationException("sort(...) requires at least one key = expr pair");

        var choices = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        while (i < body.Count)
        {
            if (body[i].Type != "word" || !SortKeyRx.IsMatch(body[i].Value))
                throw new InvalidOperationException(
                    "sort(...) keys must be word characters (\\w+), got " + (body[i].Value ?? ""));

            var key = body[i].Value.ToLowerInvariant();
            if (choices.ContainsKey(key))
                throw new InvalidOperationException("duplicate sort(...) key: " + key);

            i = SkipWs(body, i + 1);
            if (i >= body.Count || body[i].Type != "word" || body[i].Value != "=")
                throw new InvalidOperationException("sort(...) expected key = expr");

            i = SkipWs(body, i + 1);
            if (i >= body.Count)
                throw new InvalidOperationException("sort(...) expected expression after =");

            var exprTokens = new List<SqlToken>();
            var depth = 0;
            while (i < body.Count)
            {
                var t = body[i];
                if (t.Type == "brace")
                {
                    if (t.Value == "(")
                        depth += 1;
                    else if (t.Value == ")")
                        depth -= 1;
                    exprTokens.Add(t);
                    i += 1;
                    continue;
                }

                if (depth == 0 && t.Type == "word")
                {
                    var val = t.Value;
                    if (val == ",")
                    {
                        i += 1;
                        break;
                    }
                    if (val.EndsWith(',') && !val[..^1].Contains(','))
                    {
                        var trimmed = new SqlToken { Type = t.Type, Value = val[..^1] };
                        if (trimmed.Value != "")
                            exprTokens.Add(trimmed);
                        i += 1;
                        break;
                    }
                }

                exprTokens.Add(t);
                i += 1;
            }

            while (exprTokens.Count > 0 && exprTokens[^1].Type is "space" or "newline")
                exprTokens.RemoveAt(exprTokens.Count - 1);
            while (exprTokens.Count > 0 && exprTokens[0].Type is "space" or "newline")
                exprTokens.RemoveAt(0);

            var exprSql = TokensToSql(exprTokens).Trim();
            if (exprSql == "")
                throw new InvalidOperationException("sort(...) empty expression for key " + key);
            choices[key] = exprSql;
            i = SkipWs(body, i);
        }

        if (choices.Count == 0)
            throw new InvalidOperationException("sort(...) requires at least one key = expr pair");
        return (paramName, choices);
    }

    private static string ParseDirBody(List<SqlToken> body)
    {
        if (BodyContainsSortOrDirCall(body))
            throw new InvalidOperationException("nested sort(...)/dir(...) is not allowed");

        var i = SkipWs(body, 0);
        if (i >= body.Count || body[i].Type != "parameter")
            throw new InvalidOperationException("dir(...) requires exactly one {{param}}");

        var paramName = OptionalDesugar.ParameterNameFromToken(body[i]);
        var paramCount = body.Count(t => t.Type == "parameter");
        if (paramCount != 1)
            throw new InvalidOperationException("dir(...) requires exactly one {{param}}");

        i = SkipWs(body, i + 1);
        if (i < body.Count)
            throw new InvalidOperationException("dir(...) does not accept key = expr pairs");
        return paramName;
    }

    public static List<SqlToken> Desugar(List<SqlToken>? tokens)
    {
        if (tokens == null)
            return new List<SqlToken>();

        var result = new List<SqlToken>();
        var i = 0;
        var n = tokens.Count;
        while (i < n)
        {
            var tok = tokens[i];
            if (tok.Type == "word" &&
                (tok.Value.Equals("sort", StringComparison.OrdinalIgnoreCase) ||
                 tok.Value.Equals("dir", StringComparison.OrdinalIgnoreCase)))
            {
                var kind = tok.Value.ToLowerInvariant();
                var j = SkipWs(tokens, i + 1);
                if (j < n && tokens[j].Type == "brace" && tokens[j].Value == "(")
                {
                    var group = tokens[j].Group;
                    var k = j + 1;
                    while (k < n)
                    {
                        var t = tokens[k];
                        if (t.Type == "brace" && t.Value == ")" && t.Group == group)
                            break;
                        k += 1;
                    }
                    if (k >= n)
                        throw new InvalidOperationException("unclosed " + kind + "(...)");

                    var body = tokens.GetRange(j + 1, k - (j + 1));
                    if (body.Count == 0 || body.All(t => t.Type is "space" or "newline"))
                        throw new InvalidOperationException(kind + "() empty / no param");

                    if (kind == "sort")
                    {
                        var (paramName, choices) = ParseSortBody(body);
                        result.Add(new SqlToken
                        {
                            Type = "sort",
                            Value = "",
                            Param = paramName,
                            Choices = choices,
                        });
                    }
                    else
                    {
                        var paramName = ParseDirBody(body);
                        result.Add(new SqlToken
                        {
                            Type = "dir",
                            Value = "",
                            Param = paramName,
                        });
                    }
                    i = k + 1;
                    continue;
                }
            }

            result.Add(tok);
            i += 1;
        }

        return result;
    }

    private static bool IsClauseEndToken(SqlToken token)
    {
        if (token.Type == "brace" && token.Value == ")")
            return true;
        if (token.Type == "word")
            return OrderByClauseEnd.Contains(token.Value.Trim());
        return false;
    }

    public static void ValidateDynamicOrderBy(List<SqlToken> content, string method)
    {
        var n = content.Count;
        var i = 0;
        while (i < n)
        {
            var t = content[i];
            if (t.Type == "word" && t.Value.Equals("order", StringComparison.OrdinalIgnoreCase))
            {
                var j = SkipWs(content, i + 1);
                if (j < n && content[j].Type == "word" &&
                    content[j].Value.Equals("by", StringComparison.OrdinalIgnoreCase))
                {
                    var k = SkipWs(content, j + 1);
                    var hasDynamic = false;
                    var hasStatic = false;
                    while (k < n && !IsClauseEndToken(content[k]))
                    {
                        var ct = content[k];
                        if (ct.Type is "space" or "newline")
                        {
                            k += 1;
                            continue;
                        }
                        if (ct.Type is "sort" or "dir")
                        {
                            hasDynamic = true;
                            k += 1;
                            continue;
                        }
                        hasStatic = true;
                        k += 1;
                    }
                    if (hasDynamic && hasStatic)
                    {
                        throw new InvalidOperationException(
                            "ORDER BY with sort()/dir() must not include other terms in " +
                            method + ".sql");
                    }
                    i = k;
                    continue;
                }
            }
            i += 1;
        }
    }

    public static (Dictionary<string, string?> SortMap, Dictionary<string, string> DirMap)
        ResolveValues(Twig sqlStmt, Shape? inputShape)
    {
        var sortMap = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var dirMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        foreach (var token in sqlStmt.Content)
        {
            if (token.Type == "sort")
            {
                var param = token.Param!;
                var raw = inputShape?.GetProp(param);
                if (raw == null)
                {
                    sortMap[param] = null;
                    continue;
                }
                var key = (raw as string ?? raw.ToString() ?? "").Trim().ToLowerInvariant();
                if (key == "")
                    throw new SortDirException("unknown sort key: " + FormatRaw(raw));
                if (token.Choices == null || !token.Choices.TryGetValue(key, out var expr))
                    throw new SortDirException("unknown sort key: " + (raw as string ?? raw.ToString()));
                sortMap[param] = expr;
            }
            else if (token.Type == "dir")
            {
                var param = token.Param!;
                var raw = inputShape?.GetProp(param);
                if (raw == null)
                {
                    dirMap[param] = "ASC";
                    continue;
                }
                var direction = (raw as string ?? raw.ToString() ?? "").Trim().ToLowerInvariant();
                if (direction == "asc")
                    dirMap[param] = "ASC";
                else if (direction == "desc")
                    dirMap[param] = "DESC";
                else
                    throw new SortDirException("unknown sort direction: " + (raw as string ?? raw.ToString()));
            }
        }

        return (sortMap, dirMap);
    }

    private static string FormatRaw(object raw) =>
        raw is string s ? "'" + s + "'" : (raw.ToString() ?? "");
}
