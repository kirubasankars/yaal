// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Linq;
using System.Text.RegularExpressions;

namespace Yaal.Sql;

public static class SortDirDesugar
{
    private static readonly Regex SortKeyRx = new(@"^\w+$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static readonly HashSet<string> OrderByClauseEnd = new(StringComparer.OrdinalIgnoreCase)
    {
        "limit", "offset", "fetch", "for", "union", "except", "intersect", ")", ";",
    };

    // Allowlisted client-facing dir() values -> spliced SQL suffix. NULLS placement is
    // client-controlled here (not per-author-key) since these are fixed keywords, never
    // identifiers -- no injection risk. Note: MySQL has no NULLS FIRST/LAST syntax; a
    // *_nulls_first/*_nulls_last value will raise a real SQL error on that engine.
    private static readonly Dictionary<string, string> DirVocab = new(StringComparer.OrdinalIgnoreCase)
    {
        ["asc"] = "ASC",
        ["desc"] = "DESC",
        ["asc_nulls_first"] = "ASC NULLS FIRST",
        ["asc_nulls_last"] = "ASC NULLS LAST",
        ["desc_nulls_first"] = "DESC NULLS FIRST",
        ["desc_nulls_last"] = "DESC NULLS LAST",
    };

    private static int SkipWs(List<SqlToken> tokens, int i)
    {
        while (i < tokens.Count && tokens[i].Type is "space" or "newline")
            i += 1;
        return i;
    }

    internal static string TokensToSql(IEnumerable<SqlToken> tokens) =>
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

    private static bool IsOrderByClauseEndWord(SqlToken token) =>
        token.Type == "word" && OrderByClauseEnd.Contains(token.Value.Trim());

    /// <summary>
    /// Split an ORDER BY body into comma-separated terms. Depth-aware: commas inside
    /// parens (e.g. COALESCE(a, b)) do not split a term. A comma glued onto the end of
    /// a word token (e.g. "u.user_id,") is trimmed off the term.
    ///
    /// TrailingWs is the whitespace text trimmed off the end of the last term (i.e.
    /// the gap between the clause body and ClauseEndIdx) -- callers that splice
    /// replacement text must re-append it so a following clause-end word/paren
    /// (e.g. "LIMIT") doesn't get glued onto the replacement.
    /// </summary>
    internal static (List<List<SqlToken>> Terms, int ClauseEndIdx, string TrailingWs) SplitOrderByTerms(
        List<SqlToken> content, int startIdx)
    {
        var n = content.Count;
        var terms = new List<List<SqlToken>>();
        var current = new List<SqlToken>();
        var depth = 0;
        var i = startIdx;
        while (i < n)
        {
            var t = content[i];
            if (depth == 0 && ((t.Type == "brace" && t.Value == ")") || IsOrderByClauseEndWord(t)))
                break;

            if (t.Type == "brace")
            {
                if (t.Value == "(")
                    depth += 1;
                else if (t.Value == ")")
                    depth -= 1;
                current.Add(t);
                i += 1;
                continue;
            }

            if (depth == 0 && t.Type == "word")
            {
                var val = t.Value;
                if (val == ",")
                {
                    terms.Add(current);
                    current = new List<SqlToken>();
                    i += 1;
                    continue;
                }
                if (val.EndsWith(",", StringComparison.Ordinal) && !val[..^1].Contains(','))
                {
                    var trimmed = new SqlToken { Type = t.Type, Value = val[..^1], Group = t.Group };
                    if (trimmed.Value != "")
                        current.Add(trimmed);
                    terms.Add(current);
                    current = new List<SqlToken>();
                    i += 1;
                    continue;
                }
            }

            current.Add(t);
            i += 1;
        }
        terms.Add(current);

        var tailEnd = current.Count;
        while (tailEnd > 0 && current[tailEnd - 1].Type is "space" or "newline")
            tailEnd -= 1;
        var trailingWs = string.Concat(current.GetRange(tailEnd, current.Count - tailEnd).Select(t => t.Value ?? ""));

        static List<SqlToken> Trim(List<SqlToken> term)
        {
            var start = 0;
            var end = term.Count;
            while (start < end && term[start].Type is "space" or "newline")
                start += 1;
            while (end > start && term[end - 1].Type is "space" or "newline")
                end -= 1;
            return term.GetRange(start, end - start);
        }

        return (terms.Select(Trim).ToList(), i, trailingWs);
    }

    /// <summary>
    /// v2: at most one dynamic sort()/dir() term per ORDER BY; other comma-separated
    /// terms (a static tiebreaker before or after it) are ordinary author SQL.
    ///
    /// Multiple sort()/dir() pairs are allowed in one statement (e.g. a subquery's
    /// ORDER BY plus the outer query's ORDER BY), but each must use a distinct
    /// {{param}} -- resolution is keyed by param name for the whole statement, so
    /// reusing one param across two sort() calls (each with its own choices) would
    /// silently resolve to only one of them.
    /// </summary>
    public static void ValidateDynamicOrderBy(List<SqlToken> content, string method)
    {
        var seenSortParams = new HashSet<string>(StringComparer.Ordinal);
        foreach (var tok in content)
        {
            if (tok.Type != "sort")
                continue;
            var param = tok.Param!;
            if (!seenSortParams.Add(param))
            {
                throw new InvalidOperationException(
                    "{{" + param + "}} is used in more than one sort(...) in " + method +
                    ".sql -- each sort() must use a distinct param");
            }
        }

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
                    var (terms, endIdx, _) = SplitOrderByTerms(content, k);

                    foreach (var term in terms)
                    {
                        if (term.Count == 0)
                            throw new InvalidOperationException("empty ORDER BY term in " + method + ".sql");
                    }

                    var dynamicTerms = terms.Where(term => term.Any(tok => tok.Type is "sort" or "dir")).ToList();
                    if (dynamicTerms.Count > 1)
                    {
                        throw new InvalidOperationException(
                            "only one dynamic sort()/dir() term is allowed per ORDER BY in " +
                            method + ".sql");
                    }
                    if (dynamicTerms.Count == 1)
                    {
                        var dyn = dynamicTerms[0];
                        var sortPositions = Enumerable.Range(0, dyn.Count).Where(p => dyn[p].Type == "sort").ToList();
                        var dirPositions = Enumerable.Range(0, dyn.Count).Where(p => dyn[p].Type == "dir").ToList();
                        if (sortPositions.Count != 1)
                        {
                            throw new InvalidOperationException(
                                "ORDER BY dynamic term must contain exactly one sort(...) in " +
                                method + ".sql");
                        }
                        if (dirPositions.Count > 1)
                        {
                            throw new InvalidOperationException(
                                "ORDER BY dynamic term must contain at most one dir(...) in " +
                                method + ".sql");
                        }
                        var nonWs = Enumerable.Range(0, dyn.Count)
                            .Where(p => dyn[p].Type is not ("space" or "newline")).ToList();
                        if (dirPositions.Count == 1)
                        {
                            var sPos = nonWs.IndexOf(sortPositions[0]);
                            var dPos = nonWs.IndexOf(dirPositions[0]);
                            if (dPos != sPos + 1 || nonWs.Count != 2)
                            {
                                throw new InvalidOperationException(
                                    "ORDER BY with sort()/dir() must not include other terms in " +
                                    method + ".sql");
                            }
                        }
                        else if (nonWs.Count != 1)
                        {
                            throw new InvalidOperationException(
                                "ORDER BY with sort()/dir() must not include other terms in " +
                                method + ".sql");
                        }
                    }
                    i = endIdx;
                    continue;
                }
            }
            i += 1;
        }
    }

    /// <summary>
    /// If a dir(...) token immediately follows content[sortIndex] (ws only between),
    /// return its param name; else null.
    /// </summary>
    private static string? FindPairedDirParam(List<SqlToken> content, int sortIndex)
    {
        var i = SkipWs(content, sortIndex + 1);
        return i < content.Count && content[i].Type == "dir" ? content[i].Param : null;
    }

    private static string AsRawString(object raw) => raw as string ?? raw.ToString() ?? "";

    /// <summary>
    /// Resolve sort()/dir() runtime values from inputShape.
    ///
    /// The sort param may be a single key or a comma-separated list of keys
    /// (multi-column dynamic sort). The paired dir param is matched to those keys by
    /// position (comma-separated); missing trailing entries default to "asc". Allowed
    /// dir values: asc, desc, asc_nulls_first, asc_nulls_last, desc_nulls_first,
    /// desc_nulls_last (case-insensitive).
    ///
    /// Returns sortMap where sortMap[param] is either null (elide this dynamic ORDER BY
    /// term) or a ready-to-splice "expr1 DIR1, expr2 DIR2, ..." string (direction
    /// already resolved; no separate dir map).
    /// </summary>
    public static Dictionary<string, string?> ResolveValues(Twig sqlStmt, Shape? inputShape)
    {
        var sortMap = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var content = sqlStmt.Content;

        for (var idx = 0; idx < content.Count; idx++)
        {
            var token = content[idx];
            if (token.Type != "sort")
                continue;

            var param = token.Param!;
            var rawSort = inputShape?.GetProp(param);
            if (rawSort == null)
            {
                sortMap[param] = null;
                continue;
            }

            var choices = token.Choices ?? new Dictionary<string, string>();
            var keys = AsRawString(rawSort).Split(',').Select(k => k.Trim().ToLowerInvariant()).ToList();
            var seen = new HashSet<string>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (key == "")
                    throw new SortDirException("unknown sort key: '" + AsRawString(rawSort) + "'");
                if (!choices.ContainsKey(key))
                    throw new SortDirException("unknown sort key: " + key);
                if (!seen.Add(key))
                    throw new SortDirException("duplicate sort key: " + key);
            }

            var dirParam = FindPairedDirParam(content, idx);
            var rawDir = dirParam != null ? inputShape?.GetProp(dirParam) : null;
            var dirs = rawDir != null
                ? AsRawString(rawDir).Split(',').Select(d => d.Trim().ToLowerInvariant()).ToList()
                : new List<string>();
            if (dirs.Count > keys.Count)
            {
                throw new SortDirException(
                    "too many dir values for " + keys.Count + " sort key(s): " + AsRawString(rawDir!));
            }
            while (dirs.Count < keys.Count)
                dirs.Add("asc");

            var resolvedDirs = new List<string>();
            foreach (var d in dirs)
            {
                if (!DirVocab.TryGetValue(d, out var sqlDir))
                    throw new SortDirException("unknown sort direction: " + d);
                resolvedDirs.Add(sqlDir);
            }

            sortMap[param] = string.Join(", ", keys.Zip(resolvedDirs, (k, d) => choices[k] + " " + d));
        }

        return sortMap;
    }
}
