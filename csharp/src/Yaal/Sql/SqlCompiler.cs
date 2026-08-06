// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.RegularExpressions;

namespace Yaal.Sql;

public static class SqlCompiler
{
    private static readonly HashSet<string> ClauseBoundary = new(StringComparer.OrdinalIgnoreCase)
    {
        "order", "group", "having", "limit", "offset", "fetch", "for",
        "union", "except", "intersect", ")", "where", "prewhere",
    };

    private static readonly HashSet<string> FilterClauses = new(StringComparer.OrdinalIgnoreCase)
    {
        "where", "prewhere",
    };

    private static readonly Regex OneEqualsOneCompact = new(
        @"^1\s*=\s*1$", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// Compile the ORDER BY clause starting at stmt[orderIdx] ("order"). Renders each
    /// comma-separated term: the dynamic term (containing sort()/optional dir()) is
    /// replaced by its resolved combined "expr DIR, ..." string (dropped entirely if
    /// that resolves to null); static terms are reproduced verbatim. If nothing
    /// remains, the whole clause elides.
    ///
    /// fragments (when not eliding) reuses the original "order"/"by"/whitespace
    /// tokens verbatim instead of merging them into one string, so downstream
    /// WHERE/PREWHERE cleanup -- which detects clause boundaries by exact-matching
    /// the word "order" -- still recognizes it.
    /// </summary>
    private static (bool IsOrderBy, List<string>? Fragments, int NextIdx) CompileOrderBy(
        List<SqlToken> stmt, int orderIdx, Dictionary<string, string?> sortMap)
    {
        var n = stmt.Count;
        var j = SkipWsTokens(stmt, orderIdx + 1);
        if (j >= n || stmt[j].Type != "word" || !stmt[j].Value.Equals("by", StringComparison.OrdinalIgnoreCase))
            return (false, null, orderIdx);

        var k = SkipWsTokens(stmt, j + 1);
        var (terms, endIdx, trailingWs) = SortDirDesugar.SplitOrderByTerms(stmt, k);

        var rendered = new List<string>();
        foreach (var term in terms)
        {
            var sortToken = term.FirstOrDefault(tok => tok.Type == "sort");
            if (sortToken == null)
            {
                rendered.Add(SortDirDesugar.TokensToSql(term).Trim());
                continue;
            }
            if (sortMap.TryGetValue(sortToken.Param!, out var expr) && expr != null)
                rendered.Add(expr);
            // else: this dynamic term elides; drop it (and its comma) entirely.
        }

        if (rendered.Count == 0)
            return (true, null, endIdx);

        var fragments = stmt.GetRange(orderIdx, k - orderIdx).Select(t => t.Value ?? "").ToList();
        // Re-append the whitespace trimmed off the end of the clause body so a
        // following clause-end word/paren (e.g. "LIMIT") isn't glued onto it.
        fragments.Add(string.Join(", ", rendered));
        fragments.Add(trailingWs);
        return (true, fragments, endIdx);
    }

    private static int SkipWsTokens(List<SqlToken> stmt, int i)
    {
        while (i < stmt.Count && stmt[i].Type is "space" or "newline")
            i += 1;
        return i;
    }

    public static CompiledSql Compile(
        Twig sqlStmt,
        IEnumerable<string> nulls,
        string placeholder,
        Dictionary<string, string?>? sortMap = null)
    {
        Dictionary<string, ParamDecl>? parametersMeta = null;
        if (sqlStmt.Parameters.Count > 0)
        {
            // Same param may appear multiple times in the twig; last declaration wins (Python dict).
            parametersMeta = new Dictionary<string, ParamDecl>();
            foreach (var p in sqlStmt.Parameters)
                parametersMeta[p.Name] = p;
        }

        sortMap ??= new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        var nullsSet = new HashSet<string>(nulls.Select(n => n.ToLowerInvariant()));
        var stmt = sqlStmt.Content;
        var tokens = new List<string>();
        var parameters = new List<ParamDecl>();
        int? group = null;
        // After skipping a non-null "{{p}} is null" marker, drop the following " or ".
        var skipOrAfterNullable = false;

        var idx = 0;
        while (idx < stmt.Count)
        {
            var token = stmt[idx];
            if (token.Type == "word" && token.Value.Equals("order", StringComparison.OrdinalIgnoreCase))
            {
                var (isOrderBy, fragments, nextIdx) = CompileOrderBy(stmt, idx, sortMap);
                if (isOrderBy)
                {
                    if (fragments == null)
                    {
                        // Drop preceding whitespace so we don't leave trailing spaces before LIMIT.
                        while (tokens.Count > 0 && IsWhitespaceSqlFragment(tokens[^1]))
                            tokens.RemoveAt(tokens.Count - 1);
                    }
                    else
                    {
                        tokens.AddRange(fragments);
                    }
                    idx = nextIdx;
                    continue;
                }
            }

            if (token.Type == "brace")
            {
                if (group != null)
                {
                    if (group == token.Group)
                        group = null;
                    idx += 1;
                    continue;
                }

                if (token.NullableParameter != null && nullsSet.Contains(token.NullableParameter))
                {
                    // Elide optional group; sole remaining WHERE is cleaned below (no 1 = 1 injection).
                    StripPrecedingConnector(tokens);
                    group = token.Group;
                    skipOrAfterNullable = false;
                    idx += 1;
                    continue;
                }
            }

            if (group != null)
            {
                idx += 1;
                continue;
            }

            if (skipOrAfterNullable)
            {
                if (IsWhitespaceSqlFragment(token.Value))
                {
                    idx += 1;
                    continue;
                }
                if (token.Type == "word" && token.Value.Trim().Equals("or", StringComparison.OrdinalIgnoreCase))
                {
                    // Stay in skip mode to also drop whitespace after "or".
                    idx += 1;
                    continue;
                }
                skipOrAfterNullable = false;
            }

            if (token.Type == "sort")
            {
                // Normal usage is always consumed inside an ORDER BY clause above; a
                // stray sort() outside ORDER BY (author's responsibility, unvalidated)
                // splices its resolved value here, or nothing if null.
                if (sortMap.TryGetValue(token.Param!, out var expr) && expr != null)
                    tokens.Add(expr);
                idx += 1;
                continue;
            }

            if (token.Type == "dir")
            {
                // Direction is always folded into the paired sort()'s resolved value;
                // a stray dir() (no preceding sort() in the same ORDER BY term) renders
                // nothing.
                idx += 1;
                continue;
            }

            if (token.Type == "parameter")
            {
                if (token.Nullable)
                {
                    // Param is known non-null: drop "{{p}} is null or", keep the predicate.
                    skipOrAfterNullable = true;
                    idx += 1;
                    continue;
                }

                tokens.Add(placeholder);
                parameters.Add(parametersMeta![token.Name!]);
            }
            else
            {
                tokens.Add(token.Value);
            }

            idx += 1;
        }

        CleanupCompiledSql(tokens);

        return new CompiledSql
        {
            Content = string.Concat(tokens),
            Parameters = parameters,
        };
    }

    private static bool IsWhitespaceSqlFragment(string value) =>
        value == "" || string.IsNullOrWhiteSpace(value);

    private static bool StripPrecedingConnector(List<string> tokens)
    {
        var i = tokens.Count - 1;
        while (i >= 0 && IsWhitespaceSqlFragment(tokens[i]))
            i -= 1;

        if (i < 0)
            return false;

        var word = tokens[i].Trim().ToLowerInvariant();
        if (word is not ("and" or "or"))
            return false;

        tokens.RemoveRange(i, tokens.Count - i);
        while (tokens.Count > 0 && IsWhitespaceSqlFragment(tokens[^1]))
            tokens.RemoveAt(tokens.Count - 1);
        return true;
    }

    private static (int? Index, string? Word) NextSignificant(List<string> tokens, int i)
    {
        while (i < tokens.Count)
        {
            if (!IsWhitespaceSqlFragment(tokens[i]))
                return (i, tokens[i].Trim().ToLowerInvariant());
            i += 1;
        }
        return (null, null);
    }

    private static int? MatchOneEqualsOne(List<string> tokens, int i)
    {
        var parts = new List<(int Index, string Text)>();
        var j = i;
        while (j < tokens.Count && parts.Count < 3)
        {
            if (IsWhitespaceSqlFragment(tokens[j]))
            {
                j += 1;
                continue;
            }
            parts.Add((j, tokens[j].Trim()));
            j += 1;
            if (parts.Count == 1 && OneEqualsOneCompact.IsMatch(parts[0].Text))
                return parts[0].Index + 1;
        }

        if (parts.Count >= 3 &&
            parts[0].Text == "1" &&
            parts[1].Text == "=" &&
            parts[2].Text == "1")
        {
            return parts[2].Index + 1;
        }

        return null;
    }

    private static int TrimWsBefore(List<string> tokens, int i)
    {
        while (i > 0 && IsWhitespaceSqlFragment(tokens[i - 1]))
        {
            tokens.RemoveAt(i - 1);
            i -= 1;
        }
        return i;
    }

    internal static void CleanupCompiledSql(List<string> tokens)
    {
        var changed = true;
        while (changed)
        {
            changed = false;
            for (var i = 0; i < tokens.Count; i++)
            {
                if (IsWhitespaceSqlFragment(tokens[i]))
                    continue;
                if (!FilterClauses.Contains(tokens[i].Trim()))
                    continue;

                var (j, word) = NextSignificant(tokens, i + 1);
                if (j == null || (word != null && ClauseBoundary.Contains(word)))
                {
                    // Empty WHERE/PREWHERE at EOF, before ), ORDER/GROUP/WHERE/..., etc.
                    var oldI = i;
                    i = TrimWsBefore(tokens, i);
                    if (j == null)
                    {
                        tokens.RemoveRange(i, tokens.Count - i);
                    }
                    else
                    {
                        var adjustedJ = j.Value - (oldI - i);
                        tokens.RemoveRange(i, adjustedJ - i);
                        // Keep a space before the next keyword (not before ')').
                        if (i > 0 &&
                            i < tokens.Count &&
                            !IsWhitespaceSqlFragment(tokens[i - 1]) &&
                            !IsWhitespaceSqlFragment(tokens[i]) &&
                            tokens[i].Trim() != ")")
                        {
                            tokens.Insert(i, " ");
                        }
                    }
                    changed = true;
                    break;
                }

                var oneEnd = MatchOneEqualsOne(tokens, j.Value);
                if (oneEnd == null)
                {
                    // Real predicate; keep scanning for other WHERE/PREWHERE clauses.
                    continue;
                }

                var (k, nextWord) = NextSignificant(tokens, oneEnd.Value);
                if (k == null || (nextWord != null && ClauseBoundary.Contains(nextWord)))
                {
                    var oldI = i;
                    i = TrimWsBefore(tokens, i);
                    var adjustedEnd = oneEnd.Value - (oldI - i);
                    tokens.RemoveRange(i, adjustedEnd - i);
                    while (i < tokens.Count && IsWhitespaceSqlFragment(tokens[i]))
                    {
                        if (k != null)
                            break;
                        tokens.RemoveAt(i);
                    }
                    if (k == null)
                    {
                        while (tokens.Count > 0 && IsWhitespaceSqlFragment(tokens[^1]))
                            tokens.RemoveAt(tokens.Count - 1);
                    }
                    changed = true;
                    break;
                }

                if (nextWord is "and" or "or")
                {
                    var delEnd = k.Value + 1;
                    while (delEnd < tokens.Count && IsWhitespaceSqlFragment(tokens[delEnd]))
                        delEnd += 1;
                    tokens.RemoveRange(j.Value, delEnd - j.Value);
                    changed = true;
                    break;
                }
            }
        }
    }
}
