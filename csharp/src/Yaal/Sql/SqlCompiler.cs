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
        "where", "prewhere", "having",
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
    /// filter-clause cleanup (WHERE/PREWHERE/HAVING) -- which detects clause boundaries by exact-matching
    /// the word "order" -- still recognizes it.
    /// </summary>
    private static (bool IsOrderBy, List<string>? Fragments, int NextIdx) CompileOrderBy(
        List<SqlToken> stmt, int orderIdx, Dictionary<string, string?> sortMap)
    {
        var n = stmt.Count;
        var j = SkipWsTokens(stmt, orderIdx + 1);
        if (j >= n || !SqlTokenUtil.TokenEquals(stmt[j], "by"))
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
        Dictionary<string, string?>? sortMap = null,
        IReadOnlyDictionary<string, int>? arrayLengths = null,
        IReadOnlyDictionary<string, int>? groupCounts = null,
        IReadOnlyDictionary<string, List<Dictionary<string, int>>>? groupFieldLengths = null,
        IReadOnlyDictionary<string, List<Dictionary<string, bool>>>? groupFieldIsArray = null)
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
        var arrayLengthMap = arrayLengths != null
            ? new Dictionary<string, int>(arrayLengths, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var groupCountMap = groupCounts != null
            ? new Dictionary<string, int>(groupCounts, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var groupFieldLengthMap = groupFieldLengths != null
            ? new Dictionary<string, List<Dictionary<string, int>>>(
                groupFieldLengths, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, List<Dictionary<string, int>>>(StringComparer.OrdinalIgnoreCase);
        var groupFieldIsArrayMap = groupFieldIsArray != null
            ? new Dictionary<string, List<Dictionary<string, bool>>>(
                groupFieldIsArray, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, List<Dictionary<string, bool>>>(StringComparer.OrdinalIgnoreCase);
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
            if (SqlTokenUtil.TokenEquals(token, "order"))
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

                if (token.Value == "(" && token.OptionalGroupsWrapper)
                {
                    var wrapperCloseIdx = FindMatchingCloseParen(stmt, idx);
                    if (wrapperCloseIdx == null)
                        throw new InvalidOperationException("unclosed optional(...)");
                    if (WrappedGroupsAllElide(
                            stmt, idx + 1, wrapperCloseIdx.Value, nullsSet, groupCountMap))
                    {
                        // Only a group inside, and it is gone: drop the parens too.
                        StripPrecedingConnector(tokens);
                        idx = wrapperCloseIdx.Value + 1;
                        continue;
                    }
                }

                if (token.Value == "(" && token.GroupSource != null)
                {
                    if (ShouldElideNullableGroup(token, nullsSet))
                    {
                        StripPrecedingConnector(tokens);
                        var elidedCloseIdx = FindMatchingCloseParen(stmt, idx);
                        if (elidedCloseIdx == null)
                            throw new InvalidOperationException("unclosed optional_groups(...)");
                        idx = elidedCloseIdx.Value + 1;
                        continue;
                    }

                    var source = token.GroupSource;
                    var skey = source.ToLowerInvariant();
                    if (!groupCountMap.TryGetValue(skey, out var nGroups))
                        throw new InvalidOperationException(
                            "missing optional_groups count for {{" + source + "}}");
                    if (nGroups == 0)
                    {
                        StripPrecedingConnector(tokens);
                        var closeIdx0 = FindMatchingCloseParen(stmt, idx);
                        if (closeIdx0 == null)
                            throw new InvalidOperationException("unclosed optional_groups(...)");
                        idx = closeIdx0.Value + 1;
                        continue;
                    }

                    if (!groupFieldLengthMap.TryGetValue(skey, out var rowsLens) ||
                        rowsLens.Count != nGroups)
                    {
                        throw new InvalidOperationException(
                            "missing optional_groups field lengths for {{" + source + "}}");
                    }
                    groupFieldIsArrayMap.TryGetValue(skey, out var rowsArray);
                    rowsArray ??= new List<Dictionary<string, bool>>();

                    var closeIdx = FindMatchingCloseParen(stmt, idx);
                    if (closeIdx == null)
                        throw new InvalidOperationException("unclosed optional_groups(...)");

                    var branches = new List<string>();
                    for (var gi = 0; gi < nGroups; gi++)
                    {
                        var rowArray = gi < rowsArray.Count
                            ? rowsArray[gi]
                            : new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                        var (bt, bp) = CompileStmtRange(
                            stmt,
                            idx + 1,
                            closeIdx.Value,
                            nullsSet,
                            parametersMeta!,
                            arrayLengthMap,
                            placeholder,
                            sortMap,
                            source,
                            gi,
                            rowsLens[gi],
                            rowArray);
                        branches.Add("(" + string.Concat(bt) + ")");
                        parameters.AddRange(bp);
                    }
                    var joined = string.Join(" or ", branches);
                    // Multiple rows leave a top-level "or"; parenthesize so an
                    // adjacent AND does not bind tighter than this group.
                    tokens.Add(branches.Count > 1 ? "(" + joined + ")" : joined);
                    idx = closeIdx.Value + 1;
                    continue;
                }

                if (ShouldElideNullableGroup(token, nullsSet))
                {
                    // Elide optional/nullable group; sole remaining WHERE is cleaned below (no 1 = 1 injection).
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
                if (SqlTokenUtil.TokenEquals(token, "or"))
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

                AppendCompiledParameter(
                    token.Name!,
                    parametersMeta!,
                    arrayLengthMap,
                    placeholder,
                    tokens,
                    parameters);
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

    private static void AppendCompiledParameter(
        string tokenName,
        Dictionary<string, ParamDecl> parametersMeta,
        Dictionary<string, int> arrayLengthMap,
        string placeholder,
        List<string> tokens,
        List<ParamDecl> parameters)
    {
        var meta = parametersMeta[tokenName];
        if (!ParamTypeUtil.IsArrayType(meta.Type))
        {
            tokens.Add(placeholder);
            parameters.Add(meta);
            return;
        }

        if (!arrayLengthMap.TryGetValue(tokenName, out var count))
            throw new InvalidOperationException("missing array value for {{" + tokenName + "}}");
        if (count == 0)
            throw new InvalidOperationException(
                "empty array for {{" + tokenName + "}}; IN list cannot be empty");

        var elemType = ParamTypeUtil.ElementType(meta.Type);
        tokens.Add(string.Join(", ", Enumerable.Repeat(placeholder, count)));
        for (var i = 0; i < count; i++)
        {
            parameters.Add(new ParamDecl
            {
                Name = meta.Name,
                Type = elemType,
                ArrayElement = true,
            });
        }
    }

    /// <summary>True when every optional_groups(...) inside a wrapper optional(...) elides.</summary>
    private static bool WrappedGroupsAllElide(
        List<SqlToken> stmt,
        int start,
        int end,
        HashSet<string> nullsSet,
        Dictionary<string, int> groupCountMap)
    {
        for (var idx = start; idx < end; idx++)
        {
            var token = stmt[idx];
            if (token.Type != "brace" || token.Value != "(")
                continue;
            if (token.GroupSource is not { } source)
                continue;
            if (ShouldElideNullableGroup(token, nullsSet))
                continue;
            // A missing count is an error the normal expansion path reports.
            if (groupCountMap.TryGetValue(source.ToLowerInvariant(), out var count) && count == 0)
                continue;
            return false;
        }
        return true;
    }

    private static int? FindMatchingCloseParen(List<SqlToken> stmt, int openIdx)
    {
        var grp = stmt[openIdx].Group;
        for (var j = openIdx + 1; j < stmt.Count; j++)
        {
            var t = stmt[j];
            if (t.Type == "brace" && t.Value == ")" && t.Group == grp)
                return j;
        }
        return null;
    }

    private static (List<string> Tokens, List<ParamDecl> Parameters) CompileStmtRange(
        List<SqlToken> stmt,
        int start,
        int end,
        HashSet<string> nullsSet,
        Dictionary<string, ParamDecl> parametersMeta,
        Dictionary<string, int> arrayLengthMap,
        string placeholder,
        Dictionary<string, string?> sortMap,
        string groupSource,
        int groupIndex,
        Dictionary<string, int> groupRowLengths,
        Dictionary<string, bool> groupRowIsArray)
    {
        var tokens = new List<string>();
        var parameters = new List<ParamDecl>();
        int? group = null;
        var skipOrAfterNullable = false;
        var idx = start;
        while (idx < end)
        {
            var token = stmt[idx];
            if (SqlTokenUtil.TokenEquals(token, "order"))
            {
                var (isOrderBy, fragments, nextIdx) = CompileOrderBy(stmt, idx, sortMap);
                if (isOrderBy)
                {
                    if (fragments != null)
                        tokens.AddRange(fragments);
                    idx = Math.Min(nextIdx, end);
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
                if (ShouldElideNullableGroup(token, nullsSet))
                {
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
                if (SqlTokenUtil.TokenEquals(token, "or"))
                {
                    idx += 1;
                    continue;
                }
                skipOrAfterNullable = false;
            }

            if (token.Type == "sort")
            {
                if (sortMap.TryGetValue(token.Param!, out var expr) && expr != null)
                    tokens.Add(expr);
                idx += 1;
                continue;
            }

            if (token.Type == "dir")
            {
                idx += 1;
                continue;
            }

            if (token.Type == "group_field")
            {
                var field = token.Name!;
                if (!groupRowLengths.TryGetValue(field, out var flen) &&
                    !groupRowLengths.TryGetValue(field.ToLowerInvariant(), out flen))
                {
                    throw new InvalidOperationException(
                        "missing optional_groups field length for {{" + field + "}}");
                }
                var isArr = groupRowIsArray.TryGetValue(field, out var arrFlag) && arrFlag;
                if (!isArr)
                    isArr = groupRowIsArray.TryGetValue(field.ToLowerInvariant(), out arrFlag) && arrFlag;
                if (!isArr && flen > 1)
                    isArr = true;
                AppendGroupFieldSlots(
                    field, flen, groupSource, groupIndex, isArr, placeholder, tokens, parameters);
                idx += 1;
                continue;
            }

            if (token.Type == "parameter")
            {
                if (token.Nullable)
                {
                    skipOrAfterNullable = true;
                    idx += 1;
                    continue;
                }
                AppendCompiledParameter(
                    token.Name!,
                    parametersMeta,
                    arrayLengthMap,
                    placeholder,
                    tokens,
                    parameters);
            }
            else
            {
                tokens.Add(token.Value);
            }

            idx += 1;
        }

        return (tokens, parameters);
    }

    private static void AppendGroupFieldSlots(
        string fieldName,
        int count,
        string groupSource,
        int groupIndex,
        bool fieldIsArray,
        string placeholder,
        List<string> tokens,
        List<ParamDecl> parameters)
    {
        if (count == 0)
            throw new InvalidOperationException(
                "empty array for optional_groups field {{" + fieldName + "}}");
        tokens.Add(string.Join(", ", Enumerable.Repeat(placeholder, count)));
        for (var sub = 0; sub < count; sub++)
        {
            parameters.Add(new ParamDecl
            {
                Name = fieldName,
                Type = "string",
                GroupSource = groupSource,
                GroupField = fieldName,
                GroupIndex = groupIndex,
                GroupSubindex = fieldIsArray ? sub : null,
            });
        }
    }

    private static bool ShouldElideNullableGroup(SqlToken token, HashSet<string> nullsSet)
    {
        if (token.OptionalWhenCondition is { } condition &&
            nullsSet.Contains(condition.ToLowerInvariant()))
        {
            // An absent condition drops the block outright, before the body is judged.
            return true;
        }
        if (token.GroupSource != null)
            return nullsSet.Contains(token.GroupSource.ToLowerInvariant());
        if (token.NullableParameters is { Count: > 0 } list)
        {
            var lowered = list.Select(n => n.ToLowerInvariant()).ToList();
            var missing = lowered.Where(n => nullsSet.Contains(n)).ToList();
            if (missing.Count == lowered.Count)
                return true;
            if (missing.Count == 0)
                return false;
            throw new InvalidOperationException(
                "optional(...) requires every parameter to be provided or all omitted; " +
                "partial parameters: " + string.Join(", ", missing.OrderBy(n => n, StringComparer.Ordinal)));
        }
        if (token.NullableParameter != null)
            return nullsSet.Contains(token.NullableParameter);
        return false;
    }

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
                if (word is "and" or "or")
                {
                    var (afterConnector, afterConnectorWord) =
                        NextSignificant(tokens, j!.Value + 1);
                    if (afterConnector != null &&
                        afterConnectorWord != null &&
                        !ClauseBoundary.Contains(afterConnectorWord))
                    {
                        tokens.RemoveRange(j.Value, afterConnector.Value - j.Value);
                        changed = true;
                        break;
                    }
                }
                if (j == null || (word != null && ClauseBoundary.Contains(word)))
                {
                    // Empty WHERE/PREWHERE/HAVING at EOF, before ), ORDER/GROUP/WHERE/..., etc.
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
                    // Real predicate; keep scanning for other filter clauses.
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
