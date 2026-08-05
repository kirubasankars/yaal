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

    public static CompiledSql Compile(Twig sqlStmt, IEnumerable<string> nulls, string placeholder)
    {
        Dictionary<string, ParamDecl>? parametersMeta = null;
        if (sqlStmt.Parameters.Count > 0)
        {
            // Same param may appear multiple times in the twig; last declaration wins (Python dict).
            parametersMeta = new Dictionary<string, ParamDecl>();
            foreach (var p in sqlStmt.Parameters)
                parametersMeta[p.Name] = p;
        }

        var nullsSet = new HashSet<string>(nulls.Select(n => n.ToLowerInvariant()));
        var stmt = sqlStmt.Content;
        var tokens = new List<string>();
        var parameters = new List<ParamDecl>();
        int? group = null;
        // After skipping a non-null "{{p}} is null" marker, drop the following " or ".
        var skipOrAfterNullable = false;

        foreach (var token in stmt)
        {
            if (token.Type == "brace")
            {
                if (group != null)
                {
                    if (group == token.Group)
                        group = null;
                    continue;
                }

                if (token.NullableParameter != null && nullsSet.Contains(token.NullableParameter))
                {
                    // Elide optional group; sole remaining WHERE is cleaned below (no 1 = 1 injection).
                    StripPrecedingConnector(tokens);
                    group = token.Group;
                    skipOrAfterNullable = false;
                    continue;
                }
            }

            if (group != null)
                continue;

            if (skipOrAfterNullable)
            {
                if (IsWhitespaceSqlFragment(token.Value))
                    continue;
                if (token.Type == "word" && token.Value.Trim().Equals("or", StringComparison.OrdinalIgnoreCase))
                {
                    // Stay in skip mode to also drop whitespace after "or".
                    continue;
                }
                skipOrAfterNullable = false;
            }

            if (token.Type == "parameter")
            {
                if (token.Nullable)
                {
                    // Param is known non-null: drop "{{p}} is null or", keep the predicate.
                    skipOrAfterNullable = true;
                    continue;
                }

                tokens.Add(placeholder);
                parameters.Add(parametersMeta![token.Name!]);
            }
            else
            {
                tokens.Add(token.Value);
            }
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
