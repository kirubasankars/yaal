// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Sql;

public static class OptionalDesugar
{
    public static string ParameterNameFromToken(SqlToken token) =>
        token.Value[2..^2].Trim().ToLowerInvariant();

    private static int SkipWs(List<SqlToken> tokens, int i)
    {
        while (i < tokens.Count && tokens[i].Type is "space" or "newline")
            i += 1;
        return i;
    }

    /// <summary>Index of the ')' matching the '(' at openIndex, or null when unclosed.</summary>
    private static int? MatchingCloseBrace(List<SqlToken> tokens, int openIndex)
    {
        var group = tokens[openIndex].Group;
        for (var k = openIndex + 1; k < tokens.Count; k++)
        {
            var t = tokens[k];
            if (t.Type == "brace" && t.Value == ")" && t.Group == group)
                return k;
        }
        return null;
    }

    /// <summary>
    /// Names that gate an optional(...). A nested optional_groups_*(...) contributes nothing:
    /// its blob source elides the group on its own, and its body placeholders come from
    /// each blob row. Also reports whether a nested group was seen.
    /// </summary>
    private static (List<string> Names, bool SawGroups) BodyParamNames(List<SqlToken> body)
    {
        var names = new List<string>();
        var seen = new HashSet<string>();
        var sawGroups = false;

        void Add(SqlToken token)
        {
            var name = ParameterNameFromToken(token);
            if (seen.Add(name))
                names.Add(name);
        }

        var i = 0;
        var n = body.Count;
        while (i < n)
        {
            var t = body[i];
            if (GroupDesugar.GroupJoinForToken(t) != null)
            {
                sawGroups = true;
                var j = SkipWs(body, i + 1);
                if (j < n && body[j].Type == "brace" && body[j].Value == "(")
                {
                    var close = MatchingCloseBrace(body, j);
                    if (close is { } k)
                    {
                        i = k + 1;
                        continue;
                    }
                }
            }
            if (t.Type == "parameter")
                Add(t);
            i += 1;
        }

        return (names, sawGroups);
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
            if (tok.Type == "word" && tok.Value.Equals("optional", StringComparison.OrdinalIgnoreCase))
            {
                var j = SkipWs(tokens, i + 1);

                if (j < n && tokens[j].Type == "brace" && tokens[j].Value == "(")
                {
                    var openTok = tokens[j];
                    var group = openTok.Group;
                    var k = j + 1;
                    while (k < n)
                    {
                        var t = tokens[k];
                        if (t.Type == "brace" && t.Value == ")" && t.Group == group)
                            break;
                        k += 1;
                    }
                    if (k >= n)
                        throw new InvalidOperationException("unclosed optional(...)");

                    var body = Desugar(tokens.GetRange(j + 1, k - (j + 1)));
                    var (paramNames, sawGroups) = BodyParamNames(body);

                    if (paramNames.Count == 0 && !sawGroups)
                        throw new InvalidOperationException(
                            "optional(...) requires at least one {{param}} in its body");

                    if (paramNames.Count > 0)
                    {
                        openTok.NullableParameters = paramNames;
                    }
                    else if (sawGroups)
                    {
                        // Nothing but a group inside: the group elides itself, so the
                        // wrapper parens must disappear with it rather than emit "()".
                        openTok.OptionalGroupsWrapper = true;
                    }
                    result.Add(openTok);
                    result.AddRange(body);
                    result.Add(tokens[k]);
                    i = k + 1;
                    continue;
                }
            }

            result.Add(tok);
            i += 1;
        }

        return result;
    }

    private static void EnsureNoNestedWhen(List<SqlToken> body)
    {
        foreach (var t in body)
        {
            if (t.Type == "word" && t.Value.Equals("optional_when", StringComparison.OrdinalIgnoreCase))
                throw new InvalidOperationException("nested optional_when(...) is not supported");
        }
    }

    /// <summary>
    /// Expand optional_when({{cond}}, expr) into (expr) gated on cond plus expr params.
    /// </summary>
    public static List<SqlToken> DesugarWhen(List<SqlToken>? tokens)
    {
        if (tokens == null)
            return new List<SqlToken>();

        var result = new List<SqlToken>();
        var i = 0;
        var n = tokens.Count;
        while (i < n)
        {
            var tok = tokens[i];
            if (tok.Type == "word" && tok.Value.Equals("optional_when", StringComparison.OrdinalIgnoreCase))
            {
                var j = SkipWs(tokens, i + 1);

                if (j < n && tokens[j].Type == "brace" && tokens[j].Value == "(")
                {
                    var openTok = tokens[j];
                    if (MatchingCloseBrace(tokens, j) is not { } k)
                        throw new InvalidOperationException("unclosed optional_when(...)");

                    var inner = tokens.GetRange(j + 1, k - (j + 1));
                    var p = SkipWs(inner, 0);
                    if (p >= inner.Count || inner[p].Type != "parameter")
                        throw new InvalidOperationException(
                            "optional_when(...) requires {{param}} as the first argument");

                    var condition = ParameterNameFromToken(inner[p]);
                    p = SkipWs(inner, p + 1);
                    if (p >= inner.Count || inner[p].Type != "word" || inner[p].Value != ",")
                        throw new InvalidOperationException(
                            "optional_when(...) requires a comma after the condition parameter");

                    var bodyStart = SkipWs(inner, p + 1);
                    var bodyRaw = inner.GetRange(bodyStart, inner.Count - bodyStart);
                    EnsureNoNestedWhen(bodyRaw);
                    var body = Desugar(bodyRaw);
                    var (paramNames, sawGroups) = BodyParamNames(body);

                    if (paramNames.Count == 0 && !sawGroups)
                        throw new InvalidOperationException(
                            "optional_when(...) requires at least one {{param}} in its body");

                    openTok.OptionalWhenCondition = condition;
                    if (paramNames.Count > 0)
                    {
                        openTok.NullableParameters = paramNames;
                    }
                    else if (sawGroups)
                    {
                        // Nothing but a group inside: the group elides itself, so the
                        // wrapper parens must disappear with it rather than emit "()".
                        openTok.OptionalGroupsWrapper = true;
                    }
                    result.Add(openTok);
                    result.AddRange(body);
                    result.Add(tokens[k]);
                    i = k + 1;
                    continue;
                }
            }

            result.Add(tok);
            i += 1;
        }

        return result;
    }
}
