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
    /// Names that gate an optional(...). A nested optional_groups(...) contributes only its
    /// blob source; its body placeholders come from each blob row, never from request args.
    /// </summary>
    private static List<string> BodyParamNames(List<SqlToken> body)
    {
        var names = new List<string>();
        var seen = new HashSet<string>();

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
            if (t.Type == "word" && t.Value.Equals("optional_groups", StringComparison.OrdinalIgnoreCase))
            {
                var j = SkipWs(body, i + 1);
                if (j < n && body[j].Type == "brace" && body[j].Value == "(")
                {
                    var close = MatchingCloseBrace(body, j);
                    if (close is { } k)
                    {
                        for (var m = j + 1; m < k; m++)
                        {
                            if (body[m].Type == "parameter")
                            {
                                Add(body[m]);
                                break;
                            }
                        }
                        i = k + 1;
                        continue;
                    }
                }
            }
            if (t.Type == "parameter")
                Add(t);
            i += 1;
        }

        return names;
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
                    var paramNames = BodyParamNames(body);

                    if (paramNames.Count == 0)
                        throw new InvalidOperationException(
                            "optional(...) requires at least one {{param}} in its body");

                    openTok.NullableParameters = paramNames;
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
