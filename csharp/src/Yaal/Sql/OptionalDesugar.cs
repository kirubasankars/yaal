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
                    var paramNames = new List<string>();
                    var seen = new HashSet<string>();
                    foreach (var t in body)
                    {
                        if (t.Type == "parameter")
                        {
                            var name = ParameterNameFromToken(t);
                            if (seen.Add(name))
                                paramNames.Add(name);
                        }
                    }

                    if (paramNames.Count == 0)
                        throw new InvalidOperationException("optional(...) requires exactly one {{param}} in its body");
                    if (paramNames.Count > 1)
                        throw new InvalidOperationException(
                            "optional(...) requires exactly one {{param}} in its body, found: " +
                            string.Join(", ", paramNames));

                    var p = paramNames[0];
                    result.Add(openTok);
                    result.Add(new SqlToken { Type = "parameter", Value = "{{" + p + "}}" });
                    result.Add(new SqlToken { Type = "space", Value = " " });
                    result.Add(new SqlToken { Type = "word", Value = "is" });
                    result.Add(new SqlToken { Type = "space", Value = " " });
                    result.Add(new SqlToken { Type = "word", Value = "null" });
                    result.Add(new SqlToken { Type = "space", Value = " " });
                    result.Add(new SqlToken { Type = "word", Value = "or" });
                    result.Add(new SqlToken { Type = "space", Value = " " });
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
