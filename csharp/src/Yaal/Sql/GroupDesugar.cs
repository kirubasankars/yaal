// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Sql;

public static class GroupDesugar
{
    private static int SkipWs(List<SqlToken> tokens, int i)
    {
        while (i < tokens.Count && tokens[i].Type is "space" or "newline")
            i += 1;
        return i;
    }

    private static void EnsureNoNestedGroups(List<SqlToken> body)
    {
        for (var i = 0; i < body.Count; i++)
        {
            if (body[i].Type == "word" &&
                body[i].Value.Equals("optional_groups", StringComparison.OrdinalIgnoreCase))
            {
                throw new InvalidOperationException("nested optional_groups(...) is not supported");
            }
        }
    }

    private static List<SqlToken> MarkGroupFields(List<SqlToken> body, List<string> fieldOrder)
    {
        var outBody = new List<SqlToken>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var t in body)
        {
            if (t.Type != "parameter")
            {
                outBody.Add(t);
                continue;
            }
            var name = OptionalDesugar.ParameterNameFromToken(t);
            if (seen.Add(name))
                fieldOrder.Add(name);
            outBody.Add(new SqlToken
            {
                Type = "group_field",
                Name = name,
                Value = t.Value,
            });
        }
        return outBody;
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
                tok.Value.Equals("optional_groups", StringComparison.OrdinalIgnoreCase))
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
                        throw new InvalidOperationException("unclosed optional_groups(...)");

                    var inner = tokens.GetRange(j + 1, k - (j + 1));
                    var p = SkipWs(inner, 0);
                    if (p >= inner.Count || inner[p].Type != "parameter")
                        throw new InvalidOperationException(
                            "optional_groups(...) requires {{blob}} as the first argument");

                    var sourceName = OptionalDesugar.ParameterNameFromToken(inner[p]);
                    p = SkipWs(inner, p + 1);
                    if (p >= inner.Count || inner[p].Type != "word" || inner[p].Value != ",")
                        throw new InvalidOperationException(
                            "optional_groups(...) requires a comma after the blob parameter");

                    var bodyStart = SkipWs(inner, p + 1);
                    var bodyRaw = inner.GetRange(bodyStart, inner.Count - bodyStart);
                    EnsureNoNestedGroups(bodyRaw);
                    var body = Desugar(bodyRaw);
                    body = OptionalDesugar.Desugar(body);

                    var fieldOrder = new List<string>();
                    body = MarkGroupFields(body, fieldOrder);
                    if (fieldOrder.Count == 0)
                        throw new InvalidOperationException(
                            "optional_groups(...) requires at least one {{field}} in its body");

                    openTok.GroupSource = sourceName;
                    openTok.GroupFields = fieldOrder;
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
