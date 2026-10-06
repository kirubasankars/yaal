// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Sql;

public static class GroupDesugar
{
    private const string ArgsPrefix = "$args.";

    private static readonly Dictionary<string, ParamDecl> EmptyHeaderDecls =
        new(StringComparer.OrdinalIgnoreCase);

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

    /// <summary>
    /// Row key for a group body placeholder, or null when it binds from the header.
    /// Row fields are always written bare; a `$args.` name always binds from the
    /// runtime args and must be declared in the header.
    /// </summary>
    private static string? GroupBodyRowKey(
        string fullName,
        IReadOnlyDictionary<string, ParamDecl> headerDecls,
        string groupSource)
    {
        if (fullName.Equals(groupSource, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                "optional_groups(...) blob {{" + fullName + "}} must not be used in its body");
        }
        if (!fullName.StartsWith(ArgsPrefix, StringComparison.Ordinal))
            return fullName;
        if (headerDecls.TryGetValue(fullName, out var decl) && ParamTypeUtil.IsArrayType(decl.Type))
        {
            throw new InvalidOperationException(
                "optional_groups(...) body cannot use array parameter {{" + fullName +
                "}}; put the list in each blob row instead");
        }
        return null;
    }

    private static List<SqlToken> MarkGroupFields(
        List<SqlToken> body,
        List<string> fieldOrder,
        IReadOnlyDictionary<string, ParamDecl> headerDecls,
        string groupSource)
    {
        var outBody = new List<SqlToken>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var rowFullNames = new HashSet<string>();
        foreach (var t in body)
        {
            if (t.Type != "parameter")
            {
                outBody.Add(t);
                continue;
            }
            var name = OptionalDesugar.ParameterNameFromToken(t);
            var rowKey = GroupBodyRowKey(name, headerDecls, groupSource);
            if (rowKey == null)
            {
                outBody.Add(t);
                continue;
            }
            rowFullNames.Add(name);
            if (seen.Add(rowKey))
                fieldOrder.Add(rowKey);
            outBody.Add(new SqlToken
            {
                Type = "group_field",
                Name = rowKey,
                Value = t.Value,
            });
        }

        // Row fields come from the blob, so they cannot gate an optional(...) nested
        // in the body; one keyed only on row fields would always elide.
        foreach (var t in outBody)
        {
            if (t.Type != "brace")
                continue;
            var condition = t.OptionalWhenCondition;
            if (condition != null && !condition.StartsWith(ArgsPrefix, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "optional_when(...) inside optional_groups(...) must use a " +
                    "{{$args.param}} condition; blob row fields are always required");
            }
            if (t.NullableParameters is not { Count: > 0 } nullableParams)
                continue;
            var kept = new List<string>();
            foreach (var p in nullableParams)
            {
                if (!rowFullNames.Contains(p))
                    kept.Add(p);
            }
            if (kept.Count == 0)
            {
                if (condition != null)
                {
                    // The condition still gates the block, so a row-field-only body is fine.
                    t.NullableParameters = null;
                    continue;
                }
                throw new InvalidOperationException(
                    "optional(...) inside optional_groups(...) must use at least one " +
                    "{{$args.param}}; blob row fields are always required");
            }
            t.NullableParameters = kept;
        }

        return outBody;
    }

    public static List<SqlToken> Desugar(
        List<SqlToken>? tokens,
        IReadOnlyDictionary<string, ParamDecl>? headerDecls = null)
    {
        if (tokens == null)
            return new List<SqlToken>();
        headerDecls ??= EmptyHeaderDecls;

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
                    var body = Desugar(bodyRaw, headerDecls);
                    body = OptionalDesugar.Desugar(body);

                    var fieldOrder = new List<string>();
                    body = MarkGroupFields(body, fieldOrder, headerDecls, sourceName);
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
