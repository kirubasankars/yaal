// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Sql;

/// <summary>Merge adjacent whitespace and static SQL token runs after parse.</summary>
public static class TwigCompaction
{
    private static readonly HashSet<string> MergeReserved = new(StringComparer.OrdinalIgnoreCase)
    {
        "order", "by", "or",
    };

    private static readonly HashSet<string> Structural = new(StringComparer.OrdinalIgnoreCase)
    {
        "parameter", "brace", "sort", "dir",
    };

    public static List<SqlToken> Compact(List<SqlToken> content)
    {
        if (content.Count == 0)
            return content;

        var outTokens = new List<SqlToken>();
        string? bufType = null;
        var bufValue = new System.Text.StringBuilder();

        void Flush()
        {
            if (bufType == "space" && bufValue.Length > 0)
                outTokens.Add(new SqlToken { Type = "space", Value = bufValue.ToString() });
            else if (bufType == "sql" && bufValue.Length > 0)
                outTokens.Add(new SqlToken { Type = "sql", Value = bufValue.ToString() });
            bufType = null;
            bufValue.Clear();
        }

        foreach (var token in content)
        {
            var t = token.Type;
            if (Structural.Contains(t))
            {
                Flush();
                outTokens.Add(token);
                continue;
            }

            if (t is "space" or "newline")
            {
                if (bufType == "space")
                    bufValue.Append(token.Value);
                else
                {
                    Flush();
                    bufType = "space";
                    bufValue.Append(token.Value);
                }
                continue;
            }

            if (t is "word" or "sql")
            {
                var val = token.Value;
                if (MergeReserved.Contains(val.Trim()))
                {
                    Flush();
                    outTokens.Add(token);
                    continue;
                }

                if (bufType == "sql")
                    bufValue.Append(val);
                else
                {
                    Flush();
                    bufType = "sql";
                    bufValue.Append(val);
                }
                continue;
            }

            Flush();
            outTokens.Add(token);
        }

        Flush();
        return outTokens;
    }
}
