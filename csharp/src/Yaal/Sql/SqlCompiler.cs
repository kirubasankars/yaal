namespace Yaal.Sql;

public static class SqlCompiler
{
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
                    if (!StripPrecedingConnector(tokens))
                        tokens.Add("1 = 1");
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
}
