namespace Yaal.Sql;

public static class Lexer
{
    public static List<SqlToken>? Lex(string? content)
    {
        if (string.IsNullOrEmpty(content))
            return null;

        var tokens = new List<SqlToken>();
        var current = 0;
        var contentLength = content.Length;
        var braceGroup = 0;

        while (current < contentLength)
        {
            var p = content[current];
            var p1 = current + 1 < contentLength ? content[current + 1] : '\0';

            if (p is '\'' or '"')
            {
                var (next, t) = LexString(current, content, p);
                current = next;
                tokens.Add(t);
            }
            else if (p == '-' && p1 == '-')
            {
                var (next, t) = LexDash(current, content);
                current = next;
                tokens.Add(t);
            }
            else if (p == '{' && p1 == '{')
            {
                var (next, t) = LexCurlyBraces(current, content);
                current = next;
                tokens.Add(t);
            }
            else if (p == ' ')
            {
                var (next, t) = LexSpaces(current, content);
                current = next;
                tokens.Add(t);
            }
            else if (p is '(' or ')')
            {
                var (next, t) = LexBrace(current, content);
                current = next;
                if (p == '(')
                {
                    braceGroup += 1;
                    t.Group = braceGroup;
                }
                if (p == ')')
                {
                    t.Group = braceGroup;
                    braceGroup -= 1;
                }
                tokens.Add(t);
            }
            else if (p == '\n')
            {
                var (next, t) = LexNewline(current, content);
                current = next;
                tokens.Add(t);
            }
            else
            {
                var (next, t) = LexWord(current, content);
                current = next;
                tokens.Add(t);
            }
        }

        return tokens;
    }

    private static (int, SqlToken) LexDash(int current, string content)
    {
        var token = new List<char> { '-', '-' };
        current += 2;
        var contentLength = content.Length;
        while (current < contentLength)
        {
            var p = content[current];
            var p1 = current + 1 < contentLength ? content[current + 1] : '\0';
            if (p == '-' && p1 == '-')
            {
                token.Add('-');
                token.Add('-');
                current += 2;
                break;
            }
            token.Add(content[current]);
            current += 1;
        }
        return (current, new SqlToken { Type = "dash", Value = new string(token.ToArray()) });
    }

    private static (int, SqlToken) LexCurlyBraces(int current, string content)
    {
        var token = new List<char> { '{', '{' };
        current += 2;
        var contentLength = content.Length;
        while (current < contentLength)
        {
            var p = content[current];
            var p1 = current + 1 < contentLength ? content[current + 1] : '\0';
            if (p == '}' && p1 == '}')
            {
                token.Add('}');
                token.Add('}');
                current += 2;
                break;
            }
            token.Add(content[current]);
            current += 1;
        }
        return (current, new SqlToken { Type = "parameter", Value = new string(token.ToArray()) });
    }

    private static (int, SqlToken) LexString(int current, string content, char quote)
    {
        var token = new List<char> { quote };
        current += 1;
        var contentLength = content.Length;
        while (current < contentLength)
        {
            var p = content[current];
            var p1 = current + 1 < contentLength ? content[current + 1] : '\0';
            if (p == quote && p1 != quote)
            {
                token.Add(quote);
                current += 1;
                break;
            }
            token.Add(content[current]);
            current += 1;
        }
        return (current, new SqlToken { Type = "string", Value = new string(token.ToArray()) });
    }

    private static (int, SqlToken) LexSpaces(int current, string content)
    {
        var start = current;
        while (current < content.Length && content[current] == ' ')
            current += 1;
        return (current, new SqlToken { Type = "space", Value = content[start..current] });
    }

    private static (int, SqlToken) LexWord(int current, string content)
    {
        const string singles = "()'\"\n ";
        const string doubles = "{}-";
        var token = new List<char>();
        while (current < content.Length)
        {
            var p = content[current];
            var p1 = current + 1 < content.Length ? content[current + 1] : '\0';
            if (singles.Contains(p) || (doubles.Contains(p) && p == p1))
                break;
            token.Add(p);
            current += 1;
        }
        return (current, new SqlToken { Type = "word", Value = new string(token.ToArray()) });
    }

    private static (int, SqlToken) LexBrace(int current, string content) =>
        (current + 1, new SqlToken { Type = "brace", Value = content[current].ToString() });

    private static (int, SqlToken) LexNewline(int current, string content) =>
        (current + 1, new SqlToken { Type = "newline", Value = content[current].ToString() });
}
