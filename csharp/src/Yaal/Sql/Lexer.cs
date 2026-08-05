// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

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
                if (t != null)
                    tokens.Add(t);
            }
            else if (p == '{' && p1 == '{')
            {
                var (next, t) = LexCurlyBraces(current, content);
                current = next;
                tokens.Add(t);
            }
            else if (p is ' ' or '\t' or '\r')
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

    private static (int, SqlToken?) LexDash(int current, string content)
    {
        var contentLength = content.Length;
        current += 2; // skip --

        if (current >= contentLength)
            return (current, null);

        // Malformed header: space(s) between -- and (
        var j = current;
        while (j < contentLength && content[j] is ' ' or '\t' or '\r')
            j += 1;
        if (j > current && j < contentLength && content[j] == '(')
        {
            throw new InvalidOperationException(
                "invalid parameter header: use --(name type)-- without space after --");
        }

        if (content[current] == '(')
        {
            var token = new List<char> { '-', '-', '(' };
            current += 1;
            while (current < contentLength)
            {
                if (current + 2 < contentLength &&
                    content[current] == ')' &&
                    content[current + 1] == '-' &&
                    content[current + 2] == '-')
                {
                    token.Add(')');
                    token.Add('-');
                    token.Add('-');
                    current += 3;
                    return (current, new SqlToken { Type = "dash", Value = new string(token.ToArray()) });
                }
                token.Add(content[current]);
                current += 1;
            }
            throw new InvalidOperationException("unclosed parameter header --(...)--");
        }

        if (content.AsSpan(current).StartsWith("sql", StringComparison.Ordinal))
        {
            var token = new List<char> { '-', '-' };
            while (current < contentLength)
            {
                var p = content[current];
                var p1 = current + 1 < contentLength ? content[current + 1] : '\0';
                if (p == '-' && p1 == '-')
                {
                    token.Add('-');
                    token.Add('-');
                    current += 2;
                    return (current, new SqlToken { Type = "dash", Value = new string(token.ToArray()) });
                }
                token.Add(p);
                current += 1;
            }
            throw new InvalidOperationException("unclosed --sql-- directive");
        }

        // SQL line comment: discard through end of line (keep newline for the lexer)
        while (current < contentLength && content[current] != '\n')
            current += 1;
        return (current, null);
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
                return (current, new SqlToken { Type = "parameter", Value = new string(token.ToArray()) });
            }
            token.Add(content[current]);
            current += 1;
        }
        throw new InvalidOperationException("unclosed {{...}} parameter");
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
                return (current, new SqlToken { Type = "string", Value = new string(token.ToArray()) });
            }
            token.Add(content[current]);
            current += 1;
        }
        throw new InvalidOperationException("unclosed string literal");
    }

    private static (int, SqlToken) LexSpaces(int current, string content)
    {
        var start = current;
        while (current < content.Length && content[current] is ' ' or '\t' or '\r')
            current += 1;
        return (current, new SqlToken { Type = "space", Value = content[start..current] });
    }

    private static (int, SqlToken) LexWord(int current, string content)
    {
        const string singles = "()'\"\n \t\r";
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
