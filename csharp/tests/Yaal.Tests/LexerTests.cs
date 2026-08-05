// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Sql;

namespace Yaal.Tests;

public class LexerTests
{
    [Fact]
    public void Lexes_parameter_header_and_bind()
    {
        var sql = "--($args.id integer)--\nselect * from t where id = {{$args.id}}";
        var tokens = Lexer.Lex(sql)!;
        tokens.Should().Contain(t => t.Type == "dash");
        tokens.Should().Contain(t => t.Type == "parameter" && t.Value.Contains("$args.id"));
    }

    [Fact]
    public void Lexes_optional_word_and_braces()
    {
        var tokens = Lexer.Lex("optional(col = {{p}})")!;
        tokens.Should().Contain(t => t.Type == "word" && t.Value == "optional");
        tokens.Should().Contain(t => t.Type == "brace" && t.Value == "(" && t.Group == 1);
        tokens.Should().Contain(t => t.Type == "brace" && t.Value == ")" && t.Group == 1);
    }

    [Fact]
    public void Lexes_sql_connection_marker()
    {
        var tokens = Lexer.Lex("--sql(other)--\nselect 1")!;
        tokens.Should().Contain(t => t.Type == "dash" && t.Value.Contains("sql(other)"));
    }
}
