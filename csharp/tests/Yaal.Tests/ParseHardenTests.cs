using FluentAssertions;
using Yaal.Sql;

namespace Yaal.Tests;

public class ParseHardenTests
{
    [Fact]
    public void Tab_and_cr_are_space_tokens()
    {
        var tokens = Lexer.Lex("a\tb\rc")!;
        tokens.Select(t => t.Type).Should().Equal("word", "space", "word", "space", "word");
    }

    [Fact]
    public void Sql_line_comment_discarded()
    {
        var tokens = Lexer.Lex("select 1 -- ignore\nfrom t")!;
        tokens.Should().NotContain(t => t.Type == "dash");
        tokens.Select(t => t.Value).Where(v => v is not (" " or "\t" or "\r"))
            .Should().Equal("select", "1", "\n", "from", "t");
    }

    [Fact]
    public void Sql_directive_still_dash()
    {
        var tokens = Lexer.Lex("select 1 --sql-- select 2")!;
        tokens.Should().Contain(t => t.Type == "dash" && t.Value == "--sql--");
    }

    [Fact]
    public void Unclosed_string_errors()
    {
        Action act = () => Lexer.Lex("select 'oops");
        act.Should().Throw<InvalidOperationException>().WithMessage("*unclosed string*");
    }

    [Fact]
    public void Leading_ws_then_header()
    {
        var ast = SqlParser.Parse(Lexer.Lex("\n\n--(id integer)--\nselect {{id}}"), "$")!;
        ast.Parameters.Should().ContainKey("id");
        ast.Parameters!["id"].Type.Should().Be("integer");
    }
}
