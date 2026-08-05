using FluentAssertions;

namespace Yaal.Tests;

public class DatabaseUrlTests
{
    [Fact]
    public void Parses_sqlite_absolute_four_slash()
    {
        var (scheme, options) = DatabaseUrl.Parse("sqlite3:////tmp/app.db");
        scheme.Should().Be("sqlite3");
        options.Database.Should().Be("/tmp/app.db");
    }

    [Fact]
    public void Parses_sqlite_relative()
    {
        var (scheme, options) = DatabaseUrl.Parse("sqlite3://./data/app.db");
        scheme.Should().Be("sqlite3");
        options.Database.Should().Be("./data/app.db");
    }

    [Fact]
    public void Parses_sqlite_memory()
    {
        var (_, options) = DatabaseUrl.Parse("sqlite3:///");
        options.Database.Should().Be("");
    }

    [Fact]
    public void Parses_postgres()
    {
        var (scheme, options) = DatabaseUrl.Parse("postgresql://yaal:yaal@127.0.0.1:54329/yaal");
        scheme.Should().Be("postgresql");
        options.Username.Should().Be("yaal");
        options.Password.Should().Be("yaal");
        options.Host.Should().Be("127.0.0.1");
        options.Port.Should().Be("54329");
        options.Database.Should().Be("yaal");
    }

    [Fact]
    public void Parses_mysql_and_clickhouse()
    {
        var (mysql, mOpts) = DatabaseUrl.Parse("mysql://yaal:yaal@127.0.0.1:33069/yaal");
        mysql.Should().Be("mysql");
        mOpts.Port.Should().Be("33069");

        var (ch, cOpts) = DatabaseUrl.Parse("clickhouse://yaal:yaal@127.0.0.1:9000/yaal");
        ch.Should().Be("clickhouse");
        cOpts.Port.Should().Be("9000");
    }

    [Fact]
    public void Rejects_garbage_url()
    {
        var act = () => DatabaseUrl.Parse("not-a-url");
        act.Should().Throw<ArgumentException>();
    }
}
