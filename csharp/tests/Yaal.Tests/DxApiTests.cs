using FluentAssertions;

namespace Yaal.Tests;

public class DxApiTests
{
    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..", "tests", "fixtures", "api"));

    [Fact]
    public void Unsupported_scheme_throws()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var act = () => y.SetupDataProvider("db", "oracle://x");
        act.Should().Throw<UnsupportedDatabaseUrlException>();
    }

    [Fact]
    public void Missing_provider_throws()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var act = () => y.GetDataProvider("db");
        act.Should().Throw<YaalException>().WithMessage("*not configured*");
    }

    [Fact]
    public void Missing_descriptor_throws()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var act = () => y.CreateDescriptor("no/such");
        act.Should().Throw<DescriptorNotFoundException>();
    }

    [Fact]
    public void Explain_sql_binds_args_id()
    {
        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", "sqlite3:///");
        var plan = y.ExplainSql("user/get", args: new { id = 1 });
        plan.Should().NotBeEmpty();
        plan[0]["sql"]!.ToString().Should().Contain("?");
        var parameters = (List<object?>)plan[0]["parameters"]!;
        parameters.Should().ContainSingle().Which.Should().Be(1L);
    }

    [Fact]
    public void Registers_clickhouse_scheme()
    {
        var y = new Yaal(FixtureApi, debug: true);
        y.SetupDataProvider("db", "clickhouse://default:@127.0.0.1:8123/default");
        y.GetDataProvider("db").Should().NotBeNull();
    }
}
