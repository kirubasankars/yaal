using FluentAssertions;
using Yaal.Descriptors;

namespace Yaal.Tests;

public class PathEscapeTests
{
    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "tests", "fixtures", "api"));

    [Fact]
    public void Rejects_paths_outside_api_root()
    {
        var reader = new FileContentReader(FixtureApi);
        var act = () => reader.GetSql("$", "../secrets");
        act.Should().Throw<PathEscapeException>();
    }
}
