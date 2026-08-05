using FluentAssertions;

namespace Yaal.Tests;

public class BranchMapTests
{
    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..", "tests", "fixtures", "api"));

    [Fact]
    public void Builds_user_get_descriptor_with_roles_branch()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var descriptor = y.CreateDescriptor("user/get");
        descriptor.Method.Should().Be("$");
        descriptor.Twigs.Should().NotBeNull().And.NotBeEmpty();
        descriptor.OutputType.Should().Be("object");
        descriptor.PartitionBy.Should().Be("user_id");
        descriptor.Branches.Should().NotBeNull();
        descriptor.Branches!.Should().ContainSingle(b => b.Name == "roles");
        descriptor.Branches![0].UseParentRows.Should().BeTrue();
    }
}
