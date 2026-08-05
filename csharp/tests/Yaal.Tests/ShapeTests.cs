using FluentAssertions;

namespace Yaal.Tests;

public class ShapeTests
{
    [Fact]
    public void Lowercases_object_keys()
    {
        var shape = new Shape(data: new Dictionary<string, object?> { ["Name"] = "a" });
        shape.GetProp("name").Should().Be("a");
    }

    [Fact]
    public void Returns_default_from_schema()
    {
        var schema = new Dictionary<string, object?>
        {
            ["type"] = "object",
            ["properties"] = new Dictionary<string, object?>
            {
                ["x"] = new Dictionary<string, object?>
                {
                    ["type"] = "integer",
                    ["default"] = 7,
                },
            },
        };
        var shape = new Shape(schema: schema);
        shape.GetProp("x").Should().Be(7);
    }

    [Fact]
    public void Coerces_boolean_strings()
    {
        Shape.CoerceBoolean("yes").Should().BeTrue();
        Shape.CoerceBoolean("0").Should().BeFalse();
        Shape.CoerceBoolean(1).Should().BeTrue();
    }

    [Fact]
    public void Rejects_reserved_keys_in_data()
    {
        var act = () => new Shape(data: new Dictionary<string, object?> { ["$parent"] = 1 });
        act.Should().Throw<ArgumentException>();
    }

    [Fact]
    public void Extras_and_parent_paths()
    {
        var args = new Shape(data: new Dictionary<string, object?> { ["id"] = 1 });
        var extras = new Dictionary<string, Shape> { ["$args"] = args };
        var shape = new Shape(extras: extras);
        shape.GetProp("$args.id").Should().Be(1);
    }

    [Fact]
    public void Length_and_index_for_arrays()
    {
        var schema = new Dictionary<string, object?>
        {
            ["type"] = "array",
            ["properties"] = new Dictionary<string, object?>
            {
                ["name"] = new Dictionary<string, object?> { ["type"] = "string" },
            },
        };
        var data = new List<object?>
        {
            new Dictionary<string, object?> { ["name"] = "a" },
            new Dictionary<string, object?> { ["name"] = "b" },
        };
        var shape = new Shape(schema: schema, data: data);
        shape.GetProp("$length").Should().Be(2);
        ((Shape)shape.GetProp("@1")!).GetProp("name").Should().Be("b");
    }
}
