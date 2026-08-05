// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Descriptors;
using Yaal.Execution;

namespace Yaal.Tests;

public class OutputTypeMetaTests
{
    [Fact]
    public void Flat_maps_fields()
    {
        var outputModel = new Dictionary<string, object?>
        {
            ["type"] = "array",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["id"] = new Dictionary<string, object?> { ["mapped"] = "id" },
                ["name"] = new Dictionary<string, object?> { ["mapped"] = "name" },
            },
        };
        var rows = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["id"] = 1, ["name"] = "a", ["extra"] = 9 },
            new Dictionary<string, object?> { ["id"] = 2, ["name"] = "b", ["extra"] = 8 },
        };

        var list = (List<object?>)OutputMapper.Map("array", outputModel, null, rows);
        list.Should().HaveCount(2);
        var first = (IDictionary<string, object?>)list[0]!;
        first["id"].Should().Be(1);
        first["name"].Should().Be("a");
        first.Should().NotContainKey("extra");
    }

    [Fact]
    public void Nested_item_wrapper_rejected()
    {
        var outputModel = new Dictionary<string, object?>
        {
            ["type"] = "array",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["type"] = "object",
                ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["id"] = new Dictionary<string, object?> { ["mapped"] = "id" },
                },
            },
        };

        Action act = () => OutputSchema.Normalize(outputModel);
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*bare properties.type*");
    }

    [Fact]
    public void Sibling_type_meta_rejected()
    {
        var outputModel = new Dictionary<string, object?>
        {
            ["type"] = "array",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["type"] = "object",
                ["id"] = new Dictionary<string, object?> { ["mapped"] = "id" },
            },
        };

        Action act = () => OutputMapper.Map("array", outputModel, null,
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["id"] = 1 },
            });
        act.Should().Throw<InvalidOperationException>()
            .WithMessage("*bare properties.type*");
    }

    [Fact]
    public void Field_named_type_via_mapped_still_works()
    {
        var outputModel = new Dictionary<string, object?>
        {
            ["type"] = "array",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["type"] = new Dictionary<string, object?> { ["mapped"] = "kind" },
                ["id"] = new Dictionary<string, object?> { ["mapped"] = "id" },
            },
        };
        var rows = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["id"] = 1, ["kind"] = "user" },
        };

        var row = (IDictionary<string, object?>)((List<object?>)OutputMapper.Map("array", outputModel, null, rows))[0]!;
        row["type"].Should().Be("user");
        row["id"].Should().Be(1);
    }

    [Fact]
    public void Parent_rows_object_child_model_is_flat()
    {
        var childModel = new Dictionary<string, object?>
        {
            ["type"] = "object",
            ["parent_rows"] = true,
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                ["name"] = new Dictionary<string, object?> { ["mapped"] = "name" },
            },
        };

        var normalized = OutputSchema.Normalize(childModel)!;
        ((IDictionary<string, object?>)normalized["properties"]!).Should().ContainKey("name");
        ((IDictionary<string, object?>)normalized["properties"]!).Should().NotContainKey("type");

        var result = OutputMapper.Map(
            "object",
            childModel,
            null,
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["id"] = 1, ["name"] = "a" },
            });
        ((IDictionary<string, object?>)result)["name"].Should().Be("a");
    }
}
