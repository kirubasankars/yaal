// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Descriptors;
using Yaal.Execution;
using Yaal.Providers;
using Yaal.Sql;

namespace Yaal.Tests;

public class ModeTests
{
    private static Branch Branch(int twigCount = 1) => new()
    {
        Path = "op",
        Connections = new List<string> { "db" },
        InputType = "object",
        Method = "$",
        Twigs = Enumerable.Range(0, twigCount)
            .Select(_ => new Twig
            {
                Connection = "db",
                Content = new List<SqlToken>(),
                Parameters = new List<ParamDecl>(),
            })
            .ToList(),
        Model = new DescriptorModel { Output = null },
        OutputType = "array",
    };

    [Fact]
    public void Mode_params_copies_into_params_bag()
    {
        var provider = new ScriptedProvider(new[]
        {
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["$mode"] = "params", ["total_count"] = 42 },
            },
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["page"] = 1, ["total_count"] = 42 },
            },
        });
        var ctx = ContextFactory.CreateContext(new Branch { Path = "op" });
        var (rows, errors) = Executor.ExecuteBranch(
            Branch(twigCount: 2), true, new Dictionary<string, IDataProvider> { ["db"] = provider }, ctx,
            new List<IDictionary<string, object?>>());

        errors.Should().BeNull();
        rows.Should().HaveCount(1);
        rows![0]["page"].Should().Be(1);
        ((Shape)ctx.GetProp("$params")!).GetProp("total_count").Should().Be(42);
        provider.Begun.Should().BeTrue();
        provider.Ended.Should().BeTrue();
        provider.Errored.Should().BeFalse();
    }

    [Fact]
    public void Mode_error_returns_soft_errors()
    {
        var provider = new ScriptedProvider(new[]
        {
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?>
                {
                    ["$mode"] = "error",
                    ["message"] = "nope",
                    ["code"] = 1,
                },
            },
        });
        var ctx = ContextFactory.CreateContext(new Branch { Path = "op" });
        var (rows, errors) = Executor.ExecuteBranch(
            Branch(), true, new Dictionary<string, IDataProvider> { ["db"] = provider }, ctx,
            new List<IDictionary<string, object?>>());

        rows.Should().BeNull();
        errors.Should().NotBeNull().And.HaveCount(1);
        errors![0]["message"]!.ToString().Should().Be("nope");
        provider.Errored.Should().BeTrue();
        provider.Ended.Should().BeFalse();
    }

    [Fact]
    public void Mode_break_returns_rows_without_mode_column()
    {
        var provider = new ScriptedProvider(new[]
        {
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["$mode"] = "break", ["id"] = 1, ["name"] = "a" },
                new Dictionary<string, object?> { ["$mode"] = "break", ["id"] = 2, ["name"] = "b" },
            },
        });
        var ctx = ContextFactory.CreateContext(new Branch { Path = "op" });
        var (rows, errors) = Executor.ExecuteBranch(
            Branch(), true, new Dictionary<string, IDataProvider> { ["db"] = provider }, ctx,
            new List<IDictionary<string, object?>>());

        errors.Should().BeNull();
        rows.Should().HaveCount(2);
        rows![0].Should().NotContainKey("$mode");
        rows[0]["id"].Should().Be(1);
        rows[1]["name"]!.ToString().Should().Be("b");
        provider.Ended.Should().BeTrue();
    }

    [Fact]
    public void Mode_json_parses_string_column()
    {
        var provider = new ScriptedProvider(new[]
        {
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?>
                {
                    ["$mode"] = "json",
                    ["json"] = "{\"id\":7,\"ok\":true}",
                },
            },
        });
        var ctx = ContextFactory.CreateContext(new Branch { Path = "op" });
        var (rows, errors) = Executor.ExecuteBranch(
            Branch(), true, new Dictionary<string, IDataProvider> { ["db"] = provider }, ctx,
            new List<IDictionary<string, object?>>());

        errors.Should().BeNull();
        rows.Should().HaveCount(1);
        rows![0].Should().ContainKey("id");
        Convert.ToInt64(rows[0]["id"]).Should().Be(7);
        provider.Ended.Should().BeTrue();
    }

    [Fact]
    public void Ordinary_rows_without_mode()
    {
        var provider = new ScriptedProvider(new[]
        {
            new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["id"] = 1 },
                new Dictionary<string, object?> { ["id"] = 2 },
            },
        });
        var ctx = ContextFactory.CreateContext(new Branch { Path = "op" });
        var (rows, errors) = Executor.ExecuteBranch(
            Branch(), true, new Dictionary<string, IDataProvider> { ["db"] = provider }, ctx,
            new List<IDictionary<string, object?>>());

        errors.Should().BeNull();
        rows.Should().HaveCount(2);
        rows![0]["id"].Should().Be(1);
    }

    private sealed class ScriptedProvider : IDataProvider
    {
        private readonly Queue<IReadOnlyList<IDictionary<string, object?>>> _responses;

        public ScriptedProvider(IEnumerable<IReadOnlyList<IDictionary<string, object?>>> responses) =>
            _responses = new Queue<IReadOnlyList<IDictionary<string, object?>>>(responses);

        public bool Begun { get; private set; }
        public bool Ended { get; private set; }
        public bool Errored { get; private set; }

        public void Begin() => Begun = true;
        public void End() => Ended = true;
        public void Error() => Errored = true;

        public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
            Twig twig, Shape inputShape, DataProviderHelper helper) =>
            (_responses.Dequeue(), null);
    }
}
