// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Descriptors;
using Yaal.Execution;
using Yaal.Providers;
using Yaal.Sql;

namespace Yaal.Tests;

public class BugfixTests
{
    [Fact]
    public void Array_params_not_cached_across_items()
    {
        var helper = new DataProviderHelper();
        var sql = new CompiledSql
        {
            Content = "x",
            Parameters = new List<ParamDecl> { new() { Name = "id", Type = "integer" } },
        };
        helper.BuildParameters(sql, new Shape(data: new Dictionary<string, object?> { ["id"] = 1 }), (_, v) => v)
            .Should().Equal(1L);
        helper.BuildParameters(sql, new Shape(data: new Dictionary<string, object?> { ["id"] = 2 }), (_, v) => v)
            .Should().Equal(2L);
    }

    [Fact]
    public void Zero_is_converted()
    {
        var helper = new DataProviderHelper();
        var sql = new CompiledSql
        {
            Content = "x",
            Parameters = new List<ParamDecl> { new() { Name = "n", Type = "integer" } },
        };
        helper.BuildParameters(sql, new Shape(data: new Dictionary<string, object?> { ["n"] = 0 }), (_, v) => v)
            .Should().Equal(0L);
    }

    [Fact]
    public void Action_error_cleans_up_connection()
    {
        var leak = new LeakProvider();
        var descriptor = new Branch
        {
            Path = "p",
            Connections = new List<string> { "db" },
            InputType = "object",
            Method = "$",
            Twigs = new List<Twig>
            {
                new() { Connection = "db", Content = new List<SqlToken>(), Parameters = new List<ParamDecl>() },
            },
            Model = new DescriptorModel { Output = null },
            OutputType = "array",
        };
        var ctx = ContextFactory.CreateContext(new Branch { Path = "p" });
        var (rows, errors) = Executor.ExecuteBranch(
            descriptor, true, new Dictionary<string, IDataProvider> { ["db"] = leak }, ctx,
            new List<IDictionary<string, object?>>());
        rows.Should().BeNull();
        errors.Should().NotBeNull().And.NotBeEmpty();
        leak.Begun.Should().BeTrue();
        leak.Ended.Should().BeFalse();
        leak.Errored.Should().BeTrue();
    }

    [Fact]
    public void Use_parent_rows_skips_twigs()
    {
        var dp = new CountingProvider();
        var branch = new Branch
        {
            InputType = "object",
            UseParentRows = true,
            Method = "$.roles",
            Name = "roles",
            Twigs = new List<Twig>
            {
                new() { Connection = "db", Content = new List<SqlToken>(), Parameters = new List<ParamDecl>() },
            },
            OutputType = "array",
        };
        var ctx = ContextFactory.CreateContext(new Branch { Path = "p" });
        var parent = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["user_id"] = 1, ["role_id"] = 1 },
        };
        var (rows, err) = Executor.ExecuteBranch(
            branch, false, new Dictionary<string, IDataProvider> { ["db"] = dp }, ctx, parent);
        err.Should().BeNull();
        dp.Calls.Should().Be(0);
        rows.Should().HaveCount(1);
        rows![0].Should().ContainKey("user_id");
    }

    private sealed class LeakProvider : IDataProvider
    {
        public bool Begun { get; private set; }
        public bool Ended { get; private set; }
        public bool Errored { get; private set; }

        public void Begin() => Begun = true;
        public void End() => Ended = true;
        public void Error() => Errored = true;

        public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
            Twig twig, Shape inputShape, DataProviderHelper helper) =>
            (new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["$action"] = "error", ["message"] = "boom" },
            }, null);
    }

    private sealed class CountingProvider : IDataProvider
    {
        public int Calls { get; private set; }
        public void Begin() { }
        public void End() { }
        public void Error() { }

        public (IReadOnlyList<IDictionary<string, object?>> Rows, object? LastInsertedId) Execute(
            Twig twig, Shape inputShape, DataProviderHelper helper)
        {
            Calls++;
            return (new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { ["role_id"] = 1 },
            }, null);
        }
    }
}
