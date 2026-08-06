// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Descriptors;

namespace Yaal.Tests;

public class InputFromHeadersTests
{
    private sealed class HeaderContentReader : IContentReader
    {
        private readonly Dictionary<string, string> _sql;
        private readonly List<string> _files;

        public HeaderContentReader(Dictionary<string, string> sql, List<string>? files = null)
        {
            _sql = sql;
            _files = files ?? new List<string> { "$" };
        }

        public string RootPath => "";

        public string? GetSql(string method, string path) =>
            _sql.TryGetValue(method, out var sql) ? sql : null;

        public Dictionary<string, object?> GetConfig(string path, string? outputMapper) =>
            new(StringComparer.OrdinalIgnoreCase) { ["output.model"] = null };

        public List<string>? ListSql(string path) => _files.ToList();
    }

    [Fact]
    public void Derives_args_and_payload_schema()
    {
        var reader = new HeaderContentReader(new Dictionary<string, string>
        {
            ["$"] = "--($args.id integer, name! string)--\nselect {{$args.id}}, {{name}}\n",
        });
        var trunk = TrunkBuilder.CreateTrunk("op", null, reader)!;
        var args = trunk.Model!.Args!;
        var payload = trunk.Model.Payload!;
        var argsProps = (Dictionary<string, object?>)args["properties"]!;
        var payloadProps = (Dictionary<string, object?>)payload["properties"]!;
        ((Dictionary<string, object?>)argsProps["id"]!)["type"].Should().Be("integer");
        args.ContainsKey("required").Should().BeFalse();
        ((Dictionary<string, object?>)payloadProps["name"]!)["type"].Should().Be("string");
        ((IList<object?>)payload["required"]!).Should().Contain("name");
    }

    [Fact]
    public void Maps_bool_and_float_types()
    {
        var reader = new HeaderContentReader(new Dictionary<string, string>
        {
            ["$"] = "--(flag bool, amount float)--\nselect {{flag}}, {{amount}}\n",
        });
        var trunk = TrunkBuilder.CreateTrunk("op", null, reader)!;
        var props = (Dictionary<string, object?>)trunk.Model!.Payload!["properties"]!;
        ((Dictionary<string, object?>)props["flag"]!)["type"].Should().Be("boolean");
        ((Dictionary<string, object?>)props["amount"]!)["type"].Should().Be("number");
    }

    [Fact]
    public void Required_missing_soft_error()
    {
        var y = new Yaal("", new HeaderContentReader(new Dictionary<string, string>
        {
            ["$"] = "--(id! integer, name! string)--\nselect {{id}} as id, {{name}} as name\n",
        }), debug: true);
        y.SetupDataProvider("db", "sqlite3:///");
        var result = (Dictionary<string, object?>)y.Query("op", payload: new { name = "x" })!;
        result.Should().ContainKey("errors");
    }

    [Fact]
    public void Type_mismatch_soft_error()
    {
        var y = new Yaal("", new HeaderContentReader(new Dictionary<string, string>
        {
            ["$"] = "--(id integer)--\nselect {{id}} as id\n",
        }), debug: true);
        y.SetupDataProvider("db", "sqlite3:///");
        var result = (Dictionary<string, object?>)y.Query(
            "op", payload: new Dictionary<string, object?> { ["id"] = "not-an-int" })!;
        result.Should().ContainKey("errors");
    }

    [Fact]
    public void Conflict_type_across_files()
    {
        var reader = new HeaderContentReader(
            new Dictionary<string, string>
            {
                ["$"] = "--($args.id integer)--\nselect {{$args.id}} as id\n",
                ["$.child"] = "--($args.id string)--\nselect {{$args.id}} as id\n",
            },
            new List<string> { "$", "$.child" });
        Action act = () => TrunkBuilder.CreateTrunk("op", null, reader);
        act.Should().Throw<InvalidOperationException>().WithMessage("*conflicting parameter*");
    }

    [Fact]
    public void Same_declaration_twice_ok()
    {
        var reader = new HeaderContentReader(
            new Dictionary<string, string>
            {
                ["$"] = "--($args.id! integer)--\nselect {{$args.id}} as id\n",
                ["$.child"] = "--($args.id! integer)--\nselect {{$args.id}} as id\n",
            },
            new List<string> { "$", "$.child" });
        var trunk = TrunkBuilder.CreateTrunk("op", null, reader)!;
        ((IList<object?>)trunk.Model!.Args!["required"]!).Should().Contain("id");
    }

    [Fact]
    public void Fixture_user_get_without_input_yaml()
    {
        var fixtureApi = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "tests", "fixtures", "api"));

        var y = new Yaal(fixtureApi, debug: true);
        var d = y.CreateDescriptor("user/get");
        var argsProps = (Dictionary<string, object?>)d.Model!.Args!["properties"]!;
        ((Dictionary<string, object?>)argsProps["id"]!)["type"].Should().Be("integer");
        d.Model.Args.ContainsKey("required").Should().BeFalse();
    }
}
