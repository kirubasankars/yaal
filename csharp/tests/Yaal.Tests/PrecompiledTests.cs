// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Descriptors;

namespace Yaal.Tests;

public class PrecompiledTests
{
    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "tests", "fixtures", "api"));

    [Fact]
    public void Load_python_compiled_json_preserves_twig_tokens()
    {
        // Build artifact with Python shape by compiling via TrunkBuilder then
        // writing a minimal JSON matching export_descriptor.
        var y = new Yaal(FixtureApi, debug: true);
        var branch = y.CreateDescriptor("user/get");
        branch.Twigs.Should().NotBeNull().And.NotBeEmpty();

        var json = JsonUtil.Serialize(BranchToDict(branch), indented: true);
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        var loaded = Precompiled.Import(doc.RootElement);

        loaded.Path.Should().Be("user/get");
        loaded.Twigs.Should().NotBeNull().And.NotBeEmpty();
        loaded.Twigs![0].Content.Should().NotBeEmpty();
        loaded.Twigs[0].Content[0].Type.Should().NotBeNullOrEmpty();
        loaded.Validators.Should().NotBeNull();
        loaded.Validators!.Should().ContainKey("args");
    }

    [Fact]
    public void Yaal_loads_from_precompiled_directory()
    {
        var y = new Yaal(FixtureApi, debug: true);
        var branch = y.CreateDescriptor("user/list");
        var json = JsonUtil.Serialize(BranchToDict(branch), indented: true);

        var dir = Path.Combine(Path.GetTempPath(), "yaal-pre-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(Path.Combine(dir, "user"));
        File.WriteAllText(Path.Combine(dir, "user", "list.json"), json);

        try
        {
            var loaded = Precompiled.LoadFromDirectory(dir, "user/list");
            loaded.Twigs.Should().NotBeNull().And.NotBeEmpty();
            loaded.OutputType.Should().NotBeNullOrEmpty();
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* ignore */ }
        }
    }

    private static Dictionary<string, object?> BranchToDict(Branch b)
    {
        var d = new Dictionary<string, object?>
        {
            ["name"] = b.Name,
            ["method"] = b.Method,
            ["path"] = b.Path,
            ["input_type"] = b.InputType,
            ["output_type"] = b.OutputType,
            ["use_parent_rows"] = b.UseParentRows,
        };
        if (b.PartitionBy != null)
            d["partition_by"] = b.PartitionBy;
        if (b.Model != null)
        {
            d["model"] = new Dictionary<string, object?>
            {
                ["args"] = b.Model.Args,
                ["payload"] = b.Model.Payload,
                ["output"] = b.Model.Output,
            };
        }
        if (b.Parameters != null)
        {
            d["parameters"] = b.Parameters.ToDictionary(
                kv => kv.Key,
                kv => (object?)new Dictionary<string, object?>
                {
                    ["name"] = kv.Value.Name,
                    ["type"] = kv.Value.Type,
                    ["required"] = kv.Value.Required,
                });
        }
        if (b.Twigs != null)
        {
            d["twigs"] = b.Twigs.Select(t =>
            {
                var td = new Dictionary<string, object?>
                {
                    ["connection"] = t.Connection,
                    ["content"] = t.Content.Select(tok =>
                    {
                        var tokd = new Dictionary<string, object?>
                        {
                            ["type"] = tok.Type,
                            ["value"] = tok.Value,
                        };
                        if (tok.Group != null) tokd["group"] = tok.Group;
                        if (tok.Name != null) tokd["name"] = tok.Name;
                        if (tok.Nullable) tokd["nullable"] = true;
                        if (tok.NullableParameter != null)
                            tokd["nullable_parameter"] = tok.NullableParameter;
                        return tokd;
                    }).ToList(),
                    ["parameters"] = t.Parameters.Select(p => new Dictionary<string, object?>
                    {
                        ["name"] = p.Name,
                        ["type"] = p.Type,
                        ["required"] = p.Required,
                    }).ToList(),
                };
                if (t.Nullable != null)
                    td["nullable"] = t.Nullable;
                return td;
            }).ToList();
        }
        if (b.Branches != null)
            d["branches"] = b.Branches.Select(BranchToDict).ToList();
        if (b.Connections != null)
            d["connections"] = b.Connections;
        return d;
    }
}
