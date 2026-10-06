// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Descriptors;

namespace Yaal.Tests;

public class BranchCsEmitterTests
{
    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "tests", "fixtures", "api"));

    [Fact]
    public void Compile_cs_writes_registry_and_descriptor_files()
    {
        var dir = Path.Combine(Path.GetTempPath(), "yaal-cs-" + Guid.NewGuid().ToString("n"));
        try
        {
            var result = BranchCsEmitter.CompileApi(FixtureApi, dir, listPaths: new[] { "user/get" });
            result.WrittenPaths.Should().Contain("user/get/UserGet.g.cs");
            result.WrittenPaths.Should().Contain("YaalDescriptorRegistry.g.cs");

            var source = File.ReadAllText(Path.Combine(dir, "user/get/UserGet.g.cs"));
            source.Should().Contain("public static partial class UserGet");
            source.Should().Contain("Path = \"user/get\"");
            source.Should().Contain("new List<SqlToken>");
            source.Should().Contain("[\"type\"] = \"object\"");

            var registry = File.ReadAllText(Path.Combine(dir, "YaalDescriptorRegistry.g.cs"));
            registry.Should().Contain("[\"user/get\"] = UserGet.Descriptor");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* ignore */ }
        }
    }

    private static string ReadEmitted(
        string dir, BranchCsEmitter.CompileResult result, string pathPrefix)
    {
        var rel = result.WrittenPaths.Single(p => p.StartsWith(pathPrefix, StringComparison.Ordinal));
        return File.ReadAllText(Path.Combine(dir, rel));
    }

    [Fact]
    public void Compile_cs_round_trips_group_token_metadata()
    {
        var dir = Path.Combine(Path.GetTempPath(), "yaal-cs-" + Guid.NewGuid().ToString("n"));
        try
        {
            var result = BranchCsEmitter.CompileApi(
                FixtureApi, dir, listPaths: new[] { "user/groups", "user/groups_in_optional" });

            var groups = ReadEmitted(dir, result, "user/groups/");
            groups.Should().Contain("GroupSource = \"$args.pairs\"");
            groups.Should().Contain("GroupJoin = \"or\"");

            var nested = ReadEmitted(dir, result, "user/groups_in_optional/");
            nested.Should().Contain("GroupJoin = \"or\"");
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* ignore */ }
        }
    }

    [Theory]
    [InlineData("optional_groups_or", "or")]
    [InlineData("optional_groups_and", "and")]
    public void Compile_cs_round_trips_a_wrapper_only_group(string keyword, string join)
    {
        var api = Path.Combine(Path.GetTempPath(), "yaal-api-" + Guid.NewGuid().ToString("n"));
        var dir = Path.Combine(Path.GetTempPath(), "yaal-cs-" + Guid.NewGuid().ToString("n"));
        try
        {
            var branchDir = Path.Combine(api, "user", "wrap");
            Directory.CreateDirectory(branchDir);
            File.WriteAllText(Path.Combine(branchDir, "$.sql"),
                "--($args.pairs blob)--\n\nselect * from (select 1 as id union select 2) t\n" +
                "where optional(" + keyword + "({{$args.pairs}}, id = {{id}}))\n");
            File.WriteAllText(Path.Combine(branchDir, "$.output.json"),
                "{\"type\": \"array\", \"properties\": {\"id\": {\"mapped\": \"id\"}}}\n");

            var result = BranchCsEmitter.CompileApi(api, dir, listPaths: new[] { "user/wrap" });
            var source = ReadEmitted(dir, result, "user/wrap/");

            source.Should().Contain($"GroupJoin = \"{join}\"");
            // Without the wrapper flag a precompiled descriptor would emit an empty ()
            // instead of dropping the parens once the blob has no rows.
            source.Should().Contain("OptionalGroupsWrapper = true");
        }
        finally
        {
            try { Directory.Delete(api, recursive: true); } catch { /* ignore */ }
            try { Directory.Delete(dir, recursive: true); } catch { /* ignore */ }
        }
    }
}
