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
}
