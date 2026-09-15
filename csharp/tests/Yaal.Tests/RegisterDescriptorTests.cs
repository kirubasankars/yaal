// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using FluentAssertions;
using Yaal.Descriptors;

namespace Yaal.Tests;

public class RegisterDescriptorTests
{
    private static string FixtureApi =>
        Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory, "..", "..", "..", "..", "..", "..",
            "tests", "fixtures", "api"));

    [Fact]
    public void RegisterDescriptor_serves_query_without_precompiled_files()
    {
        var live = new Yaal(FixtureApi, debug: true);
        var branch = live.CreateDescriptor("user/get");

        var y = new Yaal(FixtureApi);
        y.RegisterDescriptor("user/get", branch);

        var explain = y.ExplainSql("user/get", args: new { id = 1 });
        explain.Should().NotBeEmpty();
    }

    [Fact]
    public void Registered_descriptor_used_before_corrupt_precompiled_dir()
    {
        var live = new Yaal(FixtureApi, debug: true);
        var branch = live.CreateDescriptor("user/get");

        var corruptDir = Path.Combine(Path.GetTempPath(), "yaal-bad-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(Path.Combine(corruptDir, "user"));
        File.WriteAllText(Path.Combine(corruptDir, "user", "get.json"), "{not-json");

        try
        {
            var y = new Yaal(FixtureApi, precompiled: corruptDir);
            y.RegisterDescriptor("user/get", branch);
            var explain = y.ExplainSql("user/get", args: new { id = 1 });
            explain.Should().NotBeEmpty();
        }
        finally
        {
            try { Directory.Delete(corruptDir, recursive: true); } catch { /* ignore */ }
        }
    }

    [Fact]
    public void UnregisterDescriptor_falls_back_to_precompiled()
    {
        var live = new Yaal(FixtureApi, debug: true);
        var branch = live.CreateDescriptor("user/list");
        var json = Precompiled.ExportJson(branch, indented: true);

        var dir = Path.Combine(Path.GetTempPath(), "yaal-pre-" + Guid.NewGuid().ToString("n"));
        Directory.CreateDirectory(Path.Combine(dir, "user"));
        File.WriteAllText(Path.Combine(dir, "user", "list.json"), json);

        try
        {
            var y = new Yaal(FixtureApi, precompiled: dir);
            y.RegisterDescriptor("user/list", branch);
            y.UnregisterDescriptor("user/list");
            var explain = y.ExplainSql("user/list", args: new { active = 1 });
            explain.Should().NotBeEmpty();
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* ignore */ }
        }
    }
}
