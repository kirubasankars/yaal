// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Descriptors;

static string FindRepoRoot()
{
    var dir = AppContext.BaseDirectory;
    for (var i = 0; i < 12; i++)
    {
        if (Directory.Exists(Path.Combine(dir, "tests", "fixtures", "api")))
            return dir;
        dir = Directory.GetParent(dir)?.FullName
            ?? throw new InvalidOperationException("Could not find repo root (tests/fixtures/api)");
    }

    throw new InvalidOperationException("Could not find repo root (tests/fixtures/api)");
}

var repoRoot = FindRepoRoot();
var apiRoot = Path.Combine(repoRoot, "tests", "fixtures", "api");
var outDir = Path.Combine(repoRoot, "csharp", "benchmarks", "Yaal.Benchmarks", "Generated");
if (Directory.Exists(outDir))
    Directory.Delete(outDir, recursive: true);
Directory.CreateDirectory(outDir);
var result = BranchCsEmitter.CompileApi(
    apiRoot,
    outDir,
    @namespace: "Yaal.Benchmarks.Generated",
    listPaths: new[] { "user/list" });
Console.WriteLine($"Generated {result.WrittenPaths.Count} files in {outDir}");
