// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Diagnostics;
using Yaal.Benchmarks.Generated;
using Yaal.Descriptors;
using YaalClient = global::Yaal.Yaal;

const string descriptorPath = "user/list";
const int warmup = 10;
const int iterations = 200;

var apiRoot = FindRepoRoot();
var live = new YaalClient(apiRoot, debug: true);
var branch = live.CreateDescriptor(descriptorPath);
var jsonText = Precompiled.ExportJson(branch, indented: false);

var precompiledDir = Path.Combine(Path.GetTempPath(), "yaal-bench-" + Guid.NewGuid().ToString("n"));
var relDir = Path.Combine(precompiledDir, "user");
Directory.CreateDirectory(relDir);
var jsonPath = Path.Combine(relDir, "list.json");
File.WriteAllText(jsonPath, jsonText);

try
{
    Console.WriteLine($"Descriptor: {descriptorPath}");
    Console.WriteLine($"JSON size: {jsonText.Length:N0} chars");
    Console.WriteLine($"Warmup: {warmup}, iterations: {iterations}");
    Console.WriteLine();

    Bench("Live SQL parse (CreateDescriptor)", () =>
    {
        var y = new YaalClient(apiRoot, debug: true);
        _ = y.CreateDescriptor(descriptorPath);
    });

    Bench("JSON load (file + deserialize)", () => _ = Precompiled.LoadFile(jsonPath));

    Bench("JSON deserialize (string in memory)", () => _ = Precompiled.Import(jsonText));

    Bench("CS static descriptor (UserList.Descriptor)", () => _ = UserList.Descriptor);

    Bench("Startup: Yaal + precompiled JSON + ExplainSql", () =>
    {
        var y = new YaalClient(apiRoot, precompiled: precompiledDir);
        _ = y.ExplainSql(descriptorPath, args: new { active = 1 });
    });

    Bench("Startup: Yaal + RegisterDescriptor(CS) + ExplainSql", () =>
    {
        var y = new YaalClient(apiRoot);
        y.RegisterDescriptor(descriptorPath, UserList.Descriptor);
        _ = y.ExplainSql(descriptorPath, args: new { active = 1 });
    });

    Bench("Startup: Yaal debug + ExplainSql", () =>
    {
        var y = new YaalClient(apiRoot, debug: true);
        _ = y.ExplainSql(descriptorPath, args: new { active = 1 });
    });
}
finally
{
    try { Directory.Delete(precompiledDir, recursive: true); } catch { /* ignore */ }
}

void Bench(string name, Action action)
{
    for (var i = 0; i < warmup; i++)
        action();

    GC.Collect();
    GC.WaitForPendingFinalizers();
    GC.Collect();

    var sw = Stopwatch.StartNew();
    for (var i = 0; i < iterations; i++)
        action();
    sw.Stop();

    var msPerOp = sw.Elapsed.TotalMilliseconds / iterations;
    Console.WriteLine($"{name,-50} {msPerOp,8:F3} ms/op");
}

static string FindRepoRoot()
{
    var dir = AppContext.BaseDirectory;
    for (var i = 0; i < 12; i++)
    {
        if (Directory.Exists(Path.Combine(dir, "tests", "fixtures", "api")))
            return Path.Combine(dir, "tests", "fixtures", "api");
        dir = Directory.GetParent(dir)?.FullName
            ?? throw new InvalidOperationException("Could not find tests/fixtures/api");
    }

    throw new InvalidOperationException("Could not find tests/fixtures/api");
}
