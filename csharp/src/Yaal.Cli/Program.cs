// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Descriptors;

if (args.Length == 0 || args[0] is "-h" or "--help" or "help")
{
    PrintHelp();
    return 0;
}

var command = args[0];
var rest = args.Skip(1).ToArray();
return command switch
{
    "compile" => RunCompile(rest),
    _ => UnknownCommand(command),
};

static int RunCompile(string[] args)
{
    string? api = null;
    string? outDir = null;
    var format = "json";
    var ns = "Yaal.Generated";

    for (var i = 0; i < args.Length; i++)
    {
        switch (args[i])
        {
            case "--api" when i + 1 < args.Length:
                api = args[++i];
                break;
            case "--out" when i + 1 < args.Length:
                outDir = args[++i];
                break;
            case "--format" when i + 1 < args.Length:
                format = args[++i].ToLowerInvariant();
                break;
            case "--namespace" when i + 1 < args.Length:
                ns = args[++i];
                break;
            default:
                Console.Error.WriteLine("Unknown compile argument: " + args[i]);
                return 2;
        }
    }

    if (string.IsNullOrEmpty(api) || string.IsNullOrEmpty(outDir))
    {
        Console.Error.WriteLine("compile requires --api and --out");
        return 2;
    }

    List<string> written = format switch
    {
        "json" => PrecompiledCompiler.CompileJson(api, outDir),
        "cs" => BranchCsEmitter.CompileApi(api, outDir, ns).WrittenPaths,
        _ => throw new InvalidOperationException("Unsupported format: " + format),
    };

    foreach (var path in written)
        Console.WriteLine(path);
    return 0;
}

static int UnknownCommand(string command)
{
    Console.Error.WriteLine("Unknown command: " + command);
    PrintHelp();
    return 2;
}

static void PrintHelp()
{
    Console.WriteLine("""
        yaal — compile Yaal SQL→JSON descriptors

        Usage:
          yaal compile --api <api-root> --out <dir> [--format json|cs] [--namespace <ns>]

        Formats:
          json   Portable JSON artifacts (default; compatible with Python yaal compile)
          cs     Typed C# source (.g.cs) + YaalDescriptorRegistry.g.cs for zero-parse startup
        """);
}
