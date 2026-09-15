// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Descriptors;

public static class DescriptorDiscovery
{
    public static List<string> ListDescriptors(string apiRoot)
    {
        var root = Path.GetFullPath(apiRoot);
        if (!Directory.Exists(root))
            return new List<string>();

        var found = new List<string>();
        foreach (var dir in Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories)
                     .Prepend(root))
        {
            if (!Directory.EnumerateFiles(dir, "*.sql").Any())
                continue;
            var rel = Path.GetRelativePath(root, dir).Replace('\\', '/');
            if (rel == ".")
                rel = "";
            found.Add(rel);
        }

        return found.OrderBy(x => x, StringComparer.Ordinal).ToList();
    }

    public static List<string?> DiscoverOutputMappers(string apiRoot, string path)
    {
        var mappers = new List<string?> { null };
        var folder = Path.Combine(Path.GetFullPath(apiRoot), path);
        if (!Directory.Exists(folder))
            return mappers;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var pattern in new[] { "$.output.*.yaml", "$.output.*.json" })
        {
            foreach (var file in Directory.EnumerateFiles(folder, pattern))
            {
                var name = Path.GetFileName(file);
                if (!name.StartsWith("$.output.", StringComparison.Ordinal))
                    continue;
                var ext = name.EndsWith(".yaml", StringComparison.OrdinalIgnoreCase) ? ".yaml" : ".json";
                if (!name.EndsWith(ext, StringComparison.OrdinalIgnoreCase))
                    continue;
                var mapper = name["$.output.".Length..^ext.Length];
                if (mapper.Length > 0 && seen.Add(mapper))
                    mappers.Add(mapper);
            }
        }

        return mappers;
    }

    public static string RegistryKey(string path, string? outputMapper) =>
        string.IsNullOrEmpty(outputMapper) ? path : path + "#" + outputMapper;

    public static string PathToClassName(string path, string? outputMapper = null)
    {
        var segments = string.IsNullOrEmpty(path)
            ? new[] { "Root" }
            : path.Split('/', StringSplitOptions.RemoveEmptyEntries);
        var parts = segments
            .Select(p => char.ToUpperInvariant(p[0]) + p[1..].Replace("_", "", StringComparison.Ordinal))
            .ToList();
        if (!string.IsNullOrEmpty(outputMapper))
            parts.Add(char.ToUpperInvariant(outputMapper[0]) + outputMapper[1..]);
        return string.Join("", parts);
    }
}
