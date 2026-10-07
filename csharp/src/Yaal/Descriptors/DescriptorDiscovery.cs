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
        var dirs = new List<string> { root };
        dirs.AddRange(Directory.EnumerateDirectories(root, "*", SearchOption.AllDirectories));
        foreach (var dir in dirs)
        {
            if (!Directory.EnumerateFiles(dir, "*.sql").Any())
                continue;
            var rel = RelativePath(root, dir).Replace('\\', '/');
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
        foreach (var file in Directory.EnumerateFiles(folder, "$.output.*.json"))
        {
            var name = Path.GetFileName(file);
            if (!name.StartsWith("$.output.", StringComparison.Ordinal))
                continue;
            if (!name.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                continue;
            var mapper = name["$.output.".Length..^".json".Length];
            if (mapper.Length > 0 && seen.Add(mapper))
                mappers.Add(mapper);
        }

        return mappers;
    }

    private static string RelativePath(string root, string dir)
    {
        var prefix = root.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            + Path.DirectorySeparatorChar;
        if (dir.Equals(root, StringComparison.OrdinalIgnoreCase))
            return ".";
        if (dir.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            return dir.Substring(prefix.Length);
        return dir;
    }

    public static string RegistryKey(string path, string? outputMapper) =>
        string.IsNullOrEmpty(outputMapper) ? path : path + "#" + outputMapper;

    public static string PathToClassName(string path, string? outputMapper = null)
    {
        var segments = string.IsNullOrEmpty(path)
            ? new[] { "Root" }
            : path.Split(new[] { '/' }, StringSplitOptions.RemoveEmptyEntries);
        var parts = segments
            .Select(p => char.ToUpperInvariant(p[0]) + p[1..].Replace("_", ""))
            .ToList();
        if (!string.IsNullOrEmpty(outputMapper))
            parts.Add(char.ToUpperInvariant(outputMapper[0]) + outputMapper[1..]);
        return string.Join("", parts);
    }
}
