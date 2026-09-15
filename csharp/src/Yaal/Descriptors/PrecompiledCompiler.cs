// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Descriptors;

public static class PrecompiledCompiler
{
    public static List<string> CompileJson(
        string apiRoot,
        string outDir,
        IEnumerable<string>? listPaths = null,
        bool indented = true)
    {
        var y = new Yaal(apiRoot, debug: true);
        var paths = listPaths?.ToList() ?? DescriptorDiscovery.ListDescriptors(apiRoot);
        var outRoot = Path.GetFullPath(outDir);
        Directory.CreateDirectory(outRoot);
        var written = new List<string>();

        foreach (var path in paths)
        {
            foreach (var mapper in DescriptorDiscovery.DiscoverOutputMappers(apiRoot, path))
            {
                var branch = y.CreateDescriptor(path, mapper);
                var rel = Precompiled.ArtifactFileName(path, mapper).Replace('/', Path.DirectorySeparatorChar);
                var dest = Path.Combine(outRoot, rel);
                Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
                File.WriteAllText(dest, Precompiled.ExportJson(branch, indented) + Environment.NewLine);
                written.Add(rel.Replace('\\', '/'));
            }
        }

        return written;
    }
}
