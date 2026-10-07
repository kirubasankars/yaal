// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Descriptors;
using Yaal.Execution;
using Yaal.Providers;

namespace Yaal;

public sealed class Yaal
{
    private readonly string _rootPath;
    private readonly IContentReader _contentReader;
    private readonly Dictionary<string, Branch> _descriptors = new();
    private readonly Dictionary<string, Branch> _registered = new(StringComparer.OrdinalIgnoreCase);
    private readonly bool _debug;
    private readonly string? _precompiled;

    public Yaal(
        string rootPath,
        IContentReader? contentReader = null,
        bool debug = false,
        string? precompiled = null)
    {
        _rootPath = rootPath;
        _debug = debug;
        _precompiled = precompiled;
        _contentReader = contentReader ?? new FileContentReader(_rootPath);
    }

    public string GetRootPath() => _rootPath;

    public Branch CreateDescriptor(string path, string? outputMapper = null)
    {
        var descriptor = TrunkBuilder.CreateTrunk(path, outputMapper, _contentReader);
        if (descriptor == null)
        {
            var root = _contentReader.RootPath;
            throw new DescriptorNotFoundException(
                "No SQL descriptor files (*.sql) found at " + Path.Combine(root, path));
        }
        return descriptor;
    }

    /// <summary>Clear cached descriptors (reload SQL/JSON on next query).</summary>
    public void ClearCache()
    {
        _descriptors.Clear();
    }

    /// <summary>Register an in-memory descriptor (overwrites an existing registration for the same key).</summary>
    public void RegisterDescriptor(string descriptorPath, Branch branch, string? outputMapper = null)
    {
        ArgumentNullException.ThrowIfNull(branch);
        var key = DescriptorKey(descriptorPath, outputMapper);
        _registered[key] = branch;
        _descriptors.Remove(key);
    }

    /// <summary>Remove a registered in-memory descriptor.</summary>
    public void UnregisterDescriptor(string descriptorPath, string? outputMapper = null)
    {
        var key = DescriptorKey(descriptorPath, outputMapper);
        _registered.Remove(key);
        _descriptors.Remove(key);
    }

    public object? Query(
        IDataProvider provider,
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        return GetResult(provider, descriptor, context);
    }

    public string QueryJson(
        IDataProvider provider,
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        return GetResultJson(provider, descriptor, context);
    }

    public List<Dictionary<string, object?>> ExplainSql(
        IDataProvider provider,
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null,
        string? placeholder = null)
    {
        ArgumentNullException.ThrowIfNull(provider);
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        placeholder ??= string.IsNullOrEmpty(provider.Placeholder) ? "?" : provider.Placeholder;

        var helper = new DataProviderHelper();
        var explained = new List<Dictionary<string, object?>>();

        void Walk(Branch branch, Shape shape)
        {
            if (branch.Twigs != null)
            {
                foreach (var twig in branch.Twigs)
                {
                    var compiled = helper.GetExecutableContent(placeholder, twig, shape);
                    explained.Add(new Dictionary<string, object?>
                    {
                        ["method"] = branch.Method,
                        ["sql"] = compiled.Content,
                        ["parameters"] = helper.BuildParameters(compiled, shape, (_, v) => v),
                    });
                }
            }

            if (branch.Branches == null)
                return;

            foreach (var child in branch.Branches)
            {
                var childShape = shape;
                var childName = (child.Name ?? "").ToLowerInvariant();
                if (!string.IsNullOrEmpty(childName))
                {
                    var nested = shape.GetProp(childName);
                    if (nested is Shape nestedShape)
                        childShape = nestedShape;
                }
                Walk(child, childShape);
            }
        }

        Walk(descriptor, context);
        return explained;
    }

    public object? GetResult(IDataProvider provider, Branch descriptor, Shape context) =>
        Executor.GetResult(descriptor, provider, context);

    public string GetResultJson(IDataProvider provider, Branch descriptor, Shape context) =>
        Executor.GetResultJson(descriptor, provider, context);

    private Branch LoadDescriptor(string descriptorPath, string? outputMapper)
    {
        var cacheKey = DescriptorKey(descriptorPath, outputMapper);
        if (!_debug && _descriptors.TryGetValue(cacheKey, out var cached))
            return cached;

        Branch descriptor;
        if (!_debug && _registered.TryGetValue(cacheKey, out var registered))
            descriptor = registered;
        else if (!string.IsNullOrEmpty(_precompiled) && !_debug)
            descriptor = Precompiled.LoadFromDirectory(_precompiled, descriptorPath, outputMapper);
        else
            descriptor = CreateDescriptor(descriptorPath, outputMapper);

        _descriptors[cacheKey] = descriptor;
        return descriptor;
    }

    private static string DescriptorKey(string descriptorPath, string? outputMapper) =>
        string.IsNullOrEmpty(outputMapper) ? descriptorPath : descriptorPath + "#" + outputMapper;
}
