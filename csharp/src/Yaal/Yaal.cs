// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Descriptors;
using Yaal.Execution;
using Yaal.Providers;
using Yaal.Sql;

namespace Yaal;

public sealed class Yaal
{
    private readonly string _rootPath;
    private readonly IContentReader _contentReader;
    private readonly Dictionary<string, Branch> _descriptors = new();
    private readonly Dictionary<string, IDataProviderContextManager> _dataProviders = new();
    private readonly Dictionary<string, string> _dataProviderSchemes = new();
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

    public void SetupDataProvider(string name, string databaseUri)
    {
        var (providerName, options) = DatabaseUrl.Parse(databaseUri);
        _dataProviders[name] = providerName switch
        {
            "postgresql" => PostgresProviderFactory.Create(options),
            "mysql" => MySqlProviderFactory.Create(options),
            "clickhouse" => ClickHouseProviderFactory.Create(options),
            "sqlite3" => SqliteProviderFactory.Create(options),
            _ => throw new UnsupportedDatabaseUrlException(
                $"Unsupported database URL scheme '{providerName}' for provider '{name}'. " +
                "Supported schemes: sqlite3, postgresql, mysql, clickhouse"),
        };
        _dataProviderSchemes[name] = providerName;
    }

    /// <summary>Register an app-supplied provider (custom engine, mock, wrapper).</summary>
    public void SetupDataProvider(string name, IDataProviderContextManager manager, string? scheme = null)
    {
        ArgumentNullException.ThrowIfNull(manager);
        _dataProviders[name] = manager;
        _dataProviderSchemes[name] = scheme ?? "";
    }

    public IDataProvider GetDataProvider(string name)
    {
        if (!_dataProviders.TryGetValue(name, out var manager))
        {
            throw new YaalException(
                $"Data provider '{name}' is not configured. Call setup_data_provider('{name}', url) first.");
        }
        return manager.GetContext();
    }

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

    /// <summary>Clear cached descriptors (reload SQL/YAML on next query).</summary>
    public void ClearCache()
    {
        _descriptors.Clear();
    }

    public object? Query(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        return GetResult(descriptor, context);
    }

    public string QueryJson(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        return GetResultJson(descriptor, context);
    }

    public List<Dictionary<string, object?>> ExplainSql(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null,
        string? placeholder = null)
    {
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        placeholder ??= DefaultPlaceholder();

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
                        ["connection"] = twig.Connection,
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

    public object? GetResult(Branch descriptor, Shape context) =>
        Executor.GetResult(descriptor, GetDataProvider, context);

    public string GetResultJson(Branch descriptor, Shape context) =>
        Executor.GetResultJson(descriptor, GetDataProvider, context);

    private Branch LoadDescriptor(string descriptorPath, string? outputMapper)
    {
        var cacheKey = DescriptorKey(descriptorPath, outputMapper);
        if (!_debug && _descriptors.TryGetValue(cacheKey, out var cached))
            return cached;

        // debug=true forces live SQL/YAML; otherwise prefer precompiled artifacts.
        Branch descriptor;
        if (!string.IsNullOrEmpty(_precompiled) && !_debug)
            descriptor = Precompiled.LoadFromDirectory(_precompiled, descriptorPath, outputMapper);
        else
            descriptor = CreateDescriptor(descriptorPath, outputMapper);

        _descriptors[cacheKey] = descriptor;
        return descriptor;
    }

    private static string DescriptorKey(string descriptorPath, string? outputMapper) =>
        string.IsNullOrEmpty(outputMapper) ? descriptorPath : descriptorPath + "#" + outputMapper;

    private string DefaultPlaceholder()
    {
        foreach (var scheme in _dataProviderSchemes.Values)
        {
            if (scheme is "postgresql" or "mysql" or "clickhouse")
                return "%s";
            if (scheme == "sqlite3")
                return "?";
        }
        return "?";
    }
}
