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
    private readonly Dictionary<string, Branch> _registered = new(StringComparer.OrdinalIgnoreCase);
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
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        return GetResult(descriptor, context);
    }

    /// <summary>
    /// Run a query and materialize the shaped result to <typeparamref name="T"/>.
    /// Object descriptors return a single POCO; array descriptors require <c>T</c> to be
    /// <c>List&lt;TElement&gt;</c> or use <see cref="QueryList{TElement}"/>.
    /// </summary>
    public T Query<T>(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null) where T : notnull
    {
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        var raw = GetResult(descriptor, context);
        ObjectMaterializer.ThrowIfErrors(raw);
        return MaterializeFromDescriptor<T>(descriptor, raw);
    }

    /// <summary>Run an array-output query and materialize rows to <see cref="List{T}"/>.</summary>
    public List<T> QueryList<T>(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        EnsureArrayOutput(descriptor, descriptorPath);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        var raw = GetResult(descriptor, context);
        ObjectMaterializer.ThrowIfErrors(raw);
        return ObjectMaterializer.MapList<T>(raw);
    }

    /// <summary>Run an object-output query and map the result into an existing instance.</summary>
    public T QueryInto<T>(
        string descriptorPath,
        T into,
        object? payload = null,
        object? args = null,
        string? outputMapper = null) where T : class
    {
        ArgumentNullException.ThrowIfNull(into);
        var descriptor = LoadDescriptor(descriptorPath, outputMapper);
        EnsureObjectOutput(descriptor, descriptorPath);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        var raw = GetResult(descriptor, context);
        ObjectMaterializer.ThrowIfErrors(raw);
        ObjectMaterializer.MapInto(raw, into);
        return into;
    }

    /// <summary>Materialize an already-shaped <see cref="Query"/> result.</summary>
    public static T Materialize<T>(object? shapedResult) where T : notnull
    {
        ObjectMaterializer.ThrowIfErrors(shapedResult);
        return ObjectMaterializer.Map<T>(shapedResult!);
    }

    /// <summary>Map an already-shaped result into an existing instance.</summary>
    public static void MaterializeInto(object? shapedResult, object into)
    {
        ObjectMaterializer.ThrowIfErrors(shapedResult);
        ObjectMaterializer.MapInto(shapedResult, into);
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

    private static T MaterializeFromDescriptor<T>(Branch descriptor, object? raw) where T : notnull
    {
        if (string.Equals(descriptor.OutputType, YaalConst.Array, StringComparison.OrdinalIgnoreCase))
        {
            if (TryGetListElementType(typeof(T), out var elementType))
            {
                var list = typeof(ObjectMaterializer)
                    .GetMethod(nameof(ObjectMaterializer.MapList), new[] { typeof(object) })!
                    .MakeGenericMethod(elementType)
                    .Invoke(null, new[] { raw });
                return (T)list!;
            }

            throw new InvalidOperationException(
                $"Descriptor '{descriptor.Path}' returns an array; use QueryList<{typeof(T).Name}>(...) " +
                $"or Query<List<{typeof(T).Name}>>(...).");
        }

        return ObjectMaterializer.Map<T>(raw!);
    }

    private static bool TryGetListElementType(Type type, out Type elementType)
    {
        if (type.IsArray)
        {
            elementType = type.GetElementType()!;
            return true;
        }

        if (type.IsGenericType)
        {
            var def = type.GetGenericTypeDefinition();
            if (def == typeof(List<>) || def == typeof(IList<>) || def == typeof(IReadOnlyList<>))
            {
                elementType = type.GetGenericArguments()[0];
                return true;
            }
        }

        elementType = typeof(object);
        return false;
    }

    private static void EnsureObjectOutput(Branch descriptor, string descriptorPath)
    {
        if (!string.Equals(descriptor.OutputType, YaalConst.Object, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Descriptor '{descriptorPath}' returns an array; QueryInto supports object output only.");
        }
    }

    private static void EnsureArrayOutput(Branch descriptor, string descriptorPath)
    {
        if (!string.Equals(descriptor.OutputType, YaalConst.Array, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Descriptor '{descriptorPath}' returns an object; use Query<{descriptor.OutputType}>(...) instead.");
        }
    }

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
