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
    private readonly Dictionary<string, Dictionary<string, object?>> _cache = new();
    private readonly bool _debug;

    public Yaal(string rootPath, IContentReader? contentReader = null, bool debug = false)
    {
        _rootPath = rootPath;
        _debug = debug;
        _contentReader = contentReader ?? new FileContentReader(_rootPath);
    }

    public string GetRootPath() => _rootPath;

    public void SetupDataProvider(string name, string databaseUri)
    {
        var (providerName, options) = DatabaseUrl.Parse(databaseUri);
        _dataProviders[name] = providerName switch
        {
            "postgresql" => new PostgresContextManager(options),
            "mysql" => new MySqlContextManager(options),
            "clickhouse" => new ClickHouseContextManager(options),
            "sqlite3" => new SqliteContextManager(options),
            _ => throw new UnsupportedDatabaseUrlException(
                $"Unsupported database URL scheme '{providerName}' for provider '{name}'. " +
                "Supported schemes: sqlite3, postgresql, mysql, clickhouse"),
        };
        _dataProviderSchemes[name] = providerName;
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

    public void ClearCache()
    {
        _descriptors.Clear();
        _cache.Clear();
    }

    public object? Query(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        var (descriptor, cacheKey) = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        return GetResult(descriptor, context, cacheKey);
    }

    public string QueryJson(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null)
    {
        var (descriptor, cacheKey) = LoadDescriptor(descriptorPath, outputMapper);
        var context = ContextFactory.CreateContext(descriptor, payload, args);
        return GetResultJson(descriptor, context, cacheKey);
    }

    public List<Dictionary<string, object?>> ExplainSql(
        string descriptorPath,
        object? payload = null,
        object? args = null,
        string? outputMapper = null,
        string? placeholder = null)
    {
        var (descriptor, _) = LoadDescriptor(descriptorPath, outputMapper);
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
                    var compiled = DataProviderHelper.GetExecutableContent(placeholder, twig, shape);
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

    public object? GetResult(Branch descriptor, Shape context, string? cacheKey = null)
    {
        var key = cacheKey ?? descriptor.Path;
        return Executor.GetResult(descriptor, GetDataProvider, context, CacheFor(key));
    }

    public string GetResultJson(Branch descriptor, Shape context, string? cacheKey = null)
    {
        var key = cacheKey ?? descriptor.Path;
        return Executor.GetResultJson(descriptor, GetDataProvider, context, CacheFor(key));
    }

    private (Branch Descriptor, string CacheKey) LoadDescriptor(string descriptorPath, string? outputMapper)
    {
        var cacheKey = CacheKey(descriptorPath, outputMapper);
        if (!_debug && _descriptors.TryGetValue(cacheKey, out var cached))
            return (cached, cacheKey);

        var descriptor = CreateDescriptor(descriptorPath, outputMapper);
        _descriptors[cacheKey] = descriptor;
        return (descriptor, cacheKey);
    }

    private static string CacheKey(string descriptorPath, string? outputMapper) =>
        string.IsNullOrEmpty(outputMapper) ? descriptorPath : descriptorPath + "#" + outputMapper;

    private Dictionary<string, object?> CacheFor(string path)
    {
        if (!_cache.TryGetValue(path, out var c))
        {
            c = new Dictionary<string, object?>();
            _cache[path] = c;
        }
        return c;
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
