using System.Text.Json;

namespace Yaal.Descriptors;

public interface IContentReader
{
    string? GetSql(string method, string path);
    Dictionary<string, object?> GetConfig(string path, string? outputMapper);
    List<string>? ListSql(string path);
    string RootPath { get; }
}

public sealed class FileContentReader : IContentReader
{
    private readonly string _rootPath;

    public FileContentReader(string rootPath)
    {
        _rootPath = Path.GetFullPath(rootPath);
    }

    public string RootPath => _rootPath;

    public string? GetSql(string method, string path)
    {
        var filePath = Resolve(path, method + ".sql");
        return Get(filePath);
    }

    public Dictionary<string, object?> GetConfig(string path, string? outputMapper)
    {
        var inputPath = Resolve(path, "$.input");
        var inputConfig = GetConfigFile(inputPath);

        var outputSuffix = string.IsNullOrEmpty(outputMapper) ? "" : "." + outputMapper;
        var outputPath = Resolve(path, "$.output" + outputSuffix);
        var outputConfig = GetConfigFile(outputPath);

        return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["input.model"] = inputConfig,
            ["output.model"] = outputConfig,
        };
    }

    public List<string>? ListSql(string path)
    {
        try
        {
            var dir = Resolve(path);
            return Directory.GetFiles(dir)
                .Select(Path.GetFileName)
                .Where(f => f != null && f.EndsWith(".sql", StringComparison.OrdinalIgnoreCase))
                .Select(f => f![..^4])
                .ToList()!;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }

    private string Resolve(params string[] parts)
    {
        var joined = Path.Combine(new[] { _rootPath }.Concat(parts).ToArray());
        var candidate = Path.GetFullPath(joined);
        var root = _rootPath.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        if (candidate.Equals(root, StringComparison.OrdinalIgnoreCase) ||
            candidate.StartsWith(root + Path.DirectorySeparatorChar, StringComparison.OrdinalIgnoreCase) ||
            candidate.StartsWith(root + Path.AltDirectorySeparatorChar, StringComparison.OrdinalIgnoreCase))
        {
            return candidate;
        }

        throw new PathEscapeException(
            $"descriptor path resolves outside API root '{_rootPath}'");
    }

    private static Dictionary<string, object?>? GetConfigFile(string filePath)
    {
        var yamlPath = filePath + ".yaml";
        if (File.Exists(yamlPath))
        {
            var configStr = Get(yamlPath);
            if (!string.IsNullOrEmpty(configStr))
                return JsonUtil.YamlToDict(configStr);
        }

        var jsonPath = filePath + ".json";
        if (File.Exists(jsonPath))
        {
            var configStr = Get(jsonPath);
            if (!string.IsNullOrEmpty(configStr))
            {
                var node = JsonDocument.Parse(configStr);
                return (Dictionary<string, object?>?)JsonUtil.FromJsonElement(node.RootElement);
            }
        }

        return null;
    }

    private static string? Get(string filePath)
    {
        try
        {
            return File.ReadAllText(filePath);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
    }
}
