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
        _rootPath = rootPath;
    }

    public string RootPath => _rootPath;

    public string? GetSql(string method, string path)
    {
        var filePath = Path.Combine(_rootPath, path, method + ".sql");
        return Get(filePath);
    }

    public Dictionary<string, object?> GetConfig(string path, string? outputMapper)
    {
        var inputPath = Path.Combine(_rootPath, path, "$.input");
        var inputConfig = GetConfigFile(inputPath);

        var outputSuffix = string.IsNullOrEmpty(outputMapper) ? "" : "." + outputMapper;
        var outputPath = Path.Combine(_rootPath, path, "$.output" + outputSuffix);
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
            var dir = Path.Combine(_rootPath, path);
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
