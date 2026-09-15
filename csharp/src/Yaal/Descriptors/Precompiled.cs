// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json;
using System.Text.Json.Serialization;
using Yaal.Sql;

namespace Yaal.Descriptors;

/// <summary>
/// Load Python-produced precompiled descriptor JSON (snake_case, token twigs preserved).
/// </summary>
public static class Precompiled
{
    public const int Version = 1;

    internal static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        PropertyNameCaseInsensitive = true,
        ReadCommentHandling = JsonCommentHandling.Skip,
        AllowTrailingCommas = true,
    };

    public static string ArtifactFileName(string path, string? outputMapper = null) =>
        string.IsNullOrEmpty(outputMapper) ? path + ".json" : path + "#" + outputMapper + ".json";

    public static Branch LoadFile(string filePath)
    {
        var json = File.ReadAllText(filePath);
        return Import(json);
    }

    public static Branch LoadFromDirectory(string precompiledDir, string descriptorPath, string? outputMapper = null)
    {
        var filePath = Path.Combine(precompiledDir, ArtifactFileName(descriptorPath, outputMapper));
        if (!File.Exists(filePath))
        {
            throw new DescriptorNotFoundException(
                "No precompiled descriptor at " + filePath);
        }
        return LoadFile(filePath);
    }

    public static Branch Import(JsonElement root) => Import(root.GetRawText());

    public static Branch Import(string json)
    {
        var branch = JsonSerializer.Deserialize<Branch>(json, JsonOptions)
            ?? throw new InvalidOperationException("Empty precompiled descriptor");
        return Normalize(branch);
    }

    public static string ExportJson(Branch branch, bool indented = true)
    {
        var options = new JsonSerializerOptions(JsonOptions)
        {
            WriteIndented = indented,
        };
        return JsonSerializer.Serialize(branch, options);
    }

    internal static Branch Normalize(Branch branch)
    {
        if (branch.Parameters != null)
        {
            branch.Parameters = branch.Parameters.ToDictionary(
                kv => kv.Key,
                kv => NormalizeParamDecl(kv.Value),
                StringComparer.OrdinalIgnoreCase);
        }

        if (branch.Model != null)
        {
            branch.Model = new DescriptorModel
            {
                Args = NormalizeModelDict(branch.Model.Args),
                Payload = NormalizeModelDict(branch.Model.Payload),
                Output = NormalizeModelDict(branch.Model.Output),
            };
        }

        if (branch.Twigs != null)
        {
            foreach (var twig in branch.Twigs)
                NormalizeTwig(twig);
        }

        if (branch.Branches != null)
        {
            foreach (var child in branch.Branches)
                Normalize(child);
        }

        return branch;
    }

    private static void NormalizeTwig(Twig twig)
    {
        twig.Parameters = twig.Parameters.Select(NormalizeParamDecl).ToList();
        foreach (var token in twig.Content)
            token.Content = null;
    }

    private static ParamDecl NormalizeParamDecl(ParamDecl decl)
    {
        if (decl.Default is JsonElement je)
            decl.Default = JsonUtil.FromJsonElement(je);
        if (decl.Default != null && !decl.HasDefault)
            decl.HasDefault = true;
        return decl;
    }

    private static Dictionary<string, object?>? NormalizeModelDict(Dictionary<string, object?>? dict)
    {
        if (dict == null)
            return null;

        var plain = dict.ToDictionary(
            kv => kv.Key,
            kv => kv.Value is JsonElement je ? JsonUtil.FromJsonElement(je) : kv.Value,
            StringComparer.OrdinalIgnoreCase);

        return JsonUtil.ToLowerKeysDeep(plain) as Dictionary<string, object?>;
    }
}
