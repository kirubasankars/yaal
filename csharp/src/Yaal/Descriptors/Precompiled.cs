// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json;
using Json.Schema;
using Yaal.Sql;

namespace Yaal.Descriptors;

/// <summary>
/// Load Python-produced precompiled descriptor JSON (snake_case, token twigs preserved).
/// </summary>
public static class Precompiled
{
    public const int Version = 1;

    public static string ArtifactFileName(string path, string? outputMapper = null) =>
        string.IsNullOrEmpty(outputMapper) ? path + ".json" : path + "#" + outputMapper + ".json";

    public static Branch LoadFile(string filePath)
    {
        var json = File.ReadAllText(filePath);
        using var doc = JsonDocument.Parse(json);
        return Import(doc.RootElement);
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

    public static Branch Import(JsonElement root)
    {
        var branch = ReadBranch(root);
        AttachValidators(branch);
        return branch;
    }

    private static void AttachValidators(Branch trunk)
    {
        var model = trunk.Model;
        trunk.Validators = new Dictionary<string, JsonSchema?>
        {
            ["args"] = CreateValidator(model?.Args),
            ["payload"] = CreateValidator(model?.Payload),
        };
    }

    private static JsonSchema? CreateValidator(Dictionary<string, object?>? schema)
    {
        if (schema == null || schema.Count == 0)
            return null;
        var copy = (Dictionary<string, object?>)JsonUtil.DeepCopy(schema)!;
        copy.Remove("$schema");
        var node = JsonUtil.ToJsonNode(copy);
        return JsonSchema.FromText(node!.ToJsonString());
    }

    private static Branch ReadBranch(JsonElement el)
    {
        var branch = new Branch
        {
            Name = Str(el, "name") ?? "",
            Method = Str(el, "method") ?? "",
            Path = Str(el, "path") ?? "",
            InputType = Str(el, "input_type") ?? YaalConst.Object,
            OutputType = Str(el, "output_type") ?? YaalConst.Array,
            PartitionBy = Str(el, "partition_by"),
            UseParentRows = Bool(el, "use_parent_rows"),
        };

        if (el.TryGetProperty("model", out var modelEl) && modelEl.ValueKind == JsonValueKind.Object)
        {
            branch.Model = new DescriptorModel
            {
                Args = DictOrNull(modelEl, "args"),
                Payload = DictOrNull(modelEl, "payload"),
                Output = DictOrNull(modelEl, "output"),
            };
        }

        if (el.TryGetProperty("parameters", out var paramsEl) && paramsEl.ValueKind == JsonValueKind.Object)
        {
            branch.Parameters = new Dictionary<string, ParamDecl>(StringComparer.OrdinalIgnoreCase);
            foreach (var prop in paramsEl.EnumerateObject())
            {
                branch.Parameters[prop.Name] = ReadParamDecl(prop.Value, prop.Name);
            }
        }

        if (el.TryGetProperty("twigs", out var twigsEl) && twigsEl.ValueKind == JsonValueKind.Array)
        {
            branch.Twigs = new List<Twig>();
            foreach (var twigEl in twigsEl.EnumerateArray())
                branch.Twigs.Add(ReadTwig(twigEl));
        }

        if (el.TryGetProperty("branches", out var branchesEl) && branchesEl.ValueKind == JsonValueKind.Array)
        {
            branch.Branches = new List<Branch>();
            foreach (var child in branchesEl.EnumerateArray())
                branch.Branches.Add(ReadBranch(child));
        }

        if (el.TryGetProperty("connections", out var connEl) && connEl.ValueKind == JsonValueKind.Array)
        {
            branch.Connections = connEl.EnumerateArray()
                .Select(x => x.GetString() ?? "db")
                .ToList();
        }

        return branch;
    }

    private static Twig ReadTwig(JsonElement el)
    {
        var twig = new Twig
        {
            Connection = Str(el, "connection") ?? "db",
        };

        if (el.TryGetProperty("content", out var contentEl) && contentEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var tokEl in contentEl.EnumerateArray())
                twig.Content.Add(ReadToken(tokEl));
        }

        if (el.TryGetProperty("parameters", out var paramsEl) && paramsEl.ValueKind == JsonValueKind.Array)
        {
            foreach (var p in paramsEl.EnumerateArray())
                twig.Parameters.Add(ReadParamDecl(p));
        }

        if (el.TryGetProperty("nullable", out var nullEl) && nullEl.ValueKind == JsonValueKind.Array)
        {
            twig.Nullable = nullEl.EnumerateArray()
                .Select(x => x.GetString() ?? "")
                .Where(x => x.Length > 0)
                .ToList();
        }

        return twig;
    }

    private static SqlToken ReadToken(JsonElement el)
    {
        var token = new SqlToken
        {
            Type = Str(el, "type") ?? "",
            Value = Str(el, "value") ?? "",
            Name = Str(el, "name"),
            Nullable = Bool(el, "nullable"),
            NullableParameter = Str(el, "nullable_parameter"),
        };

        if (el.TryGetProperty("group", out var groupEl) && groupEl.ValueKind == JsonValueKind.Number)
            token.Group = groupEl.GetInt32();

        return token;
    }

    private static ParamDecl ReadParamDecl(JsonElement el, string? fallbackName = null)
    {
        return new ParamDecl
        {
            Name = Str(el, "name") ?? fallbackName ?? "",
            Type = Str(el, "type") ?? "",
        };
    }

    private static Dictionary<string, object?>? DictOrNull(JsonElement parent, string name)
    {
        if (!parent.TryGetProperty(name, out var el) || el.ValueKind != JsonValueKind.Object)
            return null;
        return JsonUtil.ToDict(el);
    }

    private static string? Str(JsonElement el, string name)
    {
        if (!el.TryGetProperty(name, out var v) || v.ValueKind == JsonValueKind.Null)
            return null;
        return v.ValueKind == JsonValueKind.String ? v.GetString() : v.ToString();
    }

    private static bool Bool(JsonElement el, string name)
    {
        if (!el.TryGetProperty(name, out var v))
            return false;
        return v.ValueKind == JsonValueKind.True ||
               (v.ValueKind == JsonValueKind.String &&
                bool.TryParse(v.GetString(), out var b) && b);
    }
}
