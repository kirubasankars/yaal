using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Json.Schema;
using Yaal.Sql;

namespace Yaal.Descriptors;

public static class TrunkBuilder
{
    private static readonly Regex ArrayRx = new(@"^(?<path>\w+)\[\d+\]$", RegexOptions.Compiled);

    public static Branch? CreateTrunk(string path, string? outputMapper, IContentReader contentReader)
    {
        var orderedFiles = OrderListByDots(contentReader.ListSql(path));
        if (orderedFiles.Count == 0)
            return null;

        var trunkMap = BuildTrunkMapByFiles(orderedFiles);
        var config = contentReader.GetConfig(path, outputMapper);

        Dictionary<string, object?>? payloadSchema = null;
        Dictionary<string, object?>? argsSchema = null;
        Dictionary<string, object?>? outputSchema = null;

        if (config.TryGetValue("input.model", out var inputModelObj) &&
            inputModelObj is Dictionary<string, object?> inputModel)
        {
            if (inputModel.TryGetValue("payload", out var payload))
                payloadSchema = (Dictionary<string, object?>?)JsonUtil.ToLowerKeysDeep(payload);
            if (inputModel.TryGetValue("args", out var args))
                argsSchema = (Dictionary<string, object?>?)JsonUtil.ToLowerKeysDeep(args);
        }

        if (config.TryGetValue("output.model", out var outputModelObj) && outputModelObj != null)
            outputSchema = (Dictionary<string, object?>?)JsonUtil.ToLowerKeysDeep(outputModelObj);

        argsSchema ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["type"] = "object",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
        };
        payloadSchema ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["type"] = "object",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
        };
        outputSchema ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["type"] = "array",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
        };

        var trunk = new Branch
        {
            Name = "$",
            Method = "$",
            Path = path,
            Model = new DescriptorModel
            {
                Args = argsSchema,
                Payload = payloadSchema,
                Output = outputSchema,
            },
        };

        var payloadValidator = CreateValidator(payloadSchema);
        var argsValidator = CreateValidator(argsSchema);

        var bag = new Dictionary<string, object?>
        {
            ["connections"] = new List<string> { "db" },
        };

        if (!trunkMap.TryGetValue("$", out var dollarMapObj) || dollarMapObj == null)
            dollarMapObj = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        var mapByFiles = ConvertMap(dollarMapObj);

        BuildBranch(trunk, mapByFiles, contentReader, payloadSchema, outputSchema, trunk.Model, bag);
        trunk.Connections = (List<string>)bag["connections"]!;
        trunk.Validators = new Dictionary<string, JsonSchema?>
        {
            ["args"] = argsValidator,
            ["payload"] = payloadValidator,
        };

        return trunk;
    }

    private static JsonSchema? CreateValidator(Dictionary<string, object?>? schema)
    {
        if (schema == null)
            return null;
        // Json.Schema defaults to 2020-12. Python uses Draft4Validator. Keep input models to the
        // common subset (type/properties/required) so both dialects accept the same fixtures.
        var copy = (Dictionary<string, object?>)JsonUtil.DeepCopy(schema)!;
        copy.Remove("$schema");
        var node = JsonUtil.ToJsonNode(copy);
        return JsonSchema.FromText(node!.ToJsonString());
    }

    private static List<string> OrderListByDots(List<string>? names)
    {
        if (names == null || names.Count == 0)
            return new List<string>();

        var working = names.ToList();
        var dots = working.Select(x => x.Count(c => c == '.')).ToList();
        var ordered = new List<string>();

        while (dots.Count > 0)
        {
            var el = dots.Min();
            while (true)
            {
                var idx = dots.IndexOf(el);
                if (idx < 0)
                    break;
                ordered.Add(working[idx].ToLowerInvariant());
                working.RemoveAt(idx);
                dots.RemoveAt(idx);
            }
        }

        return ordered;
    }

    private static void BuildBranchMapByFiles(Dictionary<string, object?> branchMap, string item)
    {
        if (item == "")
            return;
        var dot = item.IndexOf('.');
        if (dot > -1)
        {
            var path = item[..dot];
            var remainingPath = item[(dot + 1)..];
            if (!branchMap.TryGetValue(path, out var nested) || nested is not Dictionary<string, object?> nestedMap)
            {
                nestedMap = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                branchMap[path] = nestedMap;
            }
            BuildBranchMapByFiles(nestedMap, remainingPath);
        }
        else if (!branchMap.ContainsKey(item))
        {
            branchMap[item] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        }
    }

    private static Dictionary<string, object?> BuildTrunkMapByFiles(List<string> nameList)
    {
        var trunkMap = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var item in nameList)
            BuildBranchMapByFiles(trunkMap, item);
        return trunkMap;
    }

    private static Dictionary<string, object?> ConvertMap(object map) =>
        map as Dictionary<string, object?> ?? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

    private static void BuildBranch(
        Branch branch,
        Dictionary<string, object?> mapByFiles,
        IContentReader contentReader,
        Dictionary<string, object?> payloadModel,
        Dictionary<string, object?>? outputModel,
        DescriptorModel model,
        Dictionary<string, object?> bag)
    {
        var path = branch.Path;
        var method = branch.Method;
        var content = contentReader.GetSql(method, path);
        var branchMap = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        if (!payloadModel.ContainsKey("properties") || payloadModel["properties"] == null)
            payloadModel["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        branch.InputType = payloadModel.TryGetValue("type", out var it) ? it?.ToString() ?? "object" : "object";
        var inputProperties = (Dictionary<string, object?>)payloadModel["properties"]!;

        Dictionary<string, object?>? outputProperties = null;
        if (outputModel != null)
        {
            branch.OutputType = outputModel.TryGetValue("type", out var ot) ? ot?.ToString() ?? "array" : "array";

            if (outputModel.TryGetValue("properties", out var op) && op is Dictionary<string, object?> opDict)
                outputProperties = opDict;

            if (outputModel.TryGetValue("parent_rows", out var pr) && pr is bool prBool)
                branch.UseParentRows = prBool;

            if (outputModel.TryGetValue("cache", out var cache) && cache is bool cacheBool)
            {
                if (branch.UseParentRows)
                    throw new InvalidOperationException("cache and use_parent_rows can't be true at a same time");
                branch.Cache = cacheBool;
            }

            if (outputModel.TryGetValue("partition_by", out var pb))
                branch.PartitionBy = pb?.ToString();

            if (outputProperties != null)
            {
                foreach (var (k, v) in outputProperties)
                {
                    if (v is Dictionary<string, object?> vDict &&
                        vDict.TryGetValue("type", out var typeObj))
                    {
                        var type = typeObj?.ToString();
                        if (type is "object" or "array")
                            branchMap[k] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    }
                }
            }
        }
        else
        {
            branch.OutputType = "array";
            branch.UseParentRows = false;
        }

        if (!string.IsNullOrEmpty(content))
        {
            var ast = SqlParser.Parse(Lexer.Lex(content), method);
            if (ast?.SqlStmts == null)
                return;

            branch.Parameters = ast.Parameters ?? new Dictionary<string, ParamDecl>();

            foreach (var (k, v) in branch.Parameters)
            {
                var paramDict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["name"] = v.Name,
                    ["type"] = v.Type,
                };
                if (k.StartsWith('$') && !k.Contains("$parent"))
                    ExpandParameter(AsModelDict(model), k, paramDict);
                else
                    ExpandParameter(payloadModel, k, paramDict);
            }

            branch.Twigs = ast.SqlStmts;

            var connections = (List<string>)bag["connections"]!;
            foreach (var twig in branch.Twigs)
            {
                if (string.IsNullOrEmpty(twig.Connection))
                    twig.Connection = "db";
                if (!connections.Contains(twig.Connection))
                    connections.Add(twig.Connection);
            }
        }

        var lowerBranchMap = branchMap.Keys.Select(k => k.ToLowerInvariant()).ToHashSet();
        foreach (var (k, v) in mapByFiles)
        {
            if (!lowerBranchMap.Contains(k.ToLowerInvariant()))
                branchMap[k] = v;
        }

        var branches = new List<Branch>();
        foreach (var (subBranchName, subBranchMapObj) in branchMap)
        {
            var subBranchMap = ConvertMap(subBranchMapObj!);
            var subBranchMethod = string.Join(".", method, subBranchName).ToLowerInvariant();
            var subBranch = new Branch
            {
                Name = subBranchName,
                Method = subBranchMethod,
                Path = path,
            };

            if (!inputProperties.ContainsKey(subBranchName))
            {
                inputProperties[subBranchName] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                {
                    ["type"] = "object",
                    ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
                };
            }

            var subBranchPayloadModel = (Dictionary<string, object?>)inputProperties[subBranchName]!;
            Dictionary<string, object?>? subBranchOutputModel = null;
            if (outputProperties != null && outputProperties.TryGetValue(subBranchName, out var som) &&
                som is Dictionary<string, object?> somDict)
            {
                subBranchOutputModel = somDict;
            }

            subBranchPayloadModel["$parent"] = payloadModel;
            BuildBranch(subBranch, subBranchMap, contentReader, subBranchPayloadModel, subBranchOutputModel, model, bag);
            subBranchPayloadModel.Remove("$parent");

            if (subBranch.UseParentRows && string.IsNullOrEmpty(branch.PartitionBy))
                throw new InvalidOperationException("parent's _partition_by is can't be empty when child wanted to use parent rows");

            branches.Add(subBranch);
        }

        if (branches.Count > 0)
            branch.Branches = branches;
    }

    private static Dictionary<string, object?> AsModelDict(DescriptorModel model)
    {
        return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["args"] = model.Args,
            ["payload"] = model.Payload,
            ["output"] = model.Output,
        };
    }

    private static void ExpandParameter(Dictionary<string, object?>? model, string prop, Dictionary<string, object?> value)
    {
        if (model == null)
            return;

        var dot = prop.IndexOf('.');
        if (dot > -1)
        {
            var path = prop[..dot];
            if (path == "$parent")
            {
                if (model.TryGetValue("$parent", out var parent) && parent is Dictionary<string, object?> parentDict)
                    model = parentDict;
                else
                    model = null;
            }
            else if (path == "$args")
            {
                model = model["args"] as Dictionary<string, object?>;
            }
            else if (path == "$params")
            {
                return;
            }
            else
            {
                if (!model.ContainsKey("properties") || model["properties"] == null)
                    model["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

                var properties = (Dictionary<string, object?>)model["properties"]!;
                var m = ArrayRx.Match(path);
                if (m.Success)
                {
                    path = m.Groups["path"].Value;
                    if (!properties.ContainsKey(path))
                    {
                        properties[path] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                        {
                            ["type"] = "array",
                            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
                        };
                    }
                }
                else if (!properties.ContainsKey(path))
                {
                    properties[path] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
                    {
                        ["type"] = "object",
                        ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
                    };
                }

                model = (Dictionary<string, object?>)properties[path]!;

                if (model.TryGetValue("required", out var requiredObj) && requiredObj is IList<object?> required)
                {
                    var props = (Dictionary<string, object?>)model["properties"]!;
                    foreach (var f in required)
                    {
                        if (f is string fs && props.TryGetValue(fs, out var fp) && fp is Dictionary<string, object?> fpDict)
                            fpDict["required"] = true;
                    }
                }
            }

            ExpandParameter(model, prop[(dot + 1)..], value);
        }
        else if (model.TryGetValue("properties", out var propsObj) &&
                 propsObj is Dictionary<string, object?> props &&
                 !props.ContainsKey(prop))
        {
            props[prop] = value;
        }
    }
}
