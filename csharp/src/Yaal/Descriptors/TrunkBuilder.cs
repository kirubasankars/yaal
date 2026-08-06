// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.RegularExpressions;
using Json.Schema;
using Yaal.Sql;

namespace Yaal.Descriptors;

public static class TrunkBuilder
{
    private static readonly Regex ArrayRx = new(@"^(?<path>\w+)\[\d+\]$", RegexOptions.Compiled);

    private static readonly Dictionary<string, string> SqlToJsonType = new(StringComparer.OrdinalIgnoreCase)
    {
        ["integer"] = "integer",
        ["string"] = "string",
        ["float"] = "number",
        ["bool"] = "boolean",
        ["blob"] = "string",
    };

    public static Branch? CreateTrunk(string path, string? outputMapper, IContentReader contentReader)
    {
        var orderedFiles = OrderListByDots(contentReader.ListSql(path));
        if (orderedFiles.Count == 0)
            return null;

        var trunkMap = BuildTrunkMapByFiles(orderedFiles);
        var config = contentReader.GetConfig(path, outputMapper);

        Dictionary<string, object?>? outputSchema = null;

        if (config.TryGetValue("output.model", out var outputModelObj) && outputModelObj != null)
            outputSchema = (Dictionary<string, object?>?)JsonUtil.ToLowerKeysDeep(outputModelObj);

        var argsSchema = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["type"] = "object",
            ["properties"] = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase),
        };
        var payloadSchema = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
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
            ["args"] = CreateValidator(argsSchema),
            ["payload"] = CreateValidator(payloadSchema),
        };

        return trunk;
    }

    private static JsonSchema? CreateValidator(Dictionary<string, object?>? schema)
    {
        if (schema == null)
            return null;
        // Json.Schema defaults to 2020-12. Python uses Draft4Validator. Keep derived input
        // models on the common subset (type/properties/required).
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
            outputModel = OutputSchema.Normalize(outputModel)!;

            if (outputModel.TryGetValue("properties", out var op) && op is Dictionary<string, object?> opDict)
                outputProperties = opDict;

            branch.OutputType = outputModel.TryGetValue("type", out var ot) && ot != null
                ? ot.ToString() ?? "array"
                : "array";

            if (outputModel.TryGetValue("parent_rows", out var pr) && pr is bool prBool)
                branch.UseParentRows = prBool;

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
                if (k.StartsWith('$') && !k.Contains("$parent"))
                    ExpandParameter(AsModelDict(model), k, v);
                else
                    ExpandParameter(payloadModel, k, v);
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

    private static string JsonTypeForParam(ParamDecl param)
    {
        if (!SqlToJsonType.TryGetValue(param.Type, out var jsonType))
            throw new InvalidOperationException("unknown parameter type '" + param.Type + "'");
        return jsonType;
    }

    private static void ExpandParameter(Dictionary<string, object?>? model, string prop, ParamDecl value)
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
            }

            ExpandParameter(model, prop[(dot + 1)..], value);
            return;
        }

        if (model == null ||
            !model.TryGetValue("properties", out var propsObj) ||
            propsObj is not Dictionary<string, object?> props)
        {
            return;
        }

        var jsonType = JsonTypeForParam(value);
        var newRequired = value.Required;
        var newHasDefault = value.HasDefault;
        var newDefault = value.Default;
        var requiredList = GetRequiredList(model);
        var existingRequired = requiredList.Contains(prop, StringComparer.OrdinalIgnoreCase);

        if (props.TryGetValue(prop, out var existingObj) && existingObj is Dictionary<string, object?> existing)
        {
            var existingType = existing.TryGetValue("type", out var t) ? t?.ToString() : null;
            var existingHasDefault = existing.ContainsKey("default");
            var existingDefault = existingHasDefault ? existing["default"] : null;
            if (!string.Equals(existingType, jsonType, StringComparison.OrdinalIgnoreCase) ||
                existingRequired != newRequired ||
                existingHasDefault != newHasDefault ||
                !DefaultsEqual(existingDefault, newDefault))
            {
                throw new InvalidOperationException(
                    "conflicting parameter declaration for '" + prop +
                    "': existing type=" + existingType + " required=" + existingRequired +
                    " default=" + existingDefault +
                    ", new type=" + jsonType + " required=" + newRequired +
                    " default=" + newDefault);
            }
            return;
        }

        var propSchema = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            ["type"] = jsonType,
        };
        if (newHasDefault)
            propSchema["default"] = newDefault;
        props[prop] = propSchema;
        if (newRequired)
        {
            if (!model.TryGetValue("required", out var reqObj) || reqObj is not List<object?> reqList)
            {
                reqList = new List<object?>();
                model["required"] = reqList;
            }
            if (!reqList.Any(x => x is string s && s.Equals(prop, StringComparison.OrdinalIgnoreCase)))
                reqList.Add(prop);
        }
    }

    private static bool DefaultsEqual(object? a, object? b)
    {
        if (a == null && b == null)
            return true;
        if (a == null || b == null)
            return false;
        if (a is long or int or short or byte || b is long or int or short or byte)
        {
            try
            {
                return Convert.ToInt64(a) == Convert.ToInt64(b);
            }
            catch
            {
                return false;
            }
        }
        if (a is double or float or decimal || b is double or float or decimal)
        {
            try
            {
                return Math.Abs(Convert.ToDouble(a) - Convert.ToDouble(b)) < 1e-9;
            }
            catch
            {
                return false;
            }
        }
        return Equals(a, b);
    }

    private static List<string> GetRequiredList(Dictionary<string, object?> model)
    {
        if (!model.TryGetValue("required", out var reqObj) || reqObj == null)
            return new List<string>();
        if (reqObj is List<object?> list)
            return list.OfType<string>().ToList();
        if (reqObj is IList<object?> ilist)
            return ilist.OfType<string>().ToList();
        return new List<string>();
    }
}
