// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json;
using Yaal.Descriptors;
using Yaal.Providers;

namespace Yaal.Execution;

public static class Executor
{
    public static object? GetResult(
        Branch descriptor,
        Func<string, IDataProvider> getDataProvider,
        Shape context)
    {
        return GetResultCore(descriptor, getDataProvider, context);
    }

    public static string GetResultJson(
        Branch descriptor,
        Func<string, IDataProvider> getDataProvider,
        Shape context)
    {
        var result = GetResult(descriptor, getDataProvider, context);
        return JsonUtil.Serialize(result);
    }

    private static object? GetResultCore(
        Branch descriptor,
        Func<string, IDataProvider> getDataProvider,
        Shape ctx)
    {
        var errors = new List<Dictionary<string, object?>>();
        var argsShape = ctx.GetProp("$args") as Shape;
        if (argsShape != null)
            errors.AddRange(argsShape.Validate(true));
        errors.AddRange(ctx.Validate(false));

        if (errors.Count > 0)
            return new Dictionary<string, object?> { ["errors"] = errors };

        var dataProviders = new Dictionary<string, IDataProvider>(StringComparer.Ordinal);
        foreach (var con in descriptor.Connections ?? new List<string> { "db" })
            dataProviders[con] = getDataProvider(con);

        var (rs, execErrors) = ExecuteBranch(descriptor, true, dataProviders, ctx, new List<IDictionary<string, object?>>());
        if (execErrors != null)
            return new Dictionary<string, object?> { ["errors"] = execErrors };

        return OutputMapper.Map(
            descriptor.OutputType,
            descriptor.Model?.Output,
            descriptor.Branches,
            rs ?? new List<IDictionary<string, object?>>());
    }

    private static (List<IDictionary<string, object?>>? Rows, List<IDictionary<string, object?>>? Errors) ExecuteTwigs(
        Branch branch,
        Dictionary<string, IDataProvider> dataProviders,
        Shape context,
        DataProviderHelper dataProviderHelper)
    {
        var errors = new List<IDictionary<string, object?>>();
        var twigs = branch.Twigs;
        var rs = new List<IDictionary<string, object?>>();

        if (twigs == null)
            return (rs, null);

        foreach (var twig in twigs)
        {
            var connection = twig.Connection;
            IReadOnlyList<IDictionary<string, object?>> output;
            object? outputLastInsertedId;
            try
            {
                (output, outputLastInsertedId) = dataProviders[connection].Execute(twig, context, dataProviderHelper);
            }
            catch (SortDirException ex)
            {
                return (null, new List<IDictionary<string, object?>>
                {
                    new Dictionary<string, object?> { ["message"] = ex.Message },
                });
            }

            ((Shape)context.GetProp("$params")!).SetProp("$last_inserted_id", outputLastInsertedId);

            if (output.Count >= 1)
            {
                var output0 = output[0];
                if (output0.ContainsKey(YaalConst.Mode))
                {
                    var modeValue = output0[YaalConst.Mode]?.ToString();
                    if (modeValue == "error")
                    {
                        errors.AddRange(output);
                        return (null, errors.Cast<IDictionary<string, object?>>().ToList());
                    }

                    if (modeValue == "json")
                    {
                        var jsonList = new List<object?>();
                        if (output0["json"] is string)
                        {
                            foreach (var o in output)
                                jsonList.Add(JsonSerializer.Deserialize<object>(o["json"]!.ToString()!));
                        }
                        else
                        {
                            jsonList.AddRange(output.Select(o => o["json"]));
                        }
                        // Return as list of dicts approximation
                        return (jsonList.Select(x => x as IDictionary<string, object?> ??
                            JsonUtil.ToDict(x) as IDictionary<string, object?> ??
                            new Dictionary<string, object?>()).ToList(), null);
                    }

                    if (modeValue == "break")
                    {
                        foreach (var o in output)
                            o.Remove(YaalConst.Mode);
                        return (output.ToList(), null);
                    }

                    if (modeValue == "params")
                    {
                        var paramsShape = (Shape)context.GetProp("$params")!;
                        foreach (var (k, v) in output0)
                            paramsShape.SetProp(k, v);
                    }
                }
                else
                {
                    rs = output.ToList();
                }
            }
        }

        return (rs, null);
    }

    private static string PartitionKey(object? value) =>
        value switch
        {
            null => "",
            string s => s,
            IFormattable f => f.ToString(null, System.Globalization.CultureInfo.InvariantCulture) ?? "",
            _ => value.ToString() ?? "",
        };

    private static void TrunkCleanup(
        Dictionary<string, IDataProvider> dataProviders,
        IDataProvider dbDataProvider,
        bool failed)
    {
        if (failed)
        {
            try { dbDataProvider.Error(); } catch { /* ignore */ }
            foreach (var (name, dataProvider) in dataProviders)
            {
                if (name != "db")
                {
                    try { dataProvider.Error(); } catch { /* ignore */ }
                }
            }
            return;
        }

        dbDataProvider.End();
        foreach (var (name, dataProvider) in dataProviders)
        {
            if (name != "db")
                dataProvider.End();
        }
    }

    internal static (List<IDictionary<string, object?>>? Rows, List<IDictionary<string, object?>>? Errors) ExecuteBranch(
        Branch branch,
        bool isTrunk,
        Dictionary<string, IDataProvider> dataProviders,
        Shape context,
        List<IDictionary<string, object?>> parentRows)
    {
        var inputType = branch.InputType;
        var outputPartitionBy = branch.PartitionBy;
        var useParentRows = branch.UseParentRows;
        var output = new List<IDictionary<string, object?>>();
        var dataProviderHelper = new DataProviderHelper();
        var dbDataProvider = dataProviders["db"];
        var began = false;
        var failed = false;

        try
        {
            if (useParentRows)
            {
                output = JsonUtil.DeepCopyRows(parentRows);
            }
            else
            {
                if (isTrunk)
                {
                    foreach (var dataProvider in dataProviders.Values)
                        dataProvider.Begin();
                    began = true;
                }

                if (inputType == "array")
                {
                    var length = Convert.ToInt32(context.GetProp("$length"));
                    for (var i = 0; i < length; i++)
                    {
                        dataProviderHelper.ClearCache();
                        var itemCtx = (Shape)context.GetProp("@" + i)!;
                        var (rs, errors) = ExecuteTwigs(branch, dataProviders, itemCtx, dataProviderHelper);
                        if (errors != null)
                        {
                            failed = true;
                            return (null, errors);
                        }
                        output.AddRange(rs!);
                    }
                }
                else if (inputType == "object")
                {
                    var (rs, errors) = ExecuteTwigs(branch, dataProviders, context, dataProviderHelper);
                    if (errors != null)
                    {
                        failed = true;
                        return (null, errors);
                    }
                    output = rs!;
                }
            }

            var branches = branch.Branches;
            if (branches != null)
            {
                foreach (var branchDescriptor in branches)
                {
                    var branchName = branchDescriptor.Name;
                    var subNodeShape = context;
                    var nested = context.GetProp(branchName.ToLowerInvariant());
                    if (nested is Shape nestedShape)
                        subNodeShape = nestedShape;

                    var (subNodeOutput, errors) = ExecuteBranch(
                        branchDescriptor, false, dataProviders, subNodeShape, output);
                    if (errors != null)
                    {
                        failed = true;
                        return (null, errors);
                    }

                    if (branch.Twigs == null && !useParentRows && output.Count == 0)
                        output.Add(new Dictionary<string, object?>());

                    if (string.IsNullOrEmpty(outputPartitionBy))
                    {
                        foreach (var row in output)
                            row[branchName] = JsonUtil.DeepCopy(subNodeOutput);
                    }
                    else
                    {
                        var subNodeGroups = new Dictionary<string, List<IDictionary<string, object?>>>(StringComparer.Ordinal);
                        foreach (var row in subNodeOutput!)
                        {
                            if (!row.ContainsKey(outputPartitionBy))
                                throw new KeyNotFoundException(
                                    "partition_by column '" + outputPartitionBy + "' missing from child row");
                            var key = PartitionKey(row[outputPartitionBy]);
                            if (!subNodeGroups.TryGetValue(key, out var list))
                            {
                                list = new List<IDictionary<string, object?>>();
                                subNodeGroups[key] = list;
                            }
                            list.Add(row);
                        }

                        var groups = new Dictionary<string, List<IDictionary<string, object?>>>(StringComparer.Ordinal);
                        foreach (var row in output)
                        {
                            if (!row.ContainsKey(outputPartitionBy))
                                throw new KeyNotFoundException(
                                    "partition_by column '" + outputPartitionBy + "' missing from parent row");
                            var key = PartitionKey(row[outputPartitionBy]);
                            if (!groups.TryGetValue(key, out var list))
                            {
                                list = new List<IDictionary<string, object?>>();
                                groups[key] = list;
                            }
                            list.Add(row);
                        }

                        var newOutput = new List<IDictionary<string, object?>>();
                        foreach (var rows in groups.Values)
                        {
                            var row = rows[0];
                            var partitionKey = PartitionKey(row[outputPartitionBy]);
                            row[branchName] = JsonUtil.DeepCopy(
                                subNodeGroups.TryGetValue(partitionKey, out var children)
                                    ? children
                                    : new List<IDictionary<string, object?>>());
                            newOutput.Add(row);
                        }
                        output = newOutput;
                    }
                }
            }

            return (output, null);
        }
        catch
        {
            failed = true;
            throw;
        }
        finally
        {
            if (isTrunk && began)
                TrunkCleanup(dataProviders, dbDataProvider, failed);
        }
    }
}
