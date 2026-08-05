using Yaal.Descriptors;

namespace Yaal.Execution;

public static class OutputMapper
{
    public static object Map(
        string outputType,
        Dictionary<string, object?>? outputModel,
        List<Branch>? branches,
        List<IDictionary<string, object?>> result)
    {
        var mappedResult = new List<object?>();

        Dictionary<string, object?>? outputProperties = null;
        if (outputModel != null &&
            outputModel.TryGetValue("properties", out var props) &&
            props is Dictionary<string, object?> propsDict)
        {
            outputProperties = propsDict;
        }

        foreach (var row in result)
        {
            var mappedObj = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var mappedTree = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

            if (branches != null)
            {
                foreach (var branchDescriptor in branches)
                {
                    var branchName = branchDescriptor.Name;
                    var branchOutputType = branchDescriptor.OutputType;

                    Dictionary<string, object?>? branchOutputModel = null;
                    if (outputProperties != null &&
                        outputProperties.TryGetValue(branchName, out var bom) &&
                        bom is Dictionary<string, object?> bomDict)
                    {
                        branchOutputModel = bomDict;
                    }

                    if (row.TryGetValue(branchName, out var childRowsObj))
                    {
                        var childRows = NormalizeRows(childRowsObj);
                        mappedTree[branchName] = Map(
                            branchOutputType,
                            branchOutputModel,
                            branchDescriptor.Branches,
                            childRows);
                    }
                }
            }

            if (outputProperties != null)
            {
                var propCount = 0;
                foreach (var (k, v) in outputProperties)
                {
                    string? mapped = null;
                    string? type = null;

                    if (v is string s)
                        mapped = s;
                    if (v is Dictionary<string, object?> vDict)
                    {
                        if (vDict.TryGetValue("mapped", out var m))
                            mapped = m?.ToString();
                        if (vDict.TryGetValue("type", out var t))
                            type = t?.ToString();
                    }

                    if (mapped != null)
                    {
                        if (row.TryGetValue(mapped, out var mappedValue))
                        {
                            mappedObj[k] = mappedValue;
                            propCount += 1;
                        }
                        else
                        {
                            throw new InvalidOperationException(mapped + " _mapped column missing from row");
                        }
                    }

                    if (type is "array" or "object")
                        mappedObj[k] = mappedTree[k];
                }

                if (propCount == 0)
                    mappedObj = row.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
            }
            else
            {
                mappedObj = row.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
            }

            foreach (var (k, v) in mappedTree)
                mappedObj[k] = v;

            mappedResult.Add(mappedObj);
        }

        if (outputType == "object")
        {
            if (mappedResult.Count > 0)
                return mappedResult[0]!;
            return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        }

        return mappedResult;
    }

    private static List<IDictionary<string, object?>> NormalizeRows(object? childRowsObj)
    {
        if (childRowsObj is List<IDictionary<string, object?>> typed)
            return typed;
        if (childRowsObj is IList<object?> list)
        {
            return list.Select(item =>
            {
                if (item is IDictionary<string, object?> d)
                    return d;
                return JsonUtil.ToDict(item) as IDictionary<string, object?>
                       ?? new Dictionary<string, object?>();
            }).ToList();
        }
        if (childRowsObj is System.Collections.IEnumerable enumerable and not string)
        {
            var result = new List<IDictionary<string, object?>>();
            foreach (var item in enumerable)
            {
                if (item is IDictionary<string, object?> d)
                    result.Add(d);
                else
                    result.Add(JsonUtil.ToDict(item) ?? new Dictionary<string, object?>());
            }
            return result;
        }
        return new List<IDictionary<string, object?>>();
    }
}
