// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Sql;

namespace Yaal.Execution;

public delegate object? ValueConverter(string paramType, object? value);

public sealed class DataProviderHelper
{
    private readonly Dictionary<string, object?> _paramCache = new(StringComparer.Ordinal);
    private readonly Dictionary<(Twig Twig, string NullsKey, string Placeholder, string SortKey, string ArrayKey, string GroupKey), CompiledSql>
        _compileCache = new();

    /// <summary>Clear bind-parameter cache (compile cache kept for the helper lifetime).</summary>
    public void ClearCache() => _paramCache.Clear();

    public CompiledSql GetExecutableContent(string placeholder, Twig twig, Shape inputShape)
    {
        var paramTypes = ParamRuntimeUtil.ParamTypesByName(twig);
        var nulls = new List<string>();
        if (twig.Nullable != null)
        {
            foreach (var n in twig.Nullable)
            {
                paramTypes.TryGetValue(n, out var ptype);
                ptype ??= "string";
                if (ParamRuntimeUtil.NullableValueIsAbsent(ptype, inputShape.GetProp(n)))
                    nulls.Add(n);
            }
        }

        Dictionary<string, string?> sortMap;
        if (twig.HasSortDir == false)
            sortMap = new Dictionary<string, string?>(StringComparer.Ordinal);
        else
            sortMap = SortDirDesugar.ResolveValues(twig, inputShape);
        var arrayLengths = ParamRuntimeUtil.ArrayLengthsForCompile(
            twig, inputShape.GetProp, nulls);
        var (groupCounts, groupFieldLengths, groupFieldIsArray) = GroupRuntimeUtil.GroupShapesForCompile(
            twig, inputShape.GetProp, nulls);
        var nullsKey = string.Join("\0", nulls.Select(n => n.ToLowerInvariant()).OrderBy(n => n, StringComparer.Ordinal));
        var sortKey = string.Join("\0", sortMap.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => kv.Key + "=" + (kv.Value ?? "")));
        var arrayKey = string.Join("\0", arrayLengths.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => kv.Key + "=" + kv.Value));
        var groupKey = string.Join("\0", groupCounts.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => kv.Key + "=" + kv.Value))
            + "|" + string.Join("\0", groupFieldLengths.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => kv.Key + ":" + string.Join(",", kv.Value.Select(row =>
                    string.Join(",", row.OrderBy(r => r.Key).Select(r => r.Key + "=" + r.Value))))))
            + "|" + string.Join("\0", groupFieldIsArray.OrderBy(kv => kv.Key, StringComparer.Ordinal)
                .Select(kv => kv.Key + ":" + string.Join(",", kv.Value.Select(row =>
                    string.Join(",", row.OrderBy(r => r.Key).Select(r => r.Key + "=" + r.Value))))));
        var key = (twig, nullsKey, placeholder, sortKey, arrayKey, groupKey);
        if (_compileCache.TryGetValue(key, out var cached))
        {
            return new CompiledSql
            {
                Content = cached.Content,
                Parameters = cached.Parameters.ToList(),
            };
        }

        var compiled = SqlCompiler.Compile(
            twig, nulls, placeholder, sortMap, arrayLengths, groupCounts, groupFieldLengths, groupFieldIsArray);
        _compileCache[key] = new CompiledSql
        {
            Content = compiled.Content,
            Parameters = compiled.Parameters.ToList(),
        };
        return new CompiledSql
        {
            Content = compiled.Content,
            Parameters = compiled.Parameters.ToList(),
        };
    }

    public List<object?> BuildParameters(CompiledSql query, Shape inputShape, ValueConverter getValueConverter)
    {
        var values = new List<object?>();
        var arrayIndexes = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var groupBlobCache = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in query.Parameters)
        {
            var paramName = p.Name;
            var paramType = p.Type;
            object? paramValue;

            if (p.GroupSource != null)
            {
                if (!groupBlobCache.TryGetValue(p.GroupSource, out var pairs))
                {
                    pairs = GroupRuntimeUtil.CoerceOptionalGroupsBlob(
                        inputShape.GetProp(p.GroupSource), p.GroupSource);
                    groupBlobCache[p.GroupSource] = pairs;
                }
                if (pairs is not System.Collections.IList list || p.GroupIndex is not int gi || gi >= list.Count)
                    throw new InvalidOperationException(
                        "optional_groups row missing for {{" + p.GroupField + "}}");
                if (list[gi] is not System.Collections.IDictionary row)
                    throw new InvalidOperationException(
                        "optional_groups {{" + p.GroupSource + "}} rows must be objects");
                paramValue = GroupRuntimeUtil.RowGetInsensitive(row, p.GroupField!);
                if (p.GroupSubindex is int sub)
                {
                    if (!GroupRuntimeUtil.IsGroupFieldArray(paramValue))
                        throw new InvalidOperationException(
                            "optional_groups field {{" + p.GroupField + "}} must be an array");
                    var arr = (System.Collections.IList)paramValue!;
                    if (sub >= arr.Count)
                        throw new InvalidOperationException(
                            "optional_groups field {{" + p.GroupField + "}} index out of range");
                    paramValue = arr[sub];
                }
                GroupRuntimeUtil.AssertGroupScalar(paramValue, p.GroupSource, p.GroupField!);
            }
            else if (p.ArrayElement)
            {
                if (!_paramCache.TryGetValue(paramName, out var seq))
                {
                    seq = inputShape.GetProp(paramName);
                    _paramCache[paramName] = seq;
                }
                var idx = arrayIndexes.GetValueOrDefault(paramName);
                if (seq is System.Collections.IList list)
                    paramValue = idx < list.Count ? list[idx] : null;
                else
                    paramValue = null;
                arrayIndexes[paramName] = idx + 1;
            }
            else if (_paramCache.TryGetValue(paramName, out var cached))
            {
                paramValue = cached;
            }
            else
            {
                paramValue = inputShape.GetProp(paramName);
                if (paramName.StartsWith('$') && !paramName.Contains("$parent"))
                    _paramCache[paramName] = paramValue;
            }

            try
            {
                if (paramValue != null && p.GroupSource == null)
                {
                    if (paramType == "integer")
                        paramValue = Convert.ToInt64(paramValue);
                    else if (paramType == "string")
                        paramValue = paramValue.ToString();
                    else
                        paramValue = getValueConverter(paramType, paramValue);
                }
                values.Add(paramValue);
            }
            catch (FormatException)
            {
                values.Add(paramValue);
            }
            catch (InvalidCastException)
            {
                values.Add(paramValue);
            }
        }

        return values;
    }
}
