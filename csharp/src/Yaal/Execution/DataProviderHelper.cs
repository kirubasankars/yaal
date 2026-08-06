// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Sql;

namespace Yaal.Execution;

public delegate object? ValueConverter(string paramType, object? value);

public sealed class DataProviderHelper
{
    private readonly Dictionary<string, object?> _paramCache = new(StringComparer.Ordinal);
    private readonly Dictionary<(Twig Twig, string NullsKey, string Placeholder, string SortKey, string DirKey), CompiledSql>
        _compileCache = new();

    /// <summary>Clear bind-parameter cache (compile cache kept for the helper lifetime).</summary>
    public void ClearCache() => _paramCache.Clear();

    public CompiledSql GetExecutableContent(string placeholder, Twig twig, Shape inputShape)
    {
        var nulls = new List<string>();
        if (twig.Nullable != null)
        {
            foreach (var n in twig.Nullable)
            {
                if (inputShape.GetProp(n) == null)
                    nulls.Add(n);
            }
        }

        var (sortMap, dirMap) = SortDirDesugar.ResolveValues(twig, inputShape);
        var nullsKey = string.Join("\0", nulls.Select(n => n.ToLowerInvariant()).OrderBy(n => n, StringComparer.Ordinal));
        var sortKey = string.Join("\0", sortMap.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => kv.Key + "=" + (kv.Value ?? "")));
        var dirKey = string.Join("\0", dirMap.OrderBy(kv => kv.Key, StringComparer.Ordinal)
            .Select(kv => kv.Key + "=" + kv.Value));
        var key = (twig, nullsKey, placeholder, sortKey, dirKey);
        if (_compileCache.TryGetValue(key, out var cached))
        {
            return new CompiledSql
            {
                Content = cached.Content,
                Parameters = cached.Parameters.ToList(),
            };
        }

        var compiled = SqlCompiler.Compile(twig, nulls, placeholder, sortMap, dirMap);
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
        foreach (var p in query.Parameters)
        {
            var paramName = p.Name;
            var paramType = p.Type;
            object? paramValue;

            if (_paramCache.TryGetValue(paramName, out var cached))
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
                if (paramValue != null)
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
