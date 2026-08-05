using Yaal.Sql;

namespace Yaal.Execution;

public delegate object? ValueConverter(string paramType, object? value);

public sealed class DataProviderHelper
{
    private readonly Dictionary<string, object?> _cache = new(StringComparer.Ordinal);

    public void ClearCache() => _cache.Clear();

    public static CompiledSql GetExecutableContent(string placeholder, Twig twig, Shape inputShape)
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
        return SqlCompiler.Compile(twig, nulls, placeholder);
    }

    public List<object?> BuildParameters(CompiledSql query, Shape inputShape, ValueConverter getValueConverter)
    {
        var values = new List<object?>();
        foreach (var p in query.Parameters)
        {
            var paramName = p.Name;
            var paramType = p.Type;
            object? paramValue;

            if (_cache.TryGetValue(paramName, out var cached))
            {
                paramValue = cached;
            }
            else
            {
                paramValue = inputShape.GetProp(paramName);
                if (paramName.StartsWith('$') && !paramName.Contains("$parent"))
                    _cache[paramName] = paramValue;
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
