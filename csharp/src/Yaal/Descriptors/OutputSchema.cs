// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Descriptors;

/// <summary>
/// Validate Yaal output YAML schemas (flat-only field maps).
/// </summary>
public static class OutputSchema
{
    private const string ErrBareType =
        "bare properties.type object|array is not allowed; " +
        "use flat fields under properties (root type already sets array/object)";

    /// <summary>
    /// Validate output models and recurse into named branch schemas.
    /// </summary>
    public static Dictionary<string, object?>? Normalize(Dictionary<string, object?>? model)
    {
        if (model == null)
            return null;

        var result = new Dictionary<string, object?>(model, StringComparer.OrdinalIgnoreCase);
        if (!result.TryGetValue("properties", out var propsObj) ||
            propsObj is not Dictionary<string, object?> props)
        {
            return result;
        }

        props = new Dictionary<string, object?>(props, StringComparer.OrdinalIgnoreCase);

        if (props.TryGetValue("type", out var bareObj) &&
            bareObj is string bare &&
            (bare is "object" or "array"))
        {
            throw new InvalidOperationException(ErrBareType);
        }

        var newProps = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var (k, v) in props)
        {
            if (v is Dictionary<string, object?> vDict &&
                !vDict.ContainsKey("mapped") &&
                (vDict.ContainsKey("type") || vDict.ContainsKey("properties")))
            {
                newProps[k] = Normalize(vDict)!;
            }
            else
            {
                newProps[k] = v;
            }
        }

        result["properties"] = newProps;
        return result;
    }
}
