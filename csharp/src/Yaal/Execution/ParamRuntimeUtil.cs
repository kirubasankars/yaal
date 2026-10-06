// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using Yaal.Sql;

namespace Yaal.Execution;

internal static class ParamRuntimeUtil
{
    public static Dictionary<string, string> ParamTypesByName(Twig twig)
    {
        var outMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in twig.Parameters)
            outMap[p.Name] = p.Type;
        // Desugar consumes the group blob source token, so its declared type only
        // survives on the group metadata. The header already enforced `blob`.
        foreach (var token in twig.Content)
        {
            if (token.GroupSource is { } source && !outMap.ContainsKey(source))
                outMap[source] = "blob";
        }
        return outMap;
    }

    public static bool NullableValueIsAbsent(string paramType, object? value)
    {
        if (value == null)
            return true;
        if (paramType.Equals("blob", StringComparison.OrdinalIgnoreCase))
        {
            if (value is string or byte[])
            {
                try
                {
                    value = GroupRuntimeUtil.CoerceOptionalGroupsBlob(value, "");
                }
                catch (InvalidOperationException)
                {
                    return false;
                }
            }
            return value is System.Collections.IList list && list.Count == 0;
        }
        if (ParamTypeUtil.IsArrayType(paramType))
        {
            if (value is System.Collections.ICollection coll)
                return coll.Count == 0;
            return false;
        }
        return false;
    }

    public static Dictionary<string, int> ArrayLengthsForCompile(
        Twig twig,
        Func<string, object?> getProp,
        IEnumerable<string> nulls)
    {
        var nullsSet = new HashSet<string>(
            nulls.Select(n => n.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);
        var lengths = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in twig.Parameters)
        {
            if (!ParamTypeUtil.IsArrayType(p.Type))
                continue;
            if (nullsSet.Contains(p.Name))
                continue;
            var val = getProp(p.Name);
            if (val == null)
                continue;
            if (val is System.Collections.IList list && val is not byte[])
                lengths[p.Name] = list.Count;
            else
                throw new InvalidOperationException(
                    "array parameter {{" + p.Name + "}} must be a sequence, got " +
                    val.GetType().Name);
        }
        return lengths;
    }
}
