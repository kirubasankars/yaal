// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

namespace Yaal.Sql;

public static class ParamTypeUtil
{
    private static readonly HashSet<string> ScalarTypes = new(StringComparer.OrdinalIgnoreCase)
    {
        "integer", "string", "float", "bool", "blob",
    };

    public const string ArraySuffix = "[]";

    public static bool IsArrayType(string paramType) =>
        paramType.EndsWith(ArraySuffix, StringComparison.Ordinal);

    public static string ElementType(string paramType)
    {
        if (IsArrayType(paramType))
            return paramType[..^ArraySuffix.Length];
        return paramType;
    }

    public static bool IsKnownType(string paramType)
    {
        if (ScalarTypes.Contains(paramType))
            return true;
        if (IsArrayType(paramType))
            return ScalarTypes.Contains(ElementType(paramType));
        return false;
    }
}
