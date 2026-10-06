// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Text.Json;
using Yaal.Sql;

namespace Yaal.Execution;

internal static class GroupRuntimeUtil
{
    public static object? RowGetInsensitive(System.Collections.IDictionary row, string field)
    {
        if (row.Contains(field))
            return row[field];
        foreach (var key in row.Keys)
        {
            if (key is string s && s.Equals(field, StringComparison.OrdinalIgnoreCase))
                return row[key];
        }
        throw new KeyNotFoundException(field);
    }

    internal static object? CoerceOptionalGroupsBlob(object? value, string sourceName)
    {
        if (value == null)
            return null;

        if (value is byte[] raw)
        {
            try
            {
                value = System.Text.Encoding.UTF8.GetString(raw);
            }
            catch (ArgumentException)
            {
                throw BlobShapeError(sourceName);
            }
        }

        if (value is string text)
        {
            text = text.Trim();
            if (text.Length == 0)
                return new List<object?>();
            try
            {
                using var doc = JsonDocument.Parse(text);
                value = JsonUtil.FromJsonElement(doc.RootElement);
            }
            catch (JsonException)
            {
                throw BlobShapeError(sourceName);
            }
        }

        if (value is JsonElement je)
            value = JsonUtil.FromJsonElement(je);

        if (value is not System.Collections.IList)
            throw BlobShapeError(sourceName);
        return value;
    }

    private static InvalidOperationException BlobShapeError(string sourceName) =>
        new("optional_groups {{" + sourceName + "}} must be a JSON array of objects");

    internal static void AssertGroupScalar(object? value, string source, string field)
    {
        if (value == null ||
            value is bool || value is string || value is byte[] ||
            value is sbyte || value is byte || value is short || value is ushort ||
            value is int || value is uint || value is long || value is ulong ||
            value is float || value is double || value is decimal ||
            value is DateTime || value is DateTimeOffset || value is Guid)
            return;
        throw new InvalidOperationException(
            "optional_groups field {{" + field + "}} in " + source +
            " must be a scalar value, got " + value.GetType().Name);
    }

    // byte[] is an IList but binds as a single blob value, never as an IN list.
    internal static bool IsGroupFieldArray(object? raw) =>
        raw is System.Collections.IList && raw is not byte[];

    private static int FieldPlaceholderCount(object? raw, string source, string field)
    {
        if (IsGroupFieldArray(raw))
        {
            var list = (System.Collections.IList)raw!;
            if (list.Count == 0)
                throw new InvalidOperationException(
                    "empty array for optional_groups field {{" + field + "}} in " + source +
                    "; IN list cannot be empty");
            foreach (var item in list)
                AssertGroupScalar(item, source, field);
            return list.Count;
        }
        if (raw == null)
            throw new InvalidOperationException(
                "missing optional_groups field {{" + field + "}} in " + source);
        AssertGroupScalar(raw, source, field);
        return 1;
    }

    public static (
        Dictionary<string, int> GroupCounts,
        Dictionary<string, List<Dictionary<string, int>>> FieldLengths,
        Dictionary<string, List<Dictionary<string, bool>>> FieldIsArray)
        GroupShapesForCompile(Twig twig, Func<string, object?> getProp, IEnumerable<string> nulls)
    {
        var nullsSet = new HashSet<string>(
            nulls.Select(n => n.ToLowerInvariant()),
            StringComparer.OrdinalIgnoreCase);
        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var fieldLengths = new Dictionary<string, List<Dictionary<string, int>>>(
            StringComparer.OrdinalIgnoreCase);
        var fieldIsArray = new Dictionary<string, List<Dictionary<string, bool>>>(
            StringComparer.OrdinalIgnoreCase);

        foreach (var tok in twig.Content)
        {
            if (tok.Type != "brace" || tok.Value != "(" || tok.GroupSource == null)
                continue;
            var source = tok.GroupSource;
            var key = source.ToLowerInvariant();
            if (nullsSet.Contains(key))
                continue;

            var val = CoerceOptionalGroupsBlob(getProp(source), source);
            if (val == null)
                throw new InvalidOperationException(
                    "missing blob value for optional_groups {{" + source + "}}");

            var list = (System.Collections.IList)val;

            counts[key] = list.Count;
            var perRow = new List<Dictionary<string, int>>();
            if (list.Count == 0)
            {
                fieldLengths[key] = perRow;
                fieldIsArray[key] = new List<Dictionary<string, bool>>();
                continue;
            }

            var fields = tok.GroupFields ?? new List<string>();
            var perRowArray = new List<Dictionary<string, bool>>();
            foreach (var item in list)
            {
                if (item is not System.Collections.IDictionary row)
                    throw new InvalidOperationException(
                        "optional_groups {{" + source + "}} rows must be objects");
                var rowLens = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                var rowArr = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);
                foreach (var field in fields)
                {
                    try
                    {
                        var raw = RowGetInsensitive(row, field);
                        rowLens[field] = FieldPlaceholderCount(raw, source, field);
                        rowArr[field] = IsGroupFieldArray(raw);
                    }
                    catch (KeyNotFoundException)
                    {
                        throw new InvalidOperationException(
                            "missing optional_groups field {{" + field + "}} in " + source);
                    }
                }
                perRow.Add(rowLens);
                perRowArray.Add(rowArr);
            }
            fieldLengths[key] = perRow;
            fieldIsArray[key] = perRowArray;
        }

        return (counts, fieldLengths, fieldIsArray);
    }
}
