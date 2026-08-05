// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Collections;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Yaal;

public static class JsonUtil
{
    public static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = null,
        WriteIndented = false,
    };

    public static object? ToPlain(JsonNode? node)
    {
        if (node == null)
            return null;
        return node switch
        {
            JsonObject obj => obj.ToDictionary(
                kv => kv.Key,
                kv => ToPlain(kv.Value),
                StringComparer.OrdinalIgnoreCase),
            JsonArray arr => arr.Select(ToPlain).ToList(),
            JsonValue val => val.GetValue<object?>(),
            _ => null,
        };
    }

    public static Dictionary<string, object?>? ToDict(object? value)
    {
        if (value == null)
            return null;
        if (value is Dictionary<string, object?> d)
            return d;
        if (value is IDictionary<string, object?> id)
            return id.ToDictionary(kv => kv.Key, kv => kv.Value, StringComparer.OrdinalIgnoreCase);
        if (value is JsonObject jo)
            return (Dictionary<string, object?>)ToPlain(jo)!;
        if (value is JsonElement je)
        {
            if (je.ValueKind == JsonValueKind.Object)
                return (Dictionary<string, object?>?)FromJsonElement(je);
        }
        return ObjectToDictionary(value);
    }

    public static Dictionary<string, object?> ObjectToDictionary(object value)
    {
        if (value is Dictionary<string, object?> d)
            return d;

        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        foreach (var prop in value.GetType().GetProperties())
        {
            if (!prop.CanRead)
                continue;
            result[prop.Name.ToLowerInvariant()] = NormalizeValue(prop.GetValue(value));
        }
        return result;
    }

    public static object? NormalizeValue(object? value)
    {
        if (value == null)
            return null;
        if (value is JsonElement je)
            return FromJsonElement(je);
        if (value is JsonNode jn)
            return ToPlain(jn);
        if (value is string or int or long or float or double or decimal or bool or Guid)
            return value;
        if (value is Dictionary<string, object?>)
            return value;
        if (value is string)
            return value;
        if (value is IEnumerable<object?> list)
            return list.Select(NormalizeValue).ToList();
        if (value.GetType().IsPrimitive || value is DateTime or DateTimeOffset)
            return value;
        return ObjectToDictionary(value);
    }

    public static object? FromJsonElement(JsonElement je) =>
        je.ValueKind switch
        {
            JsonValueKind.Object => je.EnumerateObject()
                .ToDictionary(p => p.Name.ToLowerInvariant(), p => FromJsonElement(p.Value), StringComparer.OrdinalIgnoreCase),
            JsonValueKind.Array => je.EnumerateArray().Select(FromJsonElement).Cast<object?>().ToList(),
            JsonValueKind.String => je.GetString(),
            JsonValueKind.Number => je.TryGetInt64(out var l) ? l : je.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => null,
        };

    public static JsonNode? ToJsonNode(object? value)
    {
        if (value == null)
            return null;
        if (value is JsonNode jn)
            return jn;
        if (value is Dictionary<string, object?> dict)
        {
            var obj = new JsonObject();
            foreach (var (k, v) in dict)
                obj[k] = ToJsonNode(v);
            return obj;
        }
        if (value is IDictionary<string, object?> idict)
        {
            var obj = new JsonObject();
            foreach (var (k, v) in idict)
                obj[k] = ToJsonNode(v);
            return obj;
        }
        if (value is IList<object?> list)
        {
            var arr = new JsonArray();
            foreach (var item in list)
                arr.Add(ToJsonNode(item));
            return arr;
        }
        if (value is not string && value is IEnumerable enumerableObj)
        {
            var arr = new JsonArray();
            foreach (var item in enumerableObj)
                arr.Add(ToJsonNode(item));
            return arr;
        }
        return JsonValue.Create(value);
    }

    public static string Serialize(object? value, bool indented = false)
    {
        var options = indented
            ? new JsonSerializerOptions { WriteIndented = true }
            : SerializerOptions;
        return JsonSerializer.Serialize(ToJsonNode(value), options);
    }

    public static object? DeepCopy(object? value)
    {
        if (value == null)
            return null;
        var json = Serialize(value);
        return ToPlain(JsonNode.Parse(json));
    }

    public static List<IDictionary<string, object?>> DeepCopyRows(IEnumerable<IDictionary<string, object?>> rows)
    {
        var copied = DeepCopy(rows.ToList());
        return NormalizeRowList(copied);
    }

    public static List<IDictionary<string, object?>> NormalizeRowList(object? value)
    {
        if (value is List<IDictionary<string, object?>> typed)
            return typed;
        if (value is IList<object?> list)
        {
            return list.Select(item =>
                item as IDictionary<string, object?> ??
                ToDict(item) as IDictionary<string, object?> ??
                new Dictionary<string, object?>()).ToList();
        }
        if (value is IEnumerable enumerable and not string)
        {
            var result = new List<IDictionary<string, object?>>();
            foreach (var item in enumerable)
            {
                result.Add(item as IDictionary<string, object?> ??
                           ToDict(item) ??
                           new Dictionary<string, object?>());
            }
            return result;
        }
        return new List<IDictionary<string, object?>>();
    }

    public static Dictionary<string, object?> ToLowerKeys(Dictionary<string, object?>? obj)
    {
        if (obj == null)
            return new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        return obj.ToDictionary(kv => kv.Key.ToLowerInvariant(), kv => kv.Value, StringComparer.OrdinalIgnoreCase);
    }

    public static object? ToLowerKeysDeep(object? obj)
    {
        if (obj is Dictionary<string, object?> dict)
        {
            var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (var (k, v) in dict)
            {
                var key = k.ToLowerInvariant();
                if (key == "properties" && v is Dictionary<string, object?> props)
                {
                    result[key] = props.ToDictionary(
                        pk => pk.Key.ToLowerInvariant(),
                        pk => ToLowerKeysDeep(pk.Value),
                        StringComparer.OrdinalIgnoreCase);
                }
                else if (key == "required" && v is IList<object?> req)
                {
                    result[key] = req.Select(x => x is string s ? s.ToLowerInvariant() : x).Cast<object?>().ToList();
                }
                else if (v is Dictionary<string, object?>)
                {
                    result[key] = ToLowerKeysDeep(v);
                }
                else if (v is IList<object?> list)
                {
                    result[key] = list.Select(item =>
                        item is Dictionary<string, object?> ? ToLowerKeysDeep(item) : item).Cast<object?>().ToList();
                }
                else
                {
                    result[key] = v;
                }
            }
            return result;
        }

        if (obj is IList<object?> listObj)
            return listObj.Select(item => item is Dictionary<string, object?> ? ToLowerKeysDeep(item) : item).Cast<object?>().ToList();

        return obj;
    }

    public static Dictionary<string, object?> YamlToDict(string yaml)
    {
        var deserializer = new YamlDotNet.Serialization.DeserializerBuilder()
            .WithAttemptingUnquotedStringTypeDeserialization()
            .Build();
        var obj = deserializer.Deserialize<object>(yaml);
        return ConvertYamlObject(obj) as Dictionary<string, object?>
               ?? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
    }

    private static object? ConvertYamlObject(object? obj)
    {
        switch (obj)
        {
            case null:
                return null;
            case string or bool or int or long or double or float or decimal:
                return obj;
            case Dictionary<object, object> map:
            {
                var dict = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                foreach (var (k, v) in map)
                    dict[k.ToString()!.ToLowerInvariant()] = ConvertYamlObject(v);
                return dict;
            }
            case List<object> list:
                return list.Select(ConvertYamlObject).Cast<object?>().ToList();
            default:
                if (obj is IDictionary anyDict)
                {
                    var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    foreach (DictionaryEntry entry in anyDict)
                        result[entry.Key!.ToString()!.ToLowerInvariant()] = ConvertYamlObject(entry.Value);
                    return result;
                }
                if (obj is System.Collections.IList ilist)
                {
                    var result = new List<object?>();
                    foreach (var item in ilist)
                        result.Add(ConvertYamlObject(item));
                    return result;
                }
                return obj;
        }
    }
}
