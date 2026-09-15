// Copyright 2018 Kiruba Sankar Swaminathan. All rights reserved.
// Use of this source code is governed by a MIT style
// license that can be found in the LICENSE file.

using System.Collections;
using System.Reflection;
using System.Text;

namespace Yaal;

/// <summary>Map shaped query results (dict/list graphs) to POCOs.</summary>
public static class ObjectMaterializer
{
    private static readonly Dictionary<Type, TypeMap> Cache = new();

    private sealed class TypeMap
    {
        public required PropertyBinding[] Properties { get; init; }
    }

    private sealed class PropertyBinding
    {
        public required string Key { get; init; }
        public required PropertyInfo Property { get; init; }
        public required Type TargetType { get; init; }
        public required bool IsList { get; init; }
        public Type? ElementType { get; init; }
    }

    public static void ThrowIfErrors(object? shapedResult)
    {
        if (shapedResult is not IDictionary<string, object?> dict)
            return;
        if (!dict.TryGetValue("errors", out var errorsObj) || errorsObj == null)
            return;

        var errors = NormalizeErrors(errorsObj);
        if (errors.Count > 0)
            throw new YaalQueryException(errors);
    }

    public static T Map<T>(object? shapedResult) where T : notnull
    {
        if (shapedResult == null)
            throw new ArgumentNullException(nameof(shapedResult));

        if (shapedResult is T typed)
            return typed;

        if (shapedResult is IDictionary<string, object?> dict)
            return (T)MapObject(typeof(T), dict, strict: true)!;

        throw new InvalidOperationException(
            "Expected a shaped object (dictionary) but got " + shapedResult.GetType().Name);
    }

    public static List<T> MapList<T>(object? shapedResult)
    {
        if (shapedResult == null)
            return new List<T>();

        if (shapedResult is List<T> typedList)
            return typedList;

        var items = NormalizeList(shapedResult);
        var result = new List<T>(items.Count);
        foreach (var item in items)
            result.Add(MapElement<T>(item, strict: true));
        return result;
    }

    public static void MapInto(object? shapedResult, object into)
    {
        ArgumentNullException.ThrowIfNull(into);

        if (shapedResult is IDictionary<string, object?> dict)
            MapIntoObject(into, dict, strict: false);
        else
            throw new InvalidOperationException(
                "Expected a shaped object (dictionary) but got " + (shapedResult?.GetType().Name ?? "null"));
    }

    internal static object? MapObject(Type type, IDictionary<string, object?> source, bool strict = true)
    {
        EnsureHasBindableProperties(type);
        var instance = Activator.CreateInstance(type)
            ?? throw new InvalidOperationException("Could not create instance of " + type.FullName);
        MapIntoObject(instance, source, strict);
        return instance;
    }

    internal static List<object?> NormalizeList(object? shapedResult)
    {
        if (shapedResult is IList<object?> objectList)
            return objectList.ToList();

        if (shapedResult is IEnumerable enumerable and not string)
            return enumerable.Cast<object?>().ToList();

        throw new InvalidOperationException(
            "Expected a shaped array (list) but got " + (shapedResult?.GetType().Name ?? "null"));
    }

    private static void MapIntoObject(object target, IDictionary<string, object?> source, bool strict)
    {
        var map = GetTypeMap(target.GetType());
        foreach (var binding in map.Properties)
        {
            if (!TryGetValue(source, binding.Key, out var raw))
            {
                if (strict)
                    throw MissingColumnException(binding.Property, target.GetType());
                continue;
            }

            if (raw == null)
            {
                binding.Property.SetValue(target, null);
                continue;
            }

            if (binding.IsList)
            {
                SetListProperty(target, binding, raw, strict);
                continue;
            }

            if (IsDictionary(raw))
            {
                var nestedDict = AsDictionary(raw)!;
                var nested = binding.Property.GetValue(target);
                if (nested == null)
                {
                    nested = MapObject(binding.TargetType, nestedDict, strict);
                    binding.Property.SetValue(target, nested);
                }
                else
                {
                    MapIntoObject(nested, nestedDict, strict);
                }
                continue;
            }

            binding.Property.SetValue(target, ConvertValue(raw, binding.TargetType));
        }
    }

    private static void SetListProperty(object target, PropertyBinding binding, object raw, bool strict)
    {
        var elementType = binding.ElementType!;
        var items = NormalizeListItems(raw);

        var existing = binding.Property.GetValue(target);
        if (existing is IList list && existing is not string && !list.IsFixedSize && list.GetType().IsGenericType)
        {
            list.Clear();
            foreach (var item in items)
                list.Add(MapElement(item, elementType, strict));
            return;
        }

        if (binding.Property.PropertyType.IsArray)
        {
            var array = Array.CreateInstance(elementType, items.Count);
            for (var i = 0; i < items.Count; i++)
                array.SetValue(MapElement(items[i], elementType, strict), i);
            binding.Property.SetValue(target, array);
            return;
        }

        var typedList = (IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType))!;
        foreach (var item in items)
            typedList.Add(MapElement(item, elementType, strict));
        binding.Property.SetValue(target, typedList);
    }

    private static List<object?> NormalizeListItems(object raw)
    {
        if (raw is IList<object?> objectList)
            return objectList.ToList();
        if (raw is IEnumerable enumerable and not string)
            return enumerable.Cast<object?>().ToList();
        throw new InvalidOperationException("Expected a list value but got " + raw.GetType().Name);
    }

    private static T MapElement<T>(object? item, bool strict)
    {
        if (item == null)
            return default!;

        if (item is T typed)
            return typed;

        if (item is IDictionary<string, object?> dict)
            return (T)MapObject(typeof(T), dict, strict)!;

        return (T)ConvertValue(item, typeof(T))!;
    }

    private static object? MapElement(object? item, Type elementType, bool strict)
    {
        if (item == null)
            return null;

        if (elementType.IsInstanceOfType(item))
            return item;

        if (item is IDictionary<string, object?> dict)
            return MapObject(elementType, dict, strict);

        return ConvertValue(item, elementType);
    }

    private static TypeMap GetTypeMap(Type type)
    {
        lock (Cache)
        {
            if (Cache.TryGetValue(type, out var cached))
                return cached;

            var bindings = new List<PropertyBinding>();
            foreach (var prop in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!prop.CanWrite)
                    continue;
                if (prop.GetCustomAttribute<YaalIgnoreAttribute>() != null)
                    continue;

                var propType = prop.PropertyType;
                var isList = TryGetElementType(propType, out var elementType);
                bindings.Add(new PropertyBinding
                {
                    Key = prop.Name,
                    Property = prop,
                    TargetType = isList ? elementType! : propType,
                    IsList = isList,
                    ElementType = elementType,
                });
            }

            cached = new TypeMap { Properties = bindings.ToArray() };
            Cache[type] = cached;
            return cached;
        }
    }

    private static void EnsureHasBindableProperties(Type type)
    {
        var map = GetTypeMap(type);
        if (map.Properties.Length == 0)
        {
            throw new InvalidOperationException(
                $"Type {type.Name} has no public writable properties to map. " +
                "Add public getters/setters or mark client-only properties with [YaalIgnore].");
        }
    }

    private static InvalidOperationException MissingColumnException(PropertyInfo property, Type ownerType)
    {
        var snake = ToSnakeCase(property.Name);
        var tried = string.Equals(snake, property.Name, StringComparison.OrdinalIgnoreCase)
            ? property.Name
            : property.Name + ", " + snake;
        return new InvalidOperationException(
            $"Property '{property.Name}' on type {ownerType.Name} has no matching column in query result (tried {tried}).");
    }

    private static bool TryGetElementType(Type type, out Type? elementType)
    {
        elementType = null;
        if (type.IsArray)
        {
            elementType = type.GetElementType();
            return true;
        }

        if (!type.IsGenericType)
            return false;

        var def = type.GetGenericTypeDefinition();
        if (def == typeof(List<>) || def == typeof(IList<>) || def == typeof(IReadOnlyList<>) ||
            def == typeof(IEnumerable<>))
        {
            elementType = type.GetGenericArguments()[0];
            return true;
        }

        return false;
    }

    private static bool TryGetValue(IDictionary<string, object?> source, string key, out object? value)
    {
        if (TryGetValueExact(source, key, out value))
            return true;

        var snake = ToSnakeCase(key);
        if (!string.Equals(snake, key, StringComparison.OrdinalIgnoreCase) &&
            TryGetValueExact(source, snake, out value))
        {
            return true;
        }

        value = null;
        return false;
    }

    private static bool TryGetValueExact(IDictionary<string, object?> source, string key, out object? value)
    {
        if (source.TryGetValue(key, out value))
            return true;

        foreach (var (k, v) in source)
        {
            if (string.Equals(k, key, StringComparison.OrdinalIgnoreCase))
            {
                value = v;
                return true;
            }
        }

        value = null;
        return false;
    }

    private static string ToSnakeCase(string name)
    {
        if (string.IsNullOrEmpty(name))
            return name;

        var sb = new StringBuilder(name.Length + 4);
        for (var i = 0; i < name.Length; i++)
        {
            var c = name[i];
            if (char.IsUpper(c))
            {
                if (i > 0)
                    sb.Append('_');
                sb.Append(char.ToLowerInvariant(c));
            }
            else
            {
                sb.Append(c);
            }
        }
        return sb.ToString();
    }

    private static bool IsDictionary(object value) =>
        value is IDictionary<string, object?> or IDictionary;

    private static IDictionary<string, object?> AsDictionary(object value)
    {
        if (value is IDictionary<string, object?> typed)
            return typed;

        if (value is IDictionary dict)
        {
            var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (DictionaryEntry entry in dict)
                result[entry.Key.ToString()!] = entry.Value;
            return result;
        }

        throw new InvalidOperationException("Expected a dictionary but got " + value.GetType().Name);
    }

    private static object? ConvertValue(object value, Type targetType)
    {
        if (value == null)
            return null;

        var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
        if (underlying.IsInstanceOfType(value))
            return value;

        if (underlying.IsEnum)
        {
            if (value is string s)
                return Enum.Parse(underlying, s, ignoreCase: true);
            return Enum.ToObject(underlying, Convert.ChangeType(value, Enum.GetUnderlyingType(underlying)!));
        }

        if (underlying == typeof(Guid) && value is string guid)
            return Guid.Parse(guid);

        if (underlying == typeof(DateTime) && value is string dt)
            return DateTime.Parse(dt, null, System.Globalization.DateTimeStyles.RoundtripKind);

        if (underlying == typeof(DateTimeOffset) && value is string dto)
            return DateTimeOffset.Parse(dto, null, System.Globalization.DateTimeStyles.RoundtripKind);

        if (value is IConvertible && typeof(IConvertible).IsAssignableFrom(underlying))
        {
            try
            {
                return Convert.ChangeType(value, underlying, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is FormatException or OverflowException or InvalidCastException)
            {
                throw new InvalidOperationException(
                    $"Cannot convert value of type {value.GetType().Name} to {targetType.Name}.", ex);
            }
        }

        throw new InvalidOperationException(
            $"Cannot convert value of type {value.GetType().Name} to {targetType.Name}.");
    }

    private static List<IDictionary<string, object?>> NormalizeErrors(object errorsObj)
    {
        var result = new List<IDictionary<string, object?>>();
        if (errorsObj is IDictionary<string, object?> single)
            return new List<IDictionary<string, object?>> { single };

        if (errorsObj is IList<object?> list)
        {
            foreach (var item in list)
            {
                if (item is IDictionary<string, object?> dict)
                    result.Add(dict);
                else if (item != null)
                    result.Add(JsonUtil.ToDict(item) ?? new Dictionary<string, object?>());
            }
            return result;
        }

        if (errorsObj is IEnumerable enumerable and not string)
        {
            foreach (var item in enumerable)
            {
                if (item is IDictionary<string, object?> dict)
                    result.Add(dict);
                else if (item != null)
                    result.Add(JsonUtil.ToDict(item) ?? new Dictionary<string, object?>());
            }
        }

        return result;
    }
}
