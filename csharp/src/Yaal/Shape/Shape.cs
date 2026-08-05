using System.Text.Json.Nodes;
using Json.Schema;

namespace Yaal;

public sealed class Shape
{
    private readonly bool _array;
    private readonly Dictionary<string, object?> _inputProperties;
    private int _index;
    private readonly Dictionary<string, object?>? _schema;
    private readonly JsonSchema? _validator;
    private readonly Shape? _parent;
    private readonly Dictionary<string, Shape>? _extras;
    private object _data;
    private object _oData;
    private readonly object _shapes;

    public Shape(
        Dictionary<string, object?>? schema = null,
        object? data = null,
        JsonSchema? validator = null,
        Shape? parentShape = null,
        Dictionary<string, Shape>? extras = null)
    {
        _schema = schema;
        _validator = validator;
        _parent = parentShape;
        _extras = extras;

        // Framework-owned keys allowed in Shape data; user/schema properties may not start with $.
        var allowedDollarDataKeys = new HashSet<string>(StringComparer.Ordinal) { "$run_id" };

        if (schema != null &&
            schema.TryGetValue(YaalConst.Properties, out var schemaPropsObj) &&
            schemaPropsObj is Dictionary<string, object?> schemaProps)
        {
            foreach (var key in schemaProps.Keys)
            {
                if (key.StartsWith('$'))
                {
                    throw new ArgumentException(
                        $"schema properties must not start with $. Reserved keyword '{key}' is not allowed.");
                }
            }
        }

        if (data is Dictionary<string, object?> dataDict)
        {
            foreach (var key in dataDict.Keys)
            {
                if (key.StartsWith('$') && !allowedDollarDataKeys.Contains(key))
                {
                    throw new ArgumentException(
                        $"properties must not start with $. Reserved keyword '{key}' is not allowed.");
                }
            }
        }

        if (extras != null && extras.Values.Any(e => e is null))
            throw new ArgumentException("$extra should be type shape.");

        schema ??= new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
        {
            [YaalConst.Type] = YaalConst.Object,
        };

        _inputProperties = schema.TryGetValue(YaalConst.Properties, out var propsObj) &&
                           propsObj is Dictionary<string, object?> props
            ? props
            : new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        var type = schema.TryGetValue(YaalConst.Type, out var t) ? t?.ToString() : YaalConst.Object;

        if (data == null)
        {
            if (type == YaalConst.Array)
            {
                _data = new List<object?>();
                _oData = new List<object?>();
            }
            else
            {
                _data = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                _oData = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            }
        }
        else
        {
            _data = data;
            _oData = data;
        }

        if (type == YaalConst.Array)
        {
            _array = true;
            if (data != null && data is not IList<object?> and not List<object?> and not System.Collections.IList)
                throw new ArgumentException("input expected as array. object is given.");
            if (_data is not IList<object?> && _data is System.Collections.IList rawList)
            {
                var converted = new List<object?>();
                foreach (var item in rawList)
                    converted.Add(JsonUtil.NormalizeValue(item));
                _data = converted;
                _oData = converted;
            }
        }
        else
        {
            if (data != null)
            {
                if (data is Dictionary<string, object?> d)
                {
                    _data = JsonUtil.ToLowerKeys(d);
                }
                else
                {
                    var asDict = JsonUtil.ToDict(data);
                    if (asDict == null)
                        throw new ArgumentException("input expected as object. " + data.GetType() + " is given.");
                    _data = JsonUtil.ToLowerKeys(asDict);
                }
            }
        }

        if (_array)
        {
            var shapes = new List<Shape>();
            var itemSchema = (Dictionary<string, object?>)JsonUtil.DeepCopy(schema)!;
            itemSchema[YaalConst.Type] = YaalConst.Object;
            var idx = 0;
            foreach (var item in (IList<object?>)_data)
            {
                var s = new Shape(schema: itemSchema, data: item, parentShape: this, extras: extras)
                {
                    _index = idx,
                };
                shapes.Add(s);
                idx += 1;
            }
            _shapes = shapes;
        }
        else
        {
            var shapes = new Dictionary<string, Shape>(StringComparer.OrdinalIgnoreCase);
            var dataMap = (Dictionary<string, object?>)_data;
            foreach (var (kRaw, v) in _inputProperties)
            {
                var k = kRaw.ToLowerInvariant();
                if (v is Dictionary<string, object?> vDict)
                {
                    var typeValue = vDict.TryGetValue(YaalConst.Type, out var tv) ? tv?.ToString() : null;
                    if (typeValue is YaalConst.Array or YaalConst.Object)
                    {
                        dataMap.TryGetValue(k, out var childData);
                        shapes[k] = new Shape(schema: vDict, data: childData, parentShape: this, extras: extras);
                    }
                }
            }
            _shapes = shapes;
        }
    }

    public object? GetProp(string prop)
    {
        var extras = _extras;
        var parent = _parent;
        var data = _data;

        var dot = prop.IndexOf('.');
        if (dot > -1)
        {
            var path = prop[..dot];
            var remainingPath = prop[(dot + 1)..];

            if (_array)
            {
                if (!int.TryParse(path[1..], out var idx))
                    throw new KeyNotFoundException("array path excepted as $index.");
                return ((List<Shape>)_shapes)[idx].GetProp(remainingPath);
            }

            if (path.StartsWith('$'))
            {
                if (path == YaalConst.Parent)
                    return parent!.GetProp(remainingPath);

                if (extras != null && extras.TryGetValue(path, out var extraShape))
                    return extraShape.GetProp(remainingPath);
            }

            return ((Dictionary<string, Shape>)_shapes)[path].GetProp(remainingPath);
        }

        if (prop.StartsWith('$'))
        {
            if (prop is YaalConst.Json or YaalConst.Parent or YaalConst.Length or YaalConst.Index)
            {
                if (prop == YaalConst.Json)
                    return JsonUtil.Serialize(GetData());
                if (prop == YaalConst.Parent)
                    return parent;
                if (prop == YaalConst.Length)
                    return data is System.Collections.ICollection c ? c.Count : 0;
                if (prop == YaalConst.Index)
                    return _index;
            }

            if (extras != null && extras.TryGetValue(prop, out var extra))
                return extra;
        }

        if (_array)
        {
            if (!int.TryParse(prop[1..], out var idx))
                throw new KeyNotFoundException("array path excepted as $index.");
            return ((List<Shape>)_shapes)[idx];
        }

        var shapesMap = (Dictionary<string, Shape>)_shapes;
        if (shapesMap.TryGetValue(prop, out var nested))
            return nested;

        var dataMap = (Dictionary<string, object?>)data;
        if (dataMap.TryGetValue(prop, out var value))
            return value;

        if (_inputProperties.TryGetValue(prop, out var propertySchema) &&
            propertySchema is Dictionary<string, object?> ps &&
            ps.TryGetValue("default", out var defaultValue))
        {
            return defaultValue;
        }

        return null;
    }

    public void SetProp(string prop, object? value)
    {
        var shapes = _shapes;
        var dot = prop.IndexOf('.');
        if (dot > -1)
        {
            var path = prop[..dot];
            var remainingPath = prop[(dot + 1)..];

            if (_array)
            {
                if (!int.TryParse(path[1..], out var idx))
                    throw new KeyNotFoundException("array path excepted as $index.");
                ((List<Shape>)shapes)[idx].SetProp(remainingPath, value);
                return;
            }

            var shapesMap = (Dictionary<string, Shape>)shapes;
            if (shapesMap.TryGetValue(path, out var nested))
            {
                nested.SetProp(remainingPath, value);
                return;
            }

            if (_extras != null && _extras.TryGetValue(path, out var extra))
                extra.SetProp(remainingPath, value);
            return;
        }

        value = TypeCast(prop, value);
        var dataMap = (Dictionary<string, object?>)_data;
        dataMap[prop.ToLowerInvariant()] = value;

        var oDataMap = (Dictionary<string, object?>)_oData;
        oDataMap.Remove(prop.ToLowerInvariant());
        oDataMap[prop] = value;
    }

    public List<Dictionary<string, object?>> Validate(bool includeExtras = false)
    {
        var errors = new List<Dictionary<string, object?>>();

        if (_extras != null && includeExtras)
        {
            foreach (var (name, extra) in _extras)
            {
                foreach (var x in extra.Validate(includeExtras))
                {
                    x["name"] = name;
                    errors.Add(x);
                }
            }
        }

        if (_validator != null)
        {
            var json = JsonUtil.Serialize(_data);
            using var doc = System.Text.Json.JsonDocument.Parse(json);
            var result = _validator.Evaluate(doc.RootElement, new EvaluationOptions
            {
                OutputFormat = OutputFormat.List,
            });
            if (!result.IsValid)
            {
                CollectSchemaErrors(result, errors);
            }
        }

        return errors;
    }

    private static void CollectSchemaErrors(EvaluationResults result, List<Dictionary<string, object?>> errors)
    {
        if (result.Errors != null)
        {
            foreach (var err in result.Errors)
            {
                errors.Add(new Dictionary<string, object?>
                {
                    ["message"] = err.Value,
                });
            }
        }

        if (result.Details == null)
            return;

        foreach (var detail in result.Details)
            CollectSchemaErrors(detail, errors);
    }

    public object GetData() => _oData;

    public Dictionary<string, object?>? GetSchema() => _schema;

    private object? TypeCast(string prop, object? value)
    {
        if (!_inputProperties.TryGetValue(prop, out var propSchemaObj) ||
            propSchemaObj is not Dictionary<string, object?> propSchema)
            return value;

        var parameterType = propSchema.TryGetValue(YaalConst.Format, out var fmt) ? fmt?.ToString() : null;
        parameterType ??= propSchema.TryGetValue(YaalConst.Type, out var ty) ? ty?.ToString() : null;

        try
        {
            if (value != null && parameterType != null)
            {
                if (parameterType == "integer" && value is not int and not long)
                    return Convert.ToInt64(value);
                if (parameterType == "string" && value is not string)
                    return value.ToString();
                if (parameterType == "float" && value is not float and not double and not decimal)
                    return Convert.ToDouble(value);
                if (parameterType == "boolean" && value is not bool)
                    return CoerceBoolean(value);
            }
        }
        catch (Exception)
        {
            throw new ArgumentException("value expected as " + parameterType + ", given " + value?.GetType());
        }

        return value;
    }

    public static bool CoerceBoolean(object value)
    {
        if (value is bool b)
            return b;
        if (value is int or long or float or double or decimal)
        {
            var n = Convert.ToDouble(value);
            if (n is 0 or 1)
                return n != 0;
        }
        if (value is string s)
        {
            var lowered = s.Trim().ToLowerInvariant();
            if (lowered is "true" or "1" or "yes" or "y" or "on")
                return true;
            if (lowered is "false" or "0" or "no" or "n" or "off")
                return false;
        }
        throw new ArgumentException("value expected as boolean, given " + value.GetType());
    }

    public override string ToString() => JsonUtil.Serialize(GetData());
}
